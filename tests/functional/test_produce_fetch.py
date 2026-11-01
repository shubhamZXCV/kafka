"""Produce/fetch functional tests F-PF-1 … F-PF-4 (LLD §15.3): the M1 walking skeleton."""

import socket
import struct
import sys
from pathlib import Path

import pandas as pd

from elog_test.tools import create_topic, describe_topic, dump, start_consumer, start_producer
from elog_test.wait import wait_until

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "analysis"))
from elog_analysis.checks import check_committed_loss, check_order, check_skipped  # noqa: E402
from elog_analysis.loaders import load_processed, load_produced  # noqa: E402


def leaders_ready(cluster, topic):
    """Every partition has a leader that answers (its HW is readable)."""
    return lambda: all(p["highWatermark"] >= 0 for p in describe_topic(cluster, topic)["partitions"])


def test_F_PF_1_walking_skeleton(make_cluster, out_dir):
    """1 M keyed records, 12 partitions, R=1: all consumed, in order, nothing acked lost."""
    records, partitions = 1_000_000, 12
    cluster = make_cluster(brokers=3)
    create_topic(cluster, "orders", partitions)
    wait_until(leaders_ready(cluster, "orders"), 20, "partition leaders")

    producer = start_producer(cluster, "orders", out_dir, count=records, keys=1000, payload=100)
    consumer = start_consumer(cluster, "orders", out_dir, count=records)
    assert producer.wait(600) == 0, "elog-producer did not get every record acked"
    assert consumer.wait(600) == 0

    log = {("orders", p): dump(cluster, "orders", p) for p in range(partitions)}
    high_watermarks = {tp: len(entries) for tp, entries in log.items()}
    assert sum(high_watermarks.values()) == records
    produced, processed = load_produced(out_dir), load_processed(out_dir)
    assert produced["count"].sum() == records
    assert check_skipped(processed, high_watermarks) == []        # all consumed (INV-05)
    assert check_order(processed, log) == []                      # INV-01
    assert check_committed_loss(produced, log) == []


def test_F_PF_2_long_poll_wakes_up(make_cluster, out_dir):
    """One record into an idle topic reaches a waiting consumer well under
    fetch.max.wait.ms (500 ms): the long-poll wakes up instead of timing out."""
    cluster = make_cluster(brokers=1)
    create_topic(cluster, "idle", 1)
    wait_until(leaders_ready(cluster, "idle"), 20, "partition leader")
    consumer = start_consumer(cluster, "idle", out_dir, count=1)
    wait_until(lambda: "consuming" in consumer.log(), 20, "consumer started")
    # The consumer is now parked in a long-poll on the empty partition.
    producer = start_producer(cluster, "idle", out_dir, count=1)
    assert producer.wait(30) == 0
    assert consumer.wait(30) == 0

    acked_ms = pd.read_csv(out_dir / "produced-p1.csv")["ts_ms"].iloc[0]
    processed_ms = pd.read_csv(out_dir / "processed-c1.csv")["ts_ms"].iloc[0]
    assert processed_ms - acked_ms < 250, f"delivered {processed_ms - acked_ms} ms after ack"


def test_F_PF_3_create_topic_is_idempotent(make_cluster):
    cluster = make_cluster(brokers=1)
    assert create_topic(cluster, "orders", 4, check=False).returncode == 0
    assert create_topic(cluster, "orders", 4, check=False).returncode == 0
    mismatch = create_topic(cluster, "orders", 5, check=False)
    assert mismatch.returncode == 1
    assert "INVALID_PARTITION_COUNT" in mismatch.stderr


def closed_by_peer(sock):
    """True once the peer closes the connection (recv returns EOF or the connection resets)."""
    sock.settimeout(5)
    try:
        return sock.recv(1) == b""
    except ConnectionResetError:
        return True


def test_F_PF_4_malformed_frames_close_only_that_connection(make_cluster, out_dir):
    """Garbage and an oversized frame header close their connection (NFR-SEC-02); the
    broker keeps serving a client that was already connected and new ones."""
    cluster = make_cluster(brokers=1)
    create_topic(cluster, "orders", 1)
    wait_until(leaders_ready(cluster, "orders"), 20, "partition leader")
    broker_port = cluster.brokers[1].port
    bystander = socket.create_connection(("127.0.0.1", broker_port))

    garbage = socket.create_connection(("127.0.0.1", broker_port))
    garbage.sendall(struct.pack(">I", 60) + b"\x41" * 60)    # bad magic, bad CRC
    assert closed_by_peer(garbage)
    oversized = socket.create_connection(("127.0.0.1", broker_port))
    oversized.sendall(struct.pack(">I", 0xFFFF_FFFF))       # 4 GiB: refused before allocating
    assert closed_by_peer(oversized)

    assert cluster.brokers[1].running()
    bystander.settimeout(0.2)
    try:
        assert bystander.recv(1) != b"", "the bystander connection was closed"
    except socket.timeout:
        pass                                                 # still open, nothing to read
    producer = start_producer(cluster, "orders", out_dir, count=100)
    assert producer.wait(30) == 0
    assert describe_topic(cluster, "orders")["partitions"][0]["highWatermark"] == 100
    assert "CORRUPT_MESSAGE" in cluster.brokers[1].log()
