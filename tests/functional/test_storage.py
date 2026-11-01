"""Storage functional tests F-STO-1 … F-STO-5 (LLD §15.3)."""

import random
import sys
from pathlib import Path

from elog_test.tools import (create_topic, describe_topic, dump, log_verify, start_consumer,
                             start_producer)
from elog_test.wait import wait_until

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "analysis"))
from elog_analysis.checks import check_committed_loss, check_order, check_skipped  # noqa: E402
from elog_analysis.loaders import load_processed, load_produced  # noqa: E402


def leaders_ready(cluster, topic):
    return lambda: all(p["highWatermark"] >= 0 for p in describe_topic(cluster, topic)["partitions"])


def segment_files(cluster, broker, topic, partition):
    return sorted((cluster.root / f"broker-{broker}" / f"{topic}-{partition}").glob("*.log"))


def test_F_STO_1_many_segments(make_cluster, out_dir):
    """200 k records with 64 KiB segments: many segments; every record consumed from offset
    0, gap-free and in order."""
    records = 200_000
    cluster = make_cluster(brokers=1, broker_settings={"segment.bytes": 64 << 10})
    create_topic(cluster, "t", 1)
    wait_until(leaders_ready(cluster, "t"), 20, "partition leader")
    assert start_producer(cluster, "t", out_dir, count=records, keys=100).wait(300) == 0
    assert len(segment_files(cluster, 1, "t", 0)) > 50

    assert start_consumer(cluster, "t", out_dir, count=records).wait(300) == 0
    processed = load_processed(out_dir)
    assert check_skipped(processed, {("t", 0): records}) == []
    assert check_order(processed, {}) == []
    assert (processed["end_offset_excl"] - processed["start_offset"]).sum() == records


def test_F_STO_2_read_from_the_middle_of_a_batch(make_cluster, out_dir):
    """A read starting inside a batch begins at exactly the requested offset, in the first
    segment, in a middle one, and in the active one."""
    cluster = make_cluster(brokers=1, broker_settings={"segment.bytes": 64 << 10})
    create_topic(cluster, "t", 1)
    wait_until(leaders_ready(cluster, "t"), 20, "partition leader")
    assert start_producer(cluster, "t", out_dir, count=20_000, keys=10).wait(120) == 0
    assert len(segment_files(cluster, 1, "t", 0)) > 3

    for start in (7, 1_234, 10_001, 19_999):
        records = dump(cluster, "t", 0, start)
        assert records[0]["offset"] == start
        assert [r["offset"] for r in records] == list(range(start, 20_000))


def high_watermark(cluster, topic, partition=0):
    return describe_topic(cluster, topic)["partitions"][partition]["highWatermark"]


def batch_starts(segment):
    """Byte positions and base offsets of the batches in a segment file (LLD §4.5:
    baseOffset u64 BE, batchLength u32 BE, then batchLength bytes)."""
    data, pos, starts = segment.read_bytes(), 0, []
    while pos + 12 <= len(data):
        base = int.from_bytes(data[pos:pos + 8], "big")
        starts.append((pos, base))
        pos += 12 + int.from_bytes(data[pos + 8:pos + 12], "big")
    return starts


def restart_broker(cluster, broker_id, topic):
    cluster.brokers[broker_id].start()
    cluster.wait_healthy(len(cluster.brokers))
    wait_until(lambda: high_watermark(cluster, topic) >= 0, 30, "partition reopened")


def test_F_STO_3_kill_during_produce(make_cluster, out_dir):
    """SIGKILL the broker during heavy produce, restart: log-verify passes and every acked
    record is still in the log."""
    cluster = make_cluster(brokers=1, broker_settings={"segment.bytes": 256 << 10})
    create_topic(cluster, "t", 1)
    wait_until(leaders_ready(cluster, "t"), 20, "partition leader")
    producer = start_producer(cluster, "t", out_dir, count=200_000, rate=40_000, keys=100)
    wait_until(lambda: high_watermark(cluster, "t") > 20_000, 60, "produce under way")
    cluster.brokers[1].kill()
    restart_broker(cluster, 1, "t")
    producer.wait(300)        # some records may have exhausted their retries: not acked

    code, report = log_verify(cluster, 1)
    assert code == 0, report
    produced = load_produced(out_dir)
    assert produced["count"].sum() > 20_000
    assert check_committed_loss(produced, {("t", 0): dump(cluster, "t", 0)}) == []


def test_F_STO_4_torn_tail_is_truncated(make_cluster, out_dir):
    """Cut the active segment at random positions inside its last batch: the broker starts,
    the log ends at the last complete batch, log-verify passes."""
    cluster = make_cluster(brokers=1)
    create_topic(cluster, "t", 1)
    wait_until(leaders_ready(cluster, "t"), 20, "partition leader")
    rng = random.Random(4)
    for round_ in range(3):
        producer = start_producer(cluster, "t", out_dir, id=f"p{round_}", count=5_000)
        assert producer.wait(60) == 0
        assert cluster.brokers[1].stop() == 0
        segment = segment_files(cluster, 1, "t", 0)[-1]
        last_pos, last_base = batch_starts(segment)[-1]
        size = segment.stat().st_size
        with open(segment, "r+b") as f:
            f.truncate(rng.randrange(last_pos + 1, size))     # somewhere inside the last batch
        restart_broker(cluster, 1, "t")
        assert high_watermark(cluster, "t") == last_base
        code, report = log_verify(cluster, 1)
        assert code == 0, report


def test_F_STO_5_indexes_are_rebuilt(make_cluster, out_dir):
    """Delete every .index file: after a restart they are rebuilt and reads from arbitrary
    offsets start exactly there."""
    cluster = make_cluster(brokers=1, broker_settings={"segment.bytes": 64 << 10})
    create_topic(cluster, "t", 1)
    wait_until(leaders_ready(cluster, "t"), 20, "partition leader")
    assert start_producer(cluster, "t", out_dir, count=30_000).wait(120) == 0
    assert cluster.brokers[1].stop() == 0
    partition_dir = cluster.root / "broker-1" / "t-0"
    for index in partition_dir.glob("*.index"):
        index.unlink()

    restart_broker(cluster, 1, "t")
    assert len(list(partition_dir.glob("*.index"))) == len(segment_files(cluster, 1, "t", 0))
    assert all(index.stat().st_size > 0 for index in partition_dir.glob("*.index"))
    for start in (0, 4_321, 17_777, 29_999):
        assert dump(cluster, "t", 0, start)[0]["offset"] == start


def test_F_STO_6_size_retention(make_cluster, out_dir):
    """Small retention.bytes: old segments are deleted, the log start advances, and a
    consumer starting at offset 0 resets to the new log start."""
    cluster = make_cluster(brokers=1, broker_settings={
        "segment.bytes": 64 << 10, "retention.bytes": 256 << 10,
        "retention.check.interval.ms": 200})
    create_topic(cluster, "t", 1)
    wait_until(leaders_ready(cluster, "t"), 20, "partition leader")
    assert start_producer(cluster, "t", out_dir, count=50_000).wait(120) == 0

    # LLD §5.7 deletes the oldest segment only while the rest still hold at least
    # retention.bytes, so about 256 KiB / 64 KiB + 2 segments remain of the several MiB.
    wait_until(lambda: len(segment_files(cluster, 1, "t", 0)) <= 8, 20, "old segments deleted")
    log_start = int(segment_files(cluster, 1, "t", 0)[0].stem)
    assert log_start > 0
    code, report = log_verify(cluster, 1)
    assert code == 0 and report["partitions"][0]["firstOffset"] == log_start

    remaining = 50_000 - log_start
    assert start_consumer(cluster, "t", out_dir, count=remaining).wait(60) == 0
    processed = load_processed(out_dir)
    assert processed["start_offset"].min() == log_start
    assert check_skipped(processed[processed["start_offset"] >= log_start]
                         .assign(start_offset=lambda d: d["start_offset"] - log_start,
                                 end_offset_excl=lambda d: d["end_offset_excl"] - log_start),
                         {("t", 0): remaining}) == []
