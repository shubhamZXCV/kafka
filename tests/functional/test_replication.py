"""Replication functional tests F-REP-1 … (LLD §15.3)."""

import hashlib

from elog_test.tools import create_topic, describe_topic, log_verify, start_producer
from elog_test.wait import wait_until


def replicas_ready(cluster, topic):
    return lambda: all(p["highWatermark"] >= 0 and len(p["isr"]) == len(p["replicas"])
                       for p in describe_topic(cluster, topic)["partitions"])


def log_digest(cluster, broker, topic, partition):
    """SHA-256 over the partition's segment files in offset order on one broker."""
    digest = hashlib.sha256()
    for segment in sorted((cluster.root / f"broker-{broker}" / f"{topic}-{partition}").glob("*.log")):
        digest.update(segment.read_bytes())
    return digest.hexdigest()


def test_F_REP_1_replicas_converge(make_cluster, out_dir):
    """R=3, 100 k records: all three replicas pass log-verify with identical offset ranges
    and byte-identical logs."""
    cluster = make_cluster(brokers=3)
    create_topic(cluster, "t", 3, replication=3, min_isr=2)
    wait_until(replicas_ready(cluster, "t"), 30, "replicas")
    assert start_producer(cluster, "t", out_dir, count=100_000).wait(300) == 0

    hws = {p["partition"]: p["highWatermark"] for p in describe_topic(cluster, "t")["partitions"]}
    assert sum(hws.values()) == 100_000
    # Followers fetch until they hold everything the leader has.
    def converged():
        return all(len({log_digest(cluster, b, "t", p) for b in (1, 2, 3)}) == 1 for p in hws)
    wait_until(converged, 30, "byte-identical replicas")
    for broker in (1, 2, 3):
        code, report = log_verify(cluster, broker)
        assert code == 0, report
        ranges = {p["partition"]: (p["firstOffset"], p["nextOffset"]) for p in report["partitions"]}
        assert ranges == {f"t-{p}": (0, hw) for p, hw in hws.items()}
