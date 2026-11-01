"""Storage functional tests F-STO-1 … (LLD §15.3)."""

import sys
from pathlib import Path

from elog_test.tools import create_topic, describe_topic, dump, start_consumer, start_producer
from elog_test.wait import wait_until

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "analysis"))
from elog_analysis.checks import check_order, check_skipped  # noqa: E402
from elog_analysis.loaders import load_processed  # noqa: E402


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
