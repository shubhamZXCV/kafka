"""Backpressure functional test F-BP-1 (LLD §15.3)."""

import os
import sys
from pathlib import Path

from elog_test.tools import create_topic, describe_topic, start_consumer, start_producer
from elog_test.wait import wait_until

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "analysis"))
from elog_analysis.loaders import load_metrics  # noqa: E402

QUEUE = 100
PROCESS_US = 1_000                     # about 1 000 records/s of consumer capacity (R-22)
RATE = 3_000                           # 3x that
SECONDS = 20
WARM_UP_MS = 5_000


def test_F_BP_1_queue_and_memory_stay_bounded(make_cluster, out_dir, monkeypatch):
    """A producer three times faster than the consumer: the in-flight queue never holds
    more than Q records, and the consumer's memory stops growing after warm-up."""
    # Under ASan, freed memory is held in a 256 MB quarantine, which alone would look like
    # growth for the first ~10 s. A small one fills during warm-up. Other builds ignore it.
    asan = os.environ.get("ASAN_OPTIONS", "")
    monkeypatch.setenv("ASAN_OPTIONS", f"{asan}:quarantine_size_mb=16".lstrip(":"))
    cluster = make_cluster(brokers=1)
    create_topic(cluster, "t", 1)
    wait_until(lambda: describe_topic(cluster, "t")["partitions"][0]["highWatermark"] >= 0,
               20, "leader")
    producer = start_producer(cluster, "t", out_dir, rate=RATE, count=RATE * SECONDS,
                              payload=1_000)
    consumer = start_consumer(cluster, "t", out_dir, inflight=QUEUE, process_us=PROCESS_US)
    assert producer.wait(SECONDS + 30) == 0
    assert consumer.stop() == 0

    metrics = load_metrics(out_dir, "metrics-c1.csv")
    depth = metrics[metrics["metric"] == "inflight_depth"]["value"]
    assert len(depth) >= SECONDS // 2 and depth.max() <= QUEUE

    rss = metrics[metrics["metric"] == "process_rss_bytes"]
    rss = rss[rss["ts_ms"] >= rss["ts_ms"].min() + WARM_UP_MS]["value"]
    # Unbounded buffering would grow by 2 000 records x 1 kB = 2 MB every second.
    assert rss.max() - rss.iloc[0] < 8 * 2**20, f"RSS grew from {rss.iloc[0]} to {rss.max()}"
