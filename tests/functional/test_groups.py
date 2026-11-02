"""Consumer-group functional tests F-GRP-1 … F-GRP-4 (LLD §15.3)."""

import sys
import time
from collections import Counter
from pathlib import Path

from elog_test.tools import (create_topic, describe_group, describe_topic, start_consumer,
                             start_producer)
from elog_test.wait import wait_until

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "analysis"))
from elog_analysis.checks import check_no_skips, count_duplicates  # noqa: E402
from elog_analysis.loaders import load_processed, load_produced  # noqa: E402

FAST = {"session_timeout_ms": 3000, "heartbeat_interval_ms": 300}


def topic_with_records(cluster, out_dir, partitions, records):
    create_topic(cluster, "t", partitions)
    wait_until(lambda: all(p["highWatermark"] >= 0
                           for p in describe_topic(cluster, "t")["partitions"]), 20, "leaders")
    if records:
        assert start_producer(cluster, "t", out_dir, count=records).wait(120) == 0


def stable_with(cluster, members):
    def check():
        g = describe_group(cluster, "g")
        return g if g["state"] == "Stable" and len(g["members"]) == members else None
    return check


def all_committed(cluster):
    """Every partition's committed offset has reached its high watermark."""
    hws = {p["partition"]: p["highWatermark"] for p in describe_topic(cluster, "t")["partitions"]}
    owned = {p["partition"]: p["committedOffset"] for p in describe_group(cluster, "g")["partitions"]}
    return len(owned) == len(hws) and all(owned[p] == hw for p, hw in hws.items())


def test_F_GRP_1_four_consumers_share_twelve_partitions(make_cluster, out_dir):
    cluster = make_cluster(brokers=2)
    topic_with_records(cluster, out_dir, 12, 0)
    for i in range(4):
        start_consumer(cluster, "t", out_dir, id=f"c{i}", group="g", **FAST)
    group = wait_until(stable_with(cluster, 4), 30, "4 stable members")
    owners = Counter(p["owner"] for p in group["partitions"])
    assert sorted(p["partition"] for p in group["partitions"]) == list(range(12))
    assert sorted(owners.values()) == [3, 3, 3, 3]
    assert set(owners) == {m["memberId"] for m in group["members"]}


def test_F_GRP_2_killed_consumer_is_replaced_without_skips(make_cluster, out_dir):
    cluster = make_cluster(brokers=2)
    topic_with_records(cluster, out_dir, 6, 30_000)
    consumers = [start_consumer(cluster, "t", out_dir, id=f"c{i}", group="g", inflight=50, **FAST)
                 for i in range(3)]
    wait_until(stable_with(cluster, 3), 30, "3 stable members")
    killed_at = time.monotonic()
    consumers[0].kill()
    wait_until(stable_with(cluster, 2), 3 + 5, "rebalance after the kill")
    assert time.monotonic() - killed_at < 3 + 5        # session timeout + margin
    wait_until(lambda: all_committed(cluster), 60, "everything committed")
    for c in consumers[1:]:
        c.stop()
    assert check_no_skips(load_processed(out_dir), load_produced(out_dir)) == []


def test_F_GRP_3_graceful_leave_rebalances_at_once(make_cluster, out_dir):
    cluster = make_cluster(brokers=1)
    topic_with_records(cluster, out_dir, 4, 0)
    slow = {"session_timeout_ms": 10_000, "heartbeat_interval_ms": 300}
    consumers = [start_consumer(cluster, "t", out_dir, id=f"c{i}", group="g", **slow)
                 for i in range(2)]
    before = wait_until(stable_with(cluster, 2), 30, "2 stable members")
    left_at = time.monotonic()
    assert consumers[0].stop() == 0
    after = wait_until(stable_with(cluster, 1), 10, "rebalance after the leave")
    assert time.monotonic() - left_at < 5               # well before the 10 s session timeout
    assert after["generation"] > before["generation"]


def test_F_GRP_4_restart_resumes_from_the_committed_offset(make_cluster, out_dir):
    cluster = make_cluster(brokers=1)
    topic_with_records(cluster, out_dir, 2, 20_000)
    first = start_consumer(cluster, "t", out_dir, id="c1", group="g", inflight=50,
                           commit_interval_records=100, **FAST)
    wait_until(lambda: sum(max(p["committedOffset"], 0)
                           for p in describe_group(cluster, "g")["partitions"]) > 5_000,
               60, "some progress committed")
    first.kill()                                        # the worst case: no final commit
    wait_until(lambda: describe_group(cluster, "g")["state"] == "Empty", 10, "member gone")
    start_consumer(cluster, "t", out_dir, id="c2", group="g", inflight=50,
                   commit_interval_records=100, **FAST)
    wait_until(lambda: all_committed(cluster), 60, "everything committed")
    cluster.processes[-1].stop()
    processed = load_processed(out_dir)
    assert check_no_skips(processed, load_produced(out_dir)) == []
    duplicates, _ = count_duplicates(processed)
    assert duplicates <= 100                            # at most one commit interval


def test_F_GRP_5_coordinator_restart(make_cluster, out_dir):
    """Restart the coordinator under a running group: the consumers rejoin, the first
    generation after the restart is the previous one + 1, and nothing is skipped."""
    cluster = make_cluster(brokers=1)
    topic_with_records(cluster, out_dir, 4, 30_000)
    consumers = [start_consumer(cluster, "t", out_dir, id=f"c{i}", group="g", inflight=50, **FAST)
                 for i in range(2)]
    before = wait_until(stable_with(cluster, 2), 30, "2 stable members")["generation"]
    events_before = len(cluster.events())

    cluster.restart_coordinator()
    wait_until(stable_with(cluster, 2), 30, "both consumers back")
    after = [int(e["generation"]) for e in cluster.events()[events_before:]
             if e["event"] == "rebalance_end"]
    assert after and after[0] == before + 1
    wait_until(lambda: all_committed(cluster), 60, "everything committed")
    for c in consumers:
        c.stop()
    assert check_no_skips(load_processed(out_dir), load_produced(out_dir)) == []
