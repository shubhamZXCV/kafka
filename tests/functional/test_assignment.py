"""Assignment functional tests F-ASG-1 and F-ASG-2 (LLD §15.3)."""

from collections import Counter

from elog_test.tools import create_topic, describe_group, describe_topic, start_consumer
from elog_test.wait import wait_until

FAST = {"session_timeout_ms": 3000, "heartbeat_interval_ms": 300}


def kill_one_of_three(make_cluster, out_dir, assignor):
    """6 partitions over consumers c0, c1, c2; c0 is killed. Returns {partition: (owner,
    epoch)} before and after the rebalance, and c0's member ID."""
    cluster = make_cluster(brokers=1, coordinator_settings={"group.default.assignor": assignor})
    create_topic(cluster, "t", 6)
    wait_until(lambda: all(p["highWatermark"] >= 0
                           for p in describe_topic(cluster, "t")["partitions"]), 20, "leaders")
    consumers = [start_consumer(cluster, "t", out_dir, id=f"c{i}", group="g", **FAST)
                 for i in range(3)]

    def stable(members):
        g = describe_group(cluster, "g")
        return g if g["state"] == "Stable" and len(g["members"]) == members else None

    def owners(group):
        return {p["partition"]: (p["owner"], p["epoch"]) for p in group["partitions"]}

    before = wait_until(lambda: stable(3), 30, "3 stable members")
    victim = next(m["memberId"] for m in before["members"] if m["memberId"].startswith("c0-"))
    consumers[0].kill()
    after = wait_until(lambda: stable(2), 3 + 10, "rebalance after the kill")
    return owners(before), owners(after), victim


def test_F_ASG_1_sticky_moves_only_the_dead_members_partitions(make_cluster, out_dir):
    before, after, victim = kill_one_of_three(make_cluster, out_dir, "sticky")
    moved = {p for p, (owner, _) in before.items() if owner == victim}
    assert len(moved) == 2
    for p, owner_and_epoch in before.items():
        if p not in moved:                              # same owner, same epoch
            assert after[p] == owner_and_epoch
    assert all(after[p][0] != victim for p in moved)
    assert sorted(Counter(owner for owner, _ in after.values()).values()) == [3, 3]


def test_F_ASG_2_roundrobin_gives_every_partition_a_new_epoch(make_cluster, out_dir):
    before, after, _ = kill_one_of_three(make_cluster, out_dir, "roundrobin")
    assert sorted(after) == list(range(6))
    assert all(after[p][1] > before[p][1] for p in range(6))
