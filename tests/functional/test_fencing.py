"""Fencing functional tests F-FEN-1 and F-FEN-2 (LLD §15.3), the centre of the project:
a consumer declared dead wakes up and commits with its old ownership epoch."""

import sys
import time
from pathlib import Path

import pandas as pd

from elog_test.tools import create_topic, describe_group, describe_topic, start_consumer, start_producer
from elog_test.wait import wait_until

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "analysis"))
from elog_analysis.checks import count_offset_regressions  # noqa: E402

SESSION_MS = 3000


def zombie_scenario(cluster, out_dir):
    """Z (slow, commits after every record) and H share a topic; Z is paused for twice its
    session timeout while H takes over and commits further ahead; then Z is resumed and
    commits with its old epoch. Returns Z's member ID before the pause."""
    create_topic(cluster, "t", 2)
    wait_until(lambda: all(p["highWatermark"] >= 0
                           for p in describe_topic(cluster, "t")["partitions"]), 20, "leaders")
    assert start_producer(cluster, "t", out_dir, count=20_000).wait(120) == 0
    common = {"group": "g", "session_timeout_ms": SESSION_MS, "heartbeat_interval_ms": 300,
              "inflight": 50}
    zombie = start_consumer(cluster, "t", out_dir, id="z", process_us=2000,
                            commit_interval_ms=0, **common)
    wait_until(lambda: describe_group(cluster, "g")["state"] == "Stable", 30, "z joined")
    start_consumer(cluster, "t", out_dir, id="h", process_us=200, **common)

    def z_has_committed():
        g = describe_group(cluster, "g")
        z = [m["memberId"] for m in g["members"] if m["clientId"] == "z"]
        if g["state"] != "Stable" or len(g["members"]) != 2 or not z:
            return None
        owned = [p for p in g["partitions"] if p["owner"] == z[0]]
        return z[0] if owned and all(p["committedOffset"] > 0 for p in owned) else None

    zombie_id = wait_until(z_has_committed, 30, "z owns a partition and has committed")
    zombie.pause()
    time.sleep(2 * SESSION_MS / 1000)          # declared dead; h takes everything over
    group = describe_group(cluster, "g")
    assert [m["clientId"] for m in group["members"]] == ["h"]
    zombie.resume()                            # z finishes its record and commits, stale
    return zombie_id


def coordinator_events(cluster):
    return pd.read_csv(cluster.root / "events-coordinator.csv")


def test_F_FEN_1_zombie_commit_is_fenced(make_cluster, out_dir):
    cluster = make_cluster(brokers=1)
    zombie_id = zombie_scenario(cluster, out_dir)
    wait_until(lambda: (coordinator_events(cluster)["event"] == "commit_fenced").any(), 20,
               "the zombie's commit to be fenced")
    regressions, _ = count_offset_regressions(coordinator_events(cluster))
    assert regressions == 0
    # The zombie dropped everything and came back as a new member.
    rejoined = wait_until(lambda: [m["memberId"] for m in describe_group(cluster, "g")["members"]
                                   if m["clientId"] == "z"], 30, "z to rejoin")
    assert rejoined[0] != zombie_id


def test_F_FEN_2_unfenced_zombie_regresses_offsets(make_cluster, out_dir):
    """The control: the same scenario with fencing off must regress an offset, which shows
    that F-FEN-1 can fail."""
    cluster = make_cluster(brokers=1, coordinator_settings={"coordinator.fencing.enabled": "false"})
    zombie_scenario(cluster, out_dir)
    wait_until(lambda: count_offset_regressions(coordinator_events(cluster))[0] >= 1, 20,
               "an offset regression")
    assert not (coordinator_events(cluster)["event"] == "commit_fenced").any()
