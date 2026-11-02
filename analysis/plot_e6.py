"""E6 leader failover (LLD §16.2): figures and summary.json for one run directory.

    python3 analysis/plot_e6.py runs/<id>

Checks: partition 0 gets a new leader that was in its ISR before the kill; every record
the producer had acknowledged is in the final log (0 committed records lost); no skips.
"""

import json
import sys
from pathlib import Path

from elog_analysis import plotting
from elog_analysis.checks import check_committed_loss, check_no_skips, count_duplicates
from elog_analysis.loaders import (load_events, load_log, load_metrics, load_phases,
                                   load_processed, load_produced)
from elog_analysis.summary import write_summary
from elog_analysis.timing import consume_rate, counter_rate, first_fault_ms, load_faults


def partition_0(path):
    topic = json.loads(Path(path).read_text())
    return next(p for p in topic["partitions"] if p["partition"] == 0)


def first_election(events, partition, after_ms):
    """(ts_ms, new leader) of partition's first leader_elected event after after_ms."""
    rows = events[(events["event"] == "leader_elected") & (events["partition"] == partition) &
                  (events["ts_ms"] > after_ms)].sort_values("ts_ms")
    if rows.empty:
        return None, None
    return int(rows["ts_ms"].iloc[0]), int(str(rows["detail"].iloc[0]).split("=")[1])


def main(run_dir):
    run_dir = Path(run_dir)
    before = partition_0(run_dir / "topic-before-kill.json")
    events, metrics, faults = load_events(run_dir), load_metrics(run_dir), load_faults(run_dir)
    kill, t0 = first_fault_ms(faults, "kill"), load_phases(run_dir)["warmup"]
    elected_ms, new_leader = first_election(events, 0, kill)

    produce, consume = counter_rate(metrics, "produce_records_total"), consume_rate(metrics)
    p99 = metrics[(metrics["metric"] == "produce_latency_ms") &
                  (metrics["labels"] == "quantile=p99")]
    p99 = p99.groupby((p99["ts_ms"] // 1000) * 1000)["value"].max()
    # Seconds in which no request completed stay empty, so the line breaks there.
    p99 = p99.reindex(range(int(p99.index.min()), int(p99.index.max()) + 1000, 1000))
    fig, axes = plotting.figure("E6 · leader of partition 0 killed", "time since start (s)",
                                "", ncols=2)
    axes[0].plot((produce.index - t0) / 1000, produce.values, label="produced")
    axes[0].plot((consume.index - t0) / 1000, consume.values, label="consumed")
    axes[0].set_ylabel("records / s")
    axes[1].plot((p99.index - t0) / 1000, p99.values, marker="o", markersize=3)
    axes[1].set_ylabel("produce latency p99 (ms)")
    for ax in axes:
        plotting.mark(ax, (kill - t0) / 1000, "kill")
    plotting.save(fig, run_dir / "e6_failover.png")

    produced, processed = load_produced(run_dir), load_processed(run_dir)
    loss = check_committed_loss(produced, load_log(run_dir))
    skips = check_no_skips(processed, produced)
    checks = {
        "new leader from the ISR": (
            new_leader is not None and new_leader != before["leader"] and
            new_leader in before["isr"],
            f"broker {before['leader']} killed (ISR {before['isr']}), new leader {new_leader}"),
        "0 committed records lost (INV-03)": (not loss, f"{len(loss)} partitions with losses"),
        "no skipped records (INV-05)": (not skips, f"{len(skips)} partitions with gaps"),
    }
    # Write unavailability: from the kill until production, after first falling below half
    # its pre-fault rate, is back above it.
    pre = produce[(produce.index >= kill - 20_000) & (produce.index < kill)].mean()
    stalled = produce[(produce.index > kill) & (produce < pre / 2)]
    back = produce[(produce.index > (stalled.index[0] if not stalled.empty else kill)) &
                   (produce >= pre / 2)]
    values = {"killed_broker": before["leader"], "new_leader": new_leader,
              "write_gap_ms": int(back.index[0]) - kill if not back.empty else None,
              "election_ms": elected_ms - kill if elected_ms else None,
              "produce_p99_max_ms": float(p99[p99.index > kill].max()) if len(p99) else None,
              "acked_records": int(produced["count"].sum()),
              "duplicates": count_duplicates(processed)[0]}
    return write_summary(run_dir, checks, values)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    sys.exit(0 if main(sys.argv[1]) else 1)
