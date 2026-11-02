"""E2 consumer kill (LLD §16.2): figures and summary.json for one run directory.

    python3 analysis/plot_e2.py runs/<id>

Checks: the rebalance completes within session timeout + 500 ms of the kill (NFR-AVL-01)
and no acked record is skipped (INV-05).
"""

import sys
from pathlib import Path

from elog_analysis import plotting
from elog_analysis.checks import check_no_skips, check_order, count_duplicates
from elog_analysis.loaders import (load_events, load_metrics, load_phases, load_processed,
                                   load_produced, load_run)
from elog_analysis.summary import write_summary
from elog_analysis.timing import (backlog, consume_rate, counter_rate, drain_rate,
                                  first_fault_ms, first_rebalance_end_after, load_faults,
                                  time_to_full_throughput_ms)

MARGIN_MS = 500


def plot_kill(run_dir, name, t0, kill, rebalance_end):
    """<name>_throughput.png and <name>_lag.png with the kill and rebalance marked; shared
    with E3, E4 and E9, which run the same scenario."""
    metrics = load_metrics(run_dir)
    produce, consume = counter_rate(metrics, "produce_records_total"), consume_rate(metrics)
    lag = backlog(metrics)
    fig, ax = plotting.figure(f"{name.upper()} · throughput around a consumer kill",
                              "time since start (s)", "records / s")
    ax.plot((produce.index - t0) / 1000, produce.values, label="produced")
    ax.plot((consume.index - t0) / 1000, consume.values, label="consumed")
    plotting.mark(ax, (kill - t0) / 1000, "kill")
    if rebalance_end:
        plotting.mark(ax, (rebalance_end - t0) / 1000, "rebalanced")
    plotting.save(fig, Path(run_dir) / f"{name}_throughput.png")

    fig, ax = plotting.figure(f"{name.upper()} · backlog (acked, not yet processed)",
                              "time since start (s)", "records")
    ax.plot((lag.index - t0) / 1000, lag.values)
    plotting.mark(ax, (kill - t0) / 1000, "kill")
    plotting.save(fig, Path(run_dir) / f"{name}_lag.png")


def kill_measurements(run_dir):
    """The timing values every kill-scenario experiment reports."""
    events, metrics, faults = load_events(run_dir), load_metrics(run_dir), load_faults(run_dir)
    kill = first_fault_ms(faults, "kill")
    lag = backlog(metrics)
    steady = lag[(lag.index >= kill - 20_000) & (lag.index < kill)].median()  # in the pipeline
    end = first_rebalance_end_after(events, kill)
    evicted = events[(events["event"] == "member_evicted") & (events["ts_ms"] < kill)]
    starts = events[(events["event"] == "rebalance_start") & (events["ts_ms"] > kill)]
    detected = int(starts["ts_ms"].min()) if not starts.empty else None
    return {
        "kill_ms": kill,
        "rebalance_end_ms": end[0] if end else None,
        "rebalance_ms": end[0] - kill if end else None,
        # The two parts: session expiry (session − time since the last heartbeat), then the
        # join barrier (survivors learn of the rebalance on their next heartbeat).
        "detection_ms": detected - kill if detected else None,
        "barrier_ms": end[1] if end else None,
        "time_to_full_throughput_ms": time_to_full_throughput_ms(metrics, events, faults),
        "drain_rate": drain_rate(lag, end[0], baseline=steady) if end else None,
        "false_evictions": len(evicted),  # before the kill nobody should be evicted
    }


def main(run_dir):
    run_dir = Path(run_dir)
    session = int(load_run(run_dir)["params"]["session_ms"])
    m = kill_measurements(run_dir)
    plot_kill(run_dir, "e2", load_phases(run_dir)["warmup"], m["kill_ms"], m["rebalance_end_ms"])

    processed = load_processed(run_dir)
    skips = check_no_skips(processed, load_produced(run_dir))
    order = check_order(processed, {})
    rebalance = m["rebalance_ms"]
    checks = {
        "rebalance within session + 500 ms (NFR-AVL-01)": (
            rebalance is not None and rebalance < session + MARGIN_MS,
            f"{rebalance} ms with a {session} ms session"),
        "no skipped records (INV-05)": (not skips, f"{len(skips)} partitions with gaps"),
        "order within an epoch (INV-01)": (not order, f"{len(order)} violations"),
    }
    values = {**m, "session_ms": session, "duplicates": count_duplicates(processed)[0]}
    return write_summary(run_dir, checks, values)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    sys.exit(0 if main(sys.argv[1]) else 1)
