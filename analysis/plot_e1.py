"""E1 steady state (LLD §16.2): figures and summary.json for one run directory.

    python3 analysis/plot_e1.py runs/<id>

Checks: per-partition order (INV-01), no skipped and no lost acked records, and stable
throughput: over the measured phase, the group's consume rate varies by at most 20 %
(coefficient of variation of the 1 s rates).
"""

import sys
from pathlib import Path

from elog_analysis import plotting
from elog_analysis.checks import (check_committed_loss, check_no_skips, check_order,
                                  count_duplicates)
from elog_analysis.loaders import (load_log, load_metrics, load_phases, load_processed,
                                   load_produced)
from elog_analysis.summary import write_summary
from elog_analysis.timing import consume_rate, counter_rate

MAX_CV = 0.20


def e2e_latency(metrics, quantile):
    """The consumers' e2e_latency_ms at `quantile`, averaged over consumers per 1 s bin."""
    rows = metrics[(metrics["metric"] == "e2e_latency_ms") &
                   (metrics["labels"] == f"quantile={quantile}")]
    return rows.groupby((rows["ts_ms"] // 1000) * 1000)["value"].mean()


def main(run_dir):
    run_dir = Path(run_dir)
    metrics, phases = load_metrics(run_dir), load_phases(run_dir)
    processed, produced, log = load_processed(run_dir), load_produced(run_dir), load_log(run_dir)
    t0, measure, drain = phases["warmup"], phases["measure"], phases["drain"]

    produce, consume = counter_rate(metrics, "produce_records_total"), consume_rate(metrics)
    measured = consume[(consume.index >= measure) & (consume.index < drain - 1000)]
    cv = measured.std() / measured.mean() if len(measured) > 1 else float("inf")
    p50, p99 = e2e_latency(metrics, "p50"), e2e_latency(metrics, "p99")
    in_phase = (p99.index >= measure) & (p99.index < drain)

    fig, ax = plotting.figure("E1 · steady-state throughput", "time since start (s)",
                              "records / s")
    ax.plot((produce.index - t0) / 1000, produce.values, label="produced")
    ax.plot((consume.index - t0) / 1000, consume.values, label="consumed")
    plotting.mark(ax, (measure - t0) / 1000, "measure")
    plotting.mark(ax, (drain - t0) / 1000, "producer stops")
    plotting.save(fig, run_dir / "e1_throughput.png")

    fig, ax = plotting.figure("E1 · end-to-end latency", "time since start (s)", "ms")
    ax.plot((p50.index - t0) / 1000, p50.values, label="p50")
    ax.plot((p99.index - t0) / 1000, p99.values, label="p99")
    plotting.mark(ax, (measure - t0) / 1000, "measure")
    plotting.save(fig, run_dir / "e1_latency.png")

    order, skips = check_order(processed, log), check_no_skips(processed, produced)
    loss = check_committed_loss(produced, log)
    checks = {
        "order (INV-01)": (not order, f"{len(order)} violations"),
        "no skipped records (INV-05)": (not skips, f"{len(skips)} partitions with gaps"),
        "no acked record lost": (not loss, f"{len(loss)} partitions with losses"),
        "stable throughput": (cv <= MAX_CV, f"CV {cv:.3f} (limit {MAX_CV})"),
    }
    values = {
        "consume_rate_mean": measured.mean(), "consume_rate_cv": cv,
        "produced_records": int(produced["count"].sum()),
        "duplicates": count_duplicates(processed)[0],
        "e2e_p50_ms_median": p50[in_phase].median(), "e2e_p99_ms_median": p99[in_phase].median(),
    }
    return write_summary(run_dir, checks, values)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    sys.exit(0 if main(sys.argv[1]) else 1)
