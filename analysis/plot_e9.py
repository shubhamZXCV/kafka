"""E9 backlog drain (LLD §16.2): figure and summary.json for one run directory.

    python3 analysis/plot_e9.py runs/<id>

Check (NFR-AVL-06): the backlog drains after the rebalance at the 3 survivors' capacity
minus the producer rate, within 25 %. The capacity per consumer is the run's rate divided
by its load share (run.json).
"""

import sys
from pathlib import Path

import numpy as np

from elog_analysis import plotting
from elog_analysis.loaders import load_metrics, load_phases, load_run
from elog_analysis.summary import write_summary
from elog_analysis.timing import backlog, consume_rate, drain_window
from plot_e2 import kill_measurements

TOLERANCE = 0.25


def main(run_dir):
    run_dir = Path(run_dir)
    params = load_run(run_dir)["params"]
    rate, load = int(params["rate"]), int(params["load_pct"]) / 100
    capacity = rate / load / int(params["consumers"])
    predicted = (int(params["consumers"]) - 1) * capacity - rate
    m = kill_measurements(run_dir)
    measured = m["drain_rate"]

    metrics = load_metrics(run_dir)
    lag, t0, end = backlog(metrics), load_phases(run_dir)["warmup"], m["rebalance_end_ms"]
    # What the survivors actually processed while a backlog remained: their capacity under
    # this load, to compare with the calibrated one in the prediction.
    steady = lag[(lag.index >= m["kill_ms"] - 20_000) & (lag.index < m["kill_ms"])].median()
    window = drain_window(lag, end, steady) if end else lag.iloc[:0]
    survivors_rate = consume_rate(metrics).reindex(window.index).mean() if len(window) else None
    fig, ax = plotting.figure("E9 · backlog drain after a consumer kill",
                              "time since start (s)", "records (acked, not yet processed)")
    ax.plot((lag.index - t0) / 1000, lag.values, label="backlog")
    if end and measured:
        start = lag[lag.index >= end - end % 1000].iloc[0]
        span = np.arange(0, start / predicted if predicted > 0 else 30, 1)
        ax.plot((end - t0) / 1000 + span, start - predicted * span, linestyle="--",
                label=f"predicted drain ({predicted:.0f} /s)")
    plotting.mark(ax, (m["kill_ms"] - t0) / 1000, "kill")
    plotting.save(fig, run_dir / "e9_drain.png")

    ok = measured is not None and abs(measured - predicted) <= TOLERANCE * predicted
    checks = {"drain ≈ capacity − producer rate (NFR-AVL-06)": (
        ok, f"{measured if measured is None else round(measured)} /s measured, "
            f"{predicted:.0f} /s predicted (±{TOLERANCE:.0%})")}
    values = {**m, "drain_rate_predicted": predicted, "capacity_per_consumer": capacity,
              "rate": rate, "survivors_rate_while_draining": survivors_rate,
              "effective_capacity_per_consumer":
                  survivors_rate / (int(params["consumers"]) - 1) if survivors_rate else None}
    return write_summary(run_dir, checks, values)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    sys.exit(0 if main(sys.argv[1]) else 1)
