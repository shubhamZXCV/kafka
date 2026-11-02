"""E3 session-timeout sweep (LLD §16.2): figures and summary.json for a sweep directory
(runs/<id>-e3/ holding one run directory per session timeout and repeat).

    python3 analysis/plot_e3.py runs/<id>-e3

Checks: rebalance time is near-linear in the session timeout (R² of a least-squares line
≥ 0.95, NFR-AVL-02), and there are no false evictions at session timeouts of 3 s or more
(NFR-AVL-03).
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

from elog_analysis import plotting
from elog_analysis.loaders import load_events, load_run
from elog_analysis.summary import write_summary
from elog_analysis.timing import first_fault_ms, load_faults
from plot_e2 import kill_measurements

MIN_R2 = 0.95
NO_FALSE_EVICTIONS_FROM_S = 3


def false_evictions(run_dir):
    """Evictions of anyone but the killed consumer-2 (member IDs are <clientId>-<hex>)."""
    events = load_events(run_dir)
    evicted = events[events["event"] == "member_evicted"]
    return int((~evicted["member"].astype(str).str.startswith("c2-")).sum())


def collect(sweep_dir):
    rows = []
    for run_dir in sorted(p for p in Path(sweep_dir).iterdir() if (p / "run.json").exists()):
        if first_fault_ms(load_faults(run_dir), "kill") is None:
            continue  # an aborted run
        m = kill_measurements(run_dir)
        rows.append({"run": run_dir.name,
                     "session_s": int(load_run(run_dir)["params"]["session_ms"]) / 1000,
                     "rebalance_s": m["rebalance_ms"] / 1000 if m["rebalance_ms"] else np.nan,
                     "detection_s": (m["detection_ms"] or np.nan) / 1000,
                     "false_evictions": false_evictions(run_dir)})
    return pd.DataFrame(rows)


def main(sweep_dir):
    sweep_dir = Path(sweep_dir)
    runs = collect(sweep_dir)
    runs.to_csv(sweep_dir / "e3_runs.csv", index=False)
    measured = runs.dropna(subset=["rebalance_s"])
    slope, intercept = np.polyfit(measured["session_s"], measured["rebalance_s"], 1)
    predicted = slope * measured["session_s"] + intercept
    residual = ((measured["rebalance_s"] - predicted) ** 2).sum()
    r2 = 1 - residual / ((measured["rebalance_s"] - measured["rebalance_s"].mean()) ** 2).sum()
    means = measured.groupby("session_s")["rebalance_s"].mean()

    fig, ax = plotting.figure("E3 · rebalance time vs session timeout", "session timeout (s)",
                              "rebalance time (s)")
    ax.scatter(measured["session_s"], measured["rebalance_s"], color=plotting.SERIES[0],
               s=24, alpha=0.6, label="runs")
    ax.plot(means.index, means.values, color=plotting.SERIES[0], label="mean")
    ax.plot(means.index, means.index, color=plotting.SERIES[1], linestyle="--",
            label="rebalance = session")
    plotting.save(fig, sweep_dir / "e3_rebalance.png")

    evictions = runs.groupby("session_s")["false_evictions"].sum()
    fig, ax = plotting.figure("E3 · false evictions", "session timeout (s)",
                              "evictions of live members")
    ax.bar(evictions.index.astype(str), evictions.values, color=plotting.SERIES[0], width=0.6)
    plotting.save(fig, sweep_dir / "e3_false_evictions.png")

    late = runs[runs["session_s"] >= NO_FALSE_EVICTIONS_FROM_S]["false_evictions"].sum()
    checks = {
        "near-linear (NFR-AVL-02)": (r2 >= MIN_R2, f"R² {r2:.3f} (≥ {MIN_R2}), slope "
                                                   f"{slope:.2f}, intercept {intercept:.2f} s"),
        "no false evictions from 3 s (NFR-AVL-03)": (late == 0, f"{late} at ≥ 3 s"),
    }
    values = {"slope": slope, "intercept_s": intercept, "r2": r2, "runs": len(runs),
              "mean_rebalance_s": {str(k): v for k, v in means.items()},
              "false_evictions": {str(k): int(v) for k, v in evictions.items()}}
    return write_summary(sweep_dir, checks, values)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    sys.exit(0 if main(sys.argv[1]) else 1)
