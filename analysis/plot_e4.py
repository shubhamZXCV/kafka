"""E4 eager vs. sticky (LLD §16.2): figures and summary.json for a sweep directory
(runs/<id>-e4/ holding one round-robin and one sticky run per repeat).

    python3 analysis/plot_e4.py runs/<id>-e4

Check: sticky reaches full throughput 40–70 % sooner than round-robin (SRS §9.1, E4),
comparing the mean time to full throughput (LLD §13.4) of the two arms.
"""

import sys
from pathlib import Path

import pandas as pd

from elog_analysis import plotting
from elog_analysis.checks import count_duplicates
from elog_analysis.loaders import load_events, load_metrics, load_processed, load_run
from elog_analysis.summary import write_summary
from elog_analysis.timing import consume_rate
from plot_e2 import kill_measurements

ARMS = ["roundrobin", "sticky"]
PREDICTED = (0.40, 0.70)  # sticky's time to full throughput is 40–70 % shorter


def moved_partitions(run_dir, kill_ms):
    events = load_events(run_dir)
    ends = events[(events["event"] == "rebalance_end") & (events["ts_ms"] > kill_ms)]
    detail = str(ends.sort_values("ts_ms")["detail"].iloc[0]) if not ends.empty else ""
    return next((int(p.split("=")[1]) for p in detail.split(";") if p.startswith("moved=")),
                None)


def collect(sweep_dir):
    rows = []
    for run_dir in sorted(p for p in Path(sweep_dir).iterdir() if (p / "faults.csv").exists()):
        m = kill_measurements(run_dir)
        rows.append({"run": run_dir.name, "assignor": load_run(run_dir)["params"]["assignor"],
                     "ttft_ms": m["time_to_full_throughput_ms"],
                     "rebalance_ms": m["rebalance_ms"],
                     "moved": moved_partitions(run_dir, m["kill_ms"]),
                     "duplicates": count_duplicates(load_processed(run_dir))[0],
                     "path": run_dir, "rebalance_end_ms": m["rebalance_end_ms"]})
    return pd.DataFrame(rows)


def plot_recovery(runs, path):
    """The group's consume rate around the rebalance, the first run of each arm."""
    fig, ax = plotting.figure("E4 · consume rate after the rebalance",
                              "time since the rebalance ended (s)", "records / s")
    for assignor in ARMS:
        run = runs[runs["assignor"] == assignor].iloc[0]
        rate = consume_rate(load_metrics(run["path"]))
        t = (rate.index - run["rebalance_end_ms"]) / 1000
        window = (t >= -15) & (t <= 30)
        ax.plot(t[window], rate.values[window], label=assignor)
    plotting.mark(ax, 0, "rebalanced")
    plotting.save(fig, path)


def main(sweep_dir):
    sweep_dir = Path(sweep_dir)
    runs = collect(sweep_dir)
    runs.drop(columns=["path"]).to_csv(sweep_dir / "e4_runs.csv", index=False)
    means = runs.groupby("assignor")[["ttft_ms", "moved", "duplicates"]].mean()

    fig, ax = plotting.figure("E4 · time to full throughput", "assignor", "ms")
    ax.bar(ARMS, [means.loc[a, "ttft_ms"] for a in ARMS], color=plotting.SERIES[0], width=0.5)
    for i, assignor in enumerate(ARMS):
        values = runs[runs["assignor"] == assignor]["ttft_ms"]
        ax.scatter([i] * len(values), values, color=plotting.INK_SECONDARY, s=16, zorder=3)
    plotting.save(fig, sweep_dir / "e4_ttft.png")
    plot_recovery(runs, sweep_dir / "e4_recovery.png")

    eager, sticky = means.loc["roundrobin", "ttft_ms"], means.loc["sticky", "ttft_ms"]
    faster = (eager - sticky) / eager if eager else float("nan")
    checks = {"sticky 40–70 % faster (SRS §9.1)": (
        PREDICTED[0] <= faster <= PREDICTED[1],
        f"{faster:.0%} faster: {sticky:.0f} ms vs {eager:.0f} ms")}
    values = {"faster_fraction": faster,
              **{f"{a}_{k}": means.loc[a, k] for a in ARMS for k in means.columns}}
    return write_summary(sweep_dir, checks, values)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    sys.exit(0 if main(sys.argv[1]) else 1)
