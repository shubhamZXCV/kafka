"""E7 overload backpressure (LLD §16.2): figures and summary.json for a sweep directory
(runs/<id>-e7/ holding one run per queue size Q and repeat).

    python3 analysis/plot_e7.py runs/<id>-e7

Checks, per run: inflight_depth never exceeds Q (FR-BP-01); each consumer's RSS grows by
at most RSS_SLACK over the measured phase (flat at its bound, FR-BP-02); the backlog not
yet fetched (Σ consumer_lag_records) grows, i.e. it waits on the brokers' disks.
consumer_lag_records is set when a partition is fetched, so the backlog curve rises in steps.
"""

import sys
from pathlib import Path

import pandas as pd

from elog_analysis import plotting
from elog_analysis.loaders import load_metrics, load_phases, load_run
from elog_analysis.summary import write_summary
from elog_analysis.timing import consume_rate, total_lag

RSS_SLACK = 8 * 2**20
CONSUMERS = ["c1", "c2", "c3", "c4"]


def measure_run(run_dir):
    metrics, phases = load_metrics(run_dir), load_phases(run_dir)
    start, stop = phases["measure"], phases["stop"]
    in_phase = metrics[(metrics["ts_ms"] >= start) & (metrics["ts_ms"] < stop)]
    depth = in_phase[in_phase["metric"] == "inflight_depth"]["value"]
    rss = in_phase[(in_phase["metric"] == "process_rss_bytes") &
                   (in_phase["node"].isin(CONSUMERS))]
    growth = max(g["value"].max() - g["value"].iloc[0] for _, g in rss.groupby("node"))
    lag = total_lag(metrics)
    lag = lag[(lag.index >= start) & (lag.index < stop)]
    rate = consume_rate(metrics)
    return {"run": Path(run_dir).name, "queue": int(load_run(run_dir)["params"]["queue"]),
            "max_depth": int(depth.max()), "rss_mb": rss["value"].mean() / 2**20,
            "rss_growth_mb": growth / 2**20, "lag_start": lag.iloc[0], "lag_end": lag.iloc[-1],
            "consume_rate": rate[(rate.index >= start) & (rate.index < stop)].mean(),
            "metrics": metrics, "t0": start}


def plot_series(runs, metric, ylabel, title, path, scale=1.0):
    """One line per queue size (first repeat): `metric` summed over the consumers."""
    fig, ax = plotting.figure(title, "time in the measured phase (s)", ylabel)
    for _, run in runs.drop_duplicates("queue").sort_values("queue").iterrows():
        m = run["metrics"]
        if metric == "lag":
            series = total_lag(m)
        else:
            rows = m[(m["metric"] == metric) & (m["node"].isin(CONSUMERS))]
            series = rows.groupby((rows["ts_ms"] // 1000) * 1000)["value"].mean()
        series = series[series.index >= run["t0"]]
        ax.plot((series.index - run["t0"]) / 1000, series.values * scale,
                label=f"Q = {run['queue']:,}")
    plotting.save(fig, path)


def main(sweep_dir):
    sweep_dir = Path(sweep_dir)
    runs = pd.DataFrame([measure_run(p) for p in sorted(Path(sweep_dir).iterdir())
                         if (p / "phases.csv").exists()])
    runs.drop(columns=["metrics", "t0"]).to_csv(sweep_dir / "e7_runs.csv", index=False)
    plot_series(runs, "inflight_depth", "records (mean per consumer)",
                "E7 · in-flight queue depth", sweep_dir / "e7_depth.png")
    plot_series(runs, "process_rss_bytes", "MB (mean per consumer)", "E7 · consumer RSS",
                sweep_dir / "e7_rss.png", scale=1 / 2**20)
    plot_series(runs, "lag", "records", "E7 · backlog not yet fetched (on the brokers)",
                sweep_dir / "e7_backlog.png")

    over = runs[runs["max_depth"] > runs["queue"]]
    grew = runs[runs["rss_growth_mb"] > RSS_SLACK / 2**20]
    flat_backlog = runs[runs["lag_end"] <= runs["lag_start"]]
    checks = {
        "depth ≤ Q (FR-BP-01)": (over.empty, f"{len(over)} runs over their Q"),
        "RSS flat (FR-BP-02)": (grew.empty, f"max growth {runs['rss_growth_mb'].max():.1f} MB "
                                            f"(limit {RSS_SLACK / 2**20:.0f} MB)"),
        "backlog grows on disk": (flat_backlog.empty, f"{len(flat_backlog)} runs without"),
    }
    per_queue = runs.groupby("queue")[["max_depth", "rss_mb", "rss_growth_mb",
                                       "consume_rate"]].mean()
    values = {f"q{q}_{k}": v for q, row in per_queue.iterrows() for k, v in row.items()}
    return write_summary(sweep_dir, checks, values)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    sys.exit(0 if main(sys.argv[1]) else 1)
