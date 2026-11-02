"""Aggregates an experiment campaign into results/ (implementation plan T8.2):

    python3 analysis/summarize.py RUNS_DIR SINCE RESULTS_DIR

Takes every run directory in RUNS_DIR named at or after SINCE (a UTC timestamp like
20261102T120000Z) and writes to RESULTS_DIR:

- checks.csv: per experiment and check, how many runs passed it, and each run's detail;
- values.csv: per experiment and value, mean, standard deviation and count over its runs;
- tables.md: both as Markdown, for summary.md;
- figures/: the figures of each sweep, and of the first run of each repeated experiment;
- archive.txt: the name, size and SHA-256 of runs/campaign-SINCE.tar.gz, which holds the
  raw data of every run used (kept out of git; the plot scripts regenerate every figure
  from it).
"""

import hashlib
import json
import math
import shutil
import sys
import tarfile
from pathlib import Path

import pandas as pd


def experiment_of(run_dir):
    """"e3" for runs/<ts>-e3; repeated runs are named runs/<ts>-e1 likewise."""
    return run_dir.name.split("-", 1)[1].split("-")[0]


def campaign_runs(runs_dir, since):
    return sorted(p for p in Path(runs_dir).iterdir()
                  if p.is_dir() and p.name[:16] >= since and (p / "summary.json").exists())


def numeric(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def is_timestamp(name, value):
    """kill_ms, rebalance_end_ms, ...: wall-clock instants, not durations to average."""
    return name.endswith("_ms") and value > 10**12


def collect(runs):
    checks, values = [], []
    for run in runs:
        summary = json.loads((run / "summary.json").read_text())
        for name, check in summary["checks"].items():
            checks.append({"experiment": experiment_of(run), "check": name,
                           "passed": check["passed"], "detail": check["detail"]})
        for name, value in summary["values"].items():
            if numeric(value) and not is_timestamp(name, value):
                values.append({"experiment": experiment_of(run), "value": name,
                               "x": float(value)})
    return pd.DataFrame(checks), pd.DataFrame(values)


def archive(runs, runs_dir, since):
    path = Path(runs_dir) / f"campaign-{since}.tar.gz"
    with tarfile.open(path, "w:gz") as tar:
        for run in runs:
            tar.add(run, arcname=run.name)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return path, digest


def main(runs_dir, since, results_dir):
    runs = campaign_runs(runs_dir, since)
    results = Path(results_dir)
    (results / "figures").mkdir(parents=True, exist_ok=True)
    checks, values = collect(runs)

    check_table = (checks.groupby(["experiment", "check"], sort=True)
                   .agg(passed=("passed", "sum"), runs=("passed", "size"),
                        detail=("detail", lambda d: " · ".join(d)))
                   .reset_index())
    value_table = (values.groupby(["experiment", "value"], sort=True)["x"]
                   .agg(["mean", "std", "count"]).reset_index())
    check_table.to_csv(results / "checks.csv", index=False)
    value_table.to_csv(results / "values.csv", index=False)

    lines = ["| Experiment | Check | Passed | Detail |", "|---|---|---|---|"]
    lines += [f"| {r.experiment.upper()} | {r.check} | {r.passed}/{r.runs} | {r.detail} |"
              for r in check_table.itertuples()]
    lines += ["", "| Experiment | Value | Mean | SD | n |", "|---|---|---|---|---|"]
    lines += [f"| {r.experiment.upper()} | {r.value} | {r.mean:.4g} | "
              f"{'' if pd.isna(r.std) else f'{r.std:.3g}'} | {r.count} |"
              for r in value_table.itertuples()]
    (results / "tables.md").write_text("\n".join(lines) + "\n")

    copied = set()
    for run in runs:
        if experiment_of(run) in copied and not any(run.glob("*/run.json")):
            continue  # one example of each repeated experiment
        copied.add(experiment_of(run))
        for figure in run.glob("*.png"):
            shutil.copy(figure, results / "figures" / figure.name)

    path, digest = archive(runs, runs_dir, since)
    (results / "archive.txt").write_text(
        f"{path.name}  {path.stat().st_size} bytes  sha256 {digest}\n"
        f"runs: {', '.join(r.name for r in runs)}\n")
    print(f"{len(runs)} runs summarised into {results}; raw data in {path}")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        sys.exit(__doc__)
    main(*sys.argv[1:])
