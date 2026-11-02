"""E8 crash duplicates (LLD §16.2): figures and summary.json for a sweep directory
(runs/<id>-e8/ holding one run per commit interval N, each with many crashes).

    python3 analysis/plot_e8.py runs/<id>-e8

Every restart is a new member, so its records carry a new ownership epoch: the duplicates
one crash caused are the overlap between what the consumer processed in one epoch and in
all later ones. Checks (FR-DEL-03, INV-10): no crash causes more than N duplicates; the mean is
within 20 % of (N − 1) / 2, the mean of k ~ Uniform[0, N); the mean grows linearly in N
with slope 0.4–0.6; the measured duplicates equal the k the crash hook logged; no skips.
"""

import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from matplotlib.ticker import MaxNLocator

from elog_analysis import plotting
from elog_analysis.checks import _merged, check_no_skips
from elog_analysis.loaders import load_processed, load_produced, load_run
from elog_analysis.summary import write_summary

HOOK = re.compile(r"crash_hook_fired processed_since_commit=(\d+)")


def overlap(a, b):
    """Offsets covered by both lists of [start, end) ranges."""
    total, b = 0, _merged(b)
    for start, end in _merged(a):
        total += sum(max(0, min(end, e) - max(start, s)) for s, e in b)
    return total


def duplicates_per_crash(processed):
    """Per ownership epoch but the last, the records processed both in it and in any later
    epoch: what its crash made the next consumer redo. (A restart can take more than one
    generation, so the redelivery need not all fall in the next epoch.) Epochs in which
    nothing was processed are skipped."""
    per_epoch = {}
    for (epoch, partition), rows in processed.groupby(["owner_epoch", "partition"]):
        per_epoch.setdefault(epoch, {})[partition] = list(
            zip(rows["start_offset"], rows["end_offset_excl"]))
    epochs = sorted(per_epoch)
    return [sum(overlap(ranges, [r for later in epochs[i + 1:]
                                 for r in per_epoch[later].get(p, [])])
                for p, ranges in per_epoch[epoch].items())
            for i, epoch in enumerate(epochs[:-1])]


def collect(sweep_dir):
    runs = []
    for run_dir in sorted(p for p in Path(sweep_dir).iterdir() if (p / "run.json").exists()):
        processed = load_processed(run_dir)
        log = (run_dir / "crashes.log").read_text(errors="replace")
        runs.append({"run": run_dir.name,
                     "interval": int(load_run(run_dir)["params"]["commit_interval"]),
                     "duplicates": duplicates_per_crash(processed),
                     "hook_k": [int(k) for k in HOOK.findall(log)],
                     "skips": len(check_no_skips(processed, load_produced(run_dir)))})
    return runs


def main(sweep_dir):
    sweep_dir = Path(sweep_dir)
    runs = collect(sweep_dir)
    rows = pd.DataFrame([{"run": r["run"], "interval": r["interval"], "crash": i + 1,
                          "duplicates": d} for r in runs for i, d in enumerate(r["duplicates"])])
    rows.to_csv(sweep_dir / "e8_crashes.csv", index=False)
    by_n = rows.groupby("interval")["duplicates"]
    stats = pd.DataFrame({"mean": by_n.mean(), "max": by_n.max(), "crashes": by_n.size()})
    slope, intercept = np.polyfit(stats.index, stats["mean"], 1)

    fig, axes = plotting.figure("E8 · duplicates per crash", "duplicates", "crashes",
                                ncols=len(stats))
    for ax, (n, values) in zip(np.atleast_1d(axes), by_n):
        ax.hist(values, bins=10, range=(0, n), color=plotting.SERIES[0], rwidth=0.9)
        ax.set_title(f"N = {n}")
        ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    plotting.save(fig, sweep_dir / "e8_histograms.png")

    fig, ax = plotting.figure("E8 · mean duplicates per crash vs commit interval",
                              "commit interval N (records)", "duplicates per crash")
    ax.plot(stats.index, stats["mean"], marker="o", label="measured mean")
    ax.plot(stats.index, (stats.index - 1) / 2, linestyle="--", label="(N − 1) / 2")
    plotting.save(fig, sweep_dir / "e8_mean.png")

    expected = (stats.index - 1) / 2
    off = (stats["mean"] - expected).abs() > 0.2 * expected
    measured, hooked = sum(rows["duplicates"]), sum(sum(r["hook_k"]) for r in runs)
    skips = sum(r["skips"] for r in runs)
    checks = {
        "duplicates ≤ N (INV-10)": ((stats["max"] <= stats.index).all(),
                                    ", ".join(f"N={n}: max {m}" for n, m in stats["max"].items())),
        "mean ≈ (N − 1) / 2": (not off.any(),
                               ", ".join(f"N={n}: {m:.1f}" for n, m in stats["mean"].items())),
        "linear in N": (0.4 <= slope <= 0.6, f"slope {slope:.3f}"),
        "duplicates = k from the hook": (measured == hooked, f"{measured} measured, {hooked} k"),
        "no skipped records (INV-05)": (skips == 0, f"{skips} partitions with gaps"),
    }
    values = {"slope": slope, "intercept": intercept,
              **{f"n{n}_{k}": v for n, row in stats.iterrows() for k, v in row.items()}}
    return write_summary(sweep_dir, checks, values)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    sys.exit(0 if main(sys.argv[1]) else 1)
