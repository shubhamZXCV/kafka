"""E5 zombie fencing (LLD §16.2): figures and summary.json for a sweep directory
(runs/<id>-e5/ holding a fenced and an unfenced run per repeat).

    python3 analysis/plot_e5.py runs/<id>-e5

Checks (SRS §9.1, the headline result): with fencing, 0 offset regressions and every
stale commit rejected; without fencing (the control), a regression in every run whose
zombie sent a stale commit; no skipped record in either. A stale commit is one the zombie
sends after SIGCONT under its evicted member ID. Whether it sends one is a race: if its
overdue heartbeat reports the eviction before its next commit is due, it drops its work
and rejoins without committing. Each arm needs at least one run with a stale commit.
"""

import sys
from pathlib import Path

import pandas as pd

from elog_analysis import plotting
from elog_analysis.checks import check_no_skips, count_duplicates, count_offset_regressions
from elog_analysis.loaders import load_events, load_processed, load_produced, load_run
from elog_analysis.summary import write_summary
from elog_analysis.timing import load_faults

ARMS = ["fenced", "unfenced"]


def stale_commits(events, resume_ms):
    """Commit entries the zombie sent after waking, under its evicted member ID, accepted
    or fenced."""
    evicted = set(events[events["event"] == "member_evicted"]["member"])
    sent = events[events["event"].isin(["commit_accepted", "commit_fenced"])
                  & events["member"].isin(evicted) & (events["ts_ms"] > resume_ms)]
    return len(sent)


def sent(arm_runs):
    """How many of an arm's runs sent at least one stale commit."""
    return int((arm_runs["stale_commits"] > 0).sum())


def collect(sweep_dir):
    rows = []
    for run_dir in sorted(p for p in Path(sweep_dir).iterdir() if (p / "faults.csv").exists()):
        events, processed = load_events(run_dir), load_processed(run_dir)
        fenced = load_run(run_dir)["params"]["fencing"] == "true"
        faults = load_faults(run_dir)
        resume = int(faults[faults["action"] == "resume"]["ts_ms"].iloc[0])
        rows.append({"run": run_dir.name, "arm": "fenced" if fenced else "unfenced",
                     "stale_commits": stale_commits(events, resume),
                     "regressions": count_offset_regressions(events)[0],
                     "fenced_commits": int((events["event"] == "commit_fenced").sum()),
                     "duplicates": count_duplicates(processed)[0],
                     "skips": len(check_no_skips(processed, load_produced(run_dir))),
                     "path": run_dir})
    return pd.DataFrame(rows)


def plot_commits(run_dir, ax, title):
    """Accepted committed offsets of each partition over time, with the pause marked."""
    events, faults = load_events(run_dir), load_faults(run_dir)
    pause, resume = (int(faults[faults["action"] == a]["ts_ms"].iloc[0])
                     for a in ("pause", "resume"))
    accepted = events[events["event"] == "commit_accepted"]
    for i, (partition, rows) in enumerate(accepted.groupby("partition")):
        if i < len(plotting.SERIES):  # three series at most; the fourth looks the same
            window = (rows["ts_ms"] > pause - 10_000) & (rows["ts_ms"] < resume + 15_000)
            ax.plot((rows["ts_ms"][window] - pause) / 1000, rows["offset"][window],
                    label=f"partition {int(partition)}", linewidth=1.5)
    plotting.mark(ax, 0, "SIGSTOP")
    plotting.mark(ax, (resume - pause) / 1000, "SIGCONT")
    ax.set_title(title)


def main(sweep_dir):
    sweep_dir = Path(sweep_dir)
    runs = collect(sweep_dir)
    runs.drop(columns=["path"]).to_csv(sweep_dir / "e5_runs.csv", index=False)
    totals = runs.groupby("arm")[["stale_commits", "regressions", "fenced_commits",
                                  "duplicates", "skips"]].sum()

    fig, ax = plotting.figure("E5 · offset regressions (all runs)", "", "regressions")
    ax.bar(["fencing on", "fencing OFF (control)"],
           [totals.loc[a, "regressions"] for a in ARMS], color=plotting.SERIES[0], width=0.5)
    plotting.save(fig, sweep_dir / "e5_regressions.png")

    fig, axes = plotting.figure("E5 · committed offsets around the zombie's pause",
                                "time since SIGSTOP (s)", "committed offset", ncols=2)
    for ax, arm, title in zip(axes, ARMS, ["fencing on", "fencing OFF (control)"]):
        plot_commits(runs[runs["arm"] == arm].iloc[0]["path"], ax, title)
    plotting.save(fig, sweep_dir / "e5_commits.png")

    fenced, unfenced = (runs[runs["arm"] == arm] for arm in ARMS)
    checks = {
        "fenced: 0 regressions (SRS §9.1)": (totals.loc["fenced", "regressions"] == 0,
                                            f"{totals.loc['fenced', 'regressions']}"),
        "fenced: every stale commit rejected": (
            totals.loc["fenced", "stale_commits"] > 0
            and (fenced["fenced_commits"] == fenced["stale_commits"]).all(),
            f"{totals.loc['fenced', 'fenced_commits']} of "
            f"{totals.loc['fenced', 'stale_commits']} rejected; "
            f"{sent(fenced)} of {len(fenced)} runs sent one"),
        "unfenced control: regressions > 0": (
            totals.loc["unfenced", "stale_commits"] > 0
            and (unfenced[unfenced["stale_commits"] > 0]["regressions"] > 0).all(),
            f"{totals.loc['unfenced', 'regressions']} in total; "
            f"{sent(unfenced)} of {len(unfenced)} runs sent a stale commit"),
        "no skipped records (INV-05)": (totals["skips"].sum() == 0,
                                        f"{totals['skips'].sum()} partitions with gaps"),
    }
    values = {f"{arm}_{k}": int(totals.loc[arm, k]) for arm in ARMS for k in totals.columns}
    return write_summary(sweep_dir, checks, values)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    sys.exit(0 if main(sys.argv[1]) else 1)
