"""The timing measurements of LLD §13.4: rebalance time (E2, E3), time to full throughput
(E4) and backlog drain rate (E9). Inputs are the frames of loaders.py plus faults.csv.
Times are wall-clock ms, as in every CSV; every process shares the host's clock.
"""

from pathlib import Path

import numpy as np
import pandas as pd


def load_faults(run_dir):
    """faults.csv (LLD §13.1), or an empty frame if no fault was injected."""
    path = Path(run_dir) / "faults.csv"
    if not path.exists():
        return pd.DataFrame(columns=["ts_ms", "action", "target", "params"])
    return pd.read_csv(path, keep_default_na=False)


def first_fault_ms(faults, action):
    rows = faults[faults["action"] == action]
    return int(rows["ts_ms"].iloc[0]) if not rows.empty else None


def first_rebalance_end_after(events, ts_ms, group=None):
    """(ts_ms, duration_ms) of the first rebalance_end after ts_ms, or None."""
    ends = events[(events["event"] == "rebalance_end") & (events["ts_ms"] > ts_ms)]
    if group is not None:
        ends = ends[ends["group"] == group]
    if ends.empty:
        return None
    row = ends.sort_values("ts_ms").iloc[0]
    return int(row["ts_ms"]), _detail(row["detail"]).get("duration_ms")


def rebalance_time_ms(events, faults, action="kill", group=None):
    """E2: rebalance_end.ts − faults[action].ts, for the first rebalance after the fault."""
    fault = first_fault_ms(faults, action)
    end = first_rebalance_end_after(events, fault, group) if fault is not None else None
    return None if end is None else end[0] - fault


def consume_rate(metrics):
    """Records consumed per second by the whole group: Σ consume_records_total increments,
    in 1 s bins. Indexed by bin start (ms). A restarted consumer's counter starts at 0
    again, so a decrease counts as the new value, not a negative increment."""
    rows = metrics[metrics["metric"] == "consume_records_total"].sort_values("ts_ms")
    increments = []
    for _, series in rows.groupby(["node", "labels"]):
        values = series["value"].to_numpy()
        diff = np.diff(values, prepend=0)
        diff[diff < 0] = values[diff < 0]
        increments.append(pd.Series(diff, index=series["ts_ms"].to_numpy()))
    if not increments:
        return pd.Series(dtype=float)
    all_increments = pd.concat(increments)
    bins = (all_increments.index // 1000) * 1000
    return all_increments.groupby(bins).sum().sort_index()


def time_to_full_throughput_ms(metrics, events, faults, action="kill", baseline_s=20,
                               fraction=0.95):
    """E4: from rebalance_end to the start of the first 1 s window whose group consume rate
    is at least `fraction` of the mean over the `baseline_s` seconds before the fault."""
    fault = first_fault_ms(faults, action)
    end = first_rebalance_end_after(events, fault) if fault is not None else None
    if end is None:
        return None
    rate = consume_rate(metrics)
    baseline = rate[(rate.index >= fault - baseline_s * 1000) & (rate.index < fault - 1000)]
    if baseline.empty:
        return None
    target = fraction * baseline.mean()
    after = rate[(rate.index >= end[0] - end[0] % 1000) & (rate >= target)]
    return None if after.empty else max(0, int(after.index[0]) - end[0])


def total_lag(metrics):
    """Σ over partitions of consumer_lag_records, in 1 s bins. Per partition the latest
    sample in the bin counts, whichever consumer reported it (owners change)."""
    rows = metrics[metrics["metric"] == "consumer_lag_records"].copy()
    rows["bin"] = (rows["ts_ms"] // 1000) * 1000
    latest = rows.sort_values("ts_ms").groupby(["bin", "labels"])["value"].last()
    per_partition = latest.unstack("labels").ffill()  # a partition not sampled keeps its lag
    return per_partition.sum(axis=1)


def drain_rate(metrics, start_ms, floor=0.02):
    """E9: records/s by which the total lag falls after start_ms (the rebalance end): minus
    the slope of a least-squares line through the lag from start_ms until it first drops
    below `floor` of its value then (or to the end of the run). Negative if the backlog
    grows; None with fewer than 3 samples."""
    lag = total_lag(metrics)
    lag = lag[lag.index >= start_ms - start_ms % 1000]
    if len(lag) < 3:
        return None
    drained = lag[lag <= floor * lag.iloc[0]]
    window = lag.loc[:drained.index[0]] if not drained.empty else lag
    if len(window) < 3:
        return None
    slope = np.polyfit((window.index - window.index[0]) / 1000.0, window.to_numpy(), 1)[0]
    return -slope


def _detail(text):
    """`k=v;k=v` from an event's detail column, values as int where possible."""
    out = {}
    for part in str(text).split(";"):
        if "=" in part:
            key, value = part.split("=", 1)
            out[key] = int(value) if value.lstrip("-").isdigit() else value
    return out
