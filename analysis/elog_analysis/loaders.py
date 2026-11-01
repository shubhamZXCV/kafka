"""Loaders for the CSV files every run writes (LLD §13.1)."""

from pathlib import Path

import pandas as pd

PRODUCED_COLUMNS = ["ts_ms", "producer", "topic", "partition", "base_offset", "count"]
PROCESSED_COLUMNS = ["ts_ms", "consumer", "topic", "partition", "start_offset",
                     "end_offset_excl", "owner_epoch"]


def _load(run_dir, pattern, columns):
    frames = [pd.read_csv(path) for path in sorted(Path(run_dir).glob(pattern))]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(columns=columns)
    data = pd.concat(frames, ignore_index=True)
    missing = set(columns) - set(data.columns)
    if missing:
        raise ValueError(f"{pattern} in {run_dir}: missing columns {sorted(missing)}")
    return data[columns]


def load_produced(run_dir):
    """Acked offset runs from every produced-*.csv: one row per (base_offset, count)."""
    return _load(run_dir, "produced-*.csv", PRODUCED_COLUMNS)


def load_processed(run_dir):
    """Processed offset ranges from every processed-*.csv, in each file's write order."""
    return _load(run_dir, "processed-*.csv", PROCESSED_COLUMNS)
