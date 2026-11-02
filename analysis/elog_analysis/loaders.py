"""Loaders for the CSV files every run writes (LLD §13.1)."""

import json
from pathlib import Path

import pandas as pd

PRODUCED_COLUMNS = ["ts_ms", "producer", "topic", "partition", "base_offset", "count"]
PROCESSED_COLUMNS = ["ts_ms", "consumer", "topic", "partition", "start_offset",
                     "end_offset_excl", "owner_epoch"]


def _load(run_dir, pattern, columns):
    # A process killed before writing anything leaves an empty file: no rows.
    frames = [pd.read_csv(path) for path in sorted(Path(run_dir).glob(pattern))
              if path.stat().st_size > 0]
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


EVENT_COLUMNS = ["ts_ms", "node", "event", "group", "topic", "partition", "generation", "epoch",
                 "member", "offset", "detail"]


def load_events(run_dir):
    """Every events-*.csv row (LLD §13.1), in each file's order."""
    return _load(run_dir, "events-*.csv", EVENT_COLUMNS)



METRIC_COLUMNS = ["ts_ms", "node", "metric", "labels", "value"]


def load_metrics(run_dir, pattern="metrics-*.csv"):
    """Metric samples (LLD §13.1), one row per metric per flush; `labels` is "" if none."""
    data = _load(run_dir, pattern, METRIC_COLUMNS)
    return data.assign(labels=data["labels"].fillna(""))


def load_log(run_dir):
    """The final committed log, as experiments save it (`elogctl dump` per partition into
    log-<topic>-<partition>.jsonl): {(topic, partition): [records in offset order]}."""
    log = {}
    for path in sorted(Path(run_dir).glob("log-*.jsonl")):
        topic, partition = path.stem[len("log-"):].rsplit("-", 1)
        log[(topic, int(partition))] = [json.loads(line) for line in path.read_text().splitlines()
                                        if line.strip()]
    return log


def load_phases(run_dir):
    """{phase: ts_ms} from phases.csv, which experiment scripts write as each phase
    (warmup, measure, drain, ...) begins."""
    path = Path(run_dir) / "phases.csv"
    if not path.exists():
        return {}
    phases = pd.read_csv(path)
    return dict(zip(phases["phase"], phases["ts_ms"].astype(int)))


def load_run(run_dir):
    """run.json (LLD §13.1): experiment, seed, params, settings, host."""
    return json.loads((Path(run_dir) / "run.json").read_text())
