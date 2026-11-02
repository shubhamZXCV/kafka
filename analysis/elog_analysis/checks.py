"""The correctness checks of LLD §13.4. Each returns a list of violations (empty = pass),
so a test can assert on it and an experiment can count it.

`log` arguments are the final log as `elogctl dump` gives it:
{(topic, partition): [{"offset", "key", "pid", "seq"}, ...]} in offset order.
"""


def check_order(processed, log):
    """INV-01 ordering.

    1. Per consumer and partition, within one ownership epoch, processed offsets only move
       forward: each range starts at or after the end of the one before it.
    2. Per key, `seq` from one producer never decreases along the log (a retried batch may
       repeat it: at-least-once).
    """
    violations = []
    for (consumer, topic, partition, epoch), ranges in processed.groupby(
            ["consumer", "topic", "partition", "owner_epoch"], sort=False):
        end = None
        for start, stop in zip(ranges["start_offset"], ranges["end_offset_excl"]):
            if end is not None and start < end:
                violations.append(f"{consumer} {topic}-{partition} epoch {epoch}: range "
                                  f"[{start}, {stop}) after one ending at {end}")
            end = stop
    for (topic, partition), records in log.items():
        last_seq = {}
        for r in records:
            if r["key"] is None or r["seq"] is None:
                continue
            key = (r["pid"], r["key"])
            if key in last_seq and r["seq"] < last_seq[key]:
                violations.append(f"{topic}-{partition} offset {r['offset']}: key {r['key']} "
                                  f"seq {r['seq']} after {last_seq[key]}")
            last_seq[key] = r["seq"]
    return violations


def check_committed_loss(produced, log):
    """Every acked (topic, partition, offset) in produced-*.csv is in the final log (E6)."""
    present = {tp: {r["offset"] for r in records} for tp, records in log.items()}
    violations = []
    for row in produced.itertuples(index=False):
        offsets = present.get((row.topic, row.partition), set())
        lost = [o for o in range(row.base_offset, row.base_offset + row.count)
                if o not in offsets]
        if lost:
            violations.append(f"{row.topic}-{row.partition}: {len(lost)} acked offsets "
                              f"missing, first {lost[0]}")
    return violations


def check_skipped(processed, high_watermarks):
    """INV-05: the union of processed ranges covers [0, HW) of every partition.

    `high_watermarks` is {(topic, partition): final HW}.
    """
    violations = []
    for (topic, partition), hw in high_watermarks.items():
        rows = processed[(processed["topic"] == topic) & (processed["partition"] == partition)]
        covered = 0
        for start, stop in sorted(zip(rows["start_offset"], rows["end_offset_excl"])):
            if start > covered:
                break
            covered = max(covered, stop)
        if covered < hw:
            violations.append(f"{topic}-{partition}: processed up to {covered} of {hw}")
    return violations


def _merged(ranges):
    """Sorted, non-overlapping [start, end) ranges covering the same offsets."""
    merged = []
    for start, end in sorted(ranges):
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return merged


def check_no_skips(processed, produced):
    """INV-05 against the producers: every acked offset in produced-*.csv lies in some
    processed range. Measured against what was produced, not what consumers saw, so a
    partition no consumer ever read still counts."""
    violations = []
    for (topic, partition), acked in produced.groupby(["topic", "partition"]):
        rows = processed[(processed["topic"] == topic) & (processed["partition"] == partition)]
        covered = _merged(zip(rows["start_offset"], rows["end_offset_excl"]))
        for base, count in zip(acked["base_offset"], acked["count"]):
            for start, end in _merged([(base, base + count)]):
                pos = start
                for c_start, c_end in covered:
                    if c_start <= pos < c_end:
                        pos = c_end
                if pos < end:
                    violations.append(f"{topic}-{partition}: acked offset {pos} never processed")
                    break
    return violations


def count_duplicates(processed):
    """Records processed more than once, per (topic, partition) and in total (E8):
    the sum of range lengths minus the distinct offsets they cover."""
    per_partition = {}
    for (topic, partition), rows in processed.groupby(["topic", "partition"]):
        ranges = list(zip(rows["start_offset"], rows["end_offset_excl"]))
        total = sum(end - start for start, end in ranges)
        distinct = sum(end - start for start, end in _merged(ranges))
        per_partition[(topic, partition)] = total - distinct
    return sum(per_partition.values()), per_partition


def count_offset_regressions(events):
    """E5: per (group, topic, partition), how often an accepted commit was lower than the
    one before it, from the coordinator's commit_accepted events in file order (LLD §13.4).
    `events` is a DataFrame of events-coordinator.csv. Returns (total, per partition)."""
    accepted = events[events["event"] == "commit_accepted"]
    per_partition = {}
    for key, rows in accepted.groupby(["group", "topic", "partition"], sort=False):
        offsets = list(rows["offset"])
        per_partition[key] = sum(1 for a, b in zip(offsets, offsets[1:]) if b < a)
    return sum(per_partition.values()), per_partition
