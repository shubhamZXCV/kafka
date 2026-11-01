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
