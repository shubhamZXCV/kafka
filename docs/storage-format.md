# Storage Format Reference

What a broker keeps in `data.dir`, as built (`src/storage`). The algorithms behind it
(append, read, truncation, recovery, retention) are in LLD §5; this page describes the
files, so you can inspect or repair a data directory. `elogctl log-verify <data.dir>` checks
all of it read-only and prints a JSON report.

## Layout

```
<data.dir>/
├── meta.properties               the broker ID this directory belongs to
├── recovery-point.checkpoint     per partition: everything below is on disk
├── high-watermark.checkpoint     per partition: last known high watermark
└── <topic>-<partition>/          one directory per hosted replica, e.g. orders-3/
    ├── 00000000000000000000.log    segments, named by their base offset (20 digits)
    ├── 00000000000000000000.index  sparse index of the segment with the same name
    ├── 00000000000000041200.log
    ├── 00000000000000041200.index
    └── leader-epoch.checkpoint     where each leader epoch starts in this log
```

In a partition directory, a file ending in `.tmp` (an unfinished write) and an `.index`
without its `.log` are deleted when the broker opens the log.

## Segment (`.log`)

Record batches back to back, exactly as on the wire ([protocol.md](protocol.md#record-batch)),
with offsets and leader epochs stamped. No file header. A broker rolls to a new segment when
the active one would exceed `segment.bytes` (64 MiB) or is older than `segment.ms` (7 days).

Batches are only ever cut at batch boundaries: truncation (a follower agreeing with a new
leader, LLD §5.5) and recovery (dropping a torn tail after a crash, LLD §5.6) both keep whole
batches.

## Sparse index (`.index`)

8-byte entries, big-endian: `relativeOffset (u32) | position (u32)`, where
`relativeOffset = batch baseOffset − segment baseOffset` and `position` is the batch's byte
offset in the `.log`. One entry for the segment's first batch, then one whenever at least
`index.interval.bytes` (4 096) were written since the last. Both fields strictly increase.

The index is an accelerator only. If it is corrupt, does not fit its log, or is empty for a
non-empty segment, the broker rebuilds it from the log at startup.

## Checkpoints

All checkpoint files are written atomically: write `<file>.tmp`, fsync it, rename it over
the old file, fsync the directory.

`recovery-point.checkpoint` and `high-watermark.checkpoint` (every 5 s): a version line,
an entry count, then `topic partition offset` per partition.

```
0
2
orders 0 41200
orders 1 39877
```

A missing or unreadable `recovery-point.checkpoint` is not an error: recovery then scans
whole logs instead of only their unflushed tails.

`leader-epoch.checkpoint`: a version line, an entry count, then `epoch startOffset` per
leader epoch, oldest first. Followers use it to find where their log diverges from a new
leader's (LLD §6.8, R-23).

```
0
2
6 0
7 41180
```

## `meta.properties`

```
version=1
broker.id=1
```

Written on first start. A broker started with a different `broker.id` on this directory
refuses to run, which guards against mixed-up data directories.

## Durability

`log.flush.policy` decides when segments reach the disk (LLD §5.8): `interval` (default)
fsyncs dirty segments every `log.flush.interval.ms` (1 s) and relies on replication against
a single crash; `batch` fsyncs after every append; `none` leaves it to the OS (tests only).
After every successful flush the recovery point moves to the log end at that moment.
