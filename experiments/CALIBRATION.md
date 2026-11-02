# Experiment Calibration

The experiments (LLD §16.2) set their loads as a share of the consumers' capacity. This file
records how that capacity was measured, the value the scripts use, and the other settings
that were tuned, each with the data behind it. Rerun `experiments/calibrate.sh` on a new
host and update `CONSUMER_CAPACITY` in `experiments/lib/common.sh` if the result differs.

## Reference host

| | |
|---|---|
| Machine | Linux 7.0 x86-64, 12 CPUs, 15.4 GiB, local SSD |
| Docker | Engine 29.1.3, Compose v5.6, images from `deploy/docker` (release build) |
| Topology | `deploy/compose.yaml`: coordinator 1 CPU, brokers 1 CPU each, consumers 0.5 CPU each |
| Measured at | commit `ebf4b13`, 2026-11-02 |

## Consumer capacity

**Model.** Every consumer sleeps `PROCESS_US` = 2 000 µs per record in its callback
(`--process-us`, R-22), the "callback sleep" of E7. A known per-record cost makes the
capacity a property of the experiment, not of how fast the host happens to run the client.

**Measurement** (`experiments/calibrate.sh`). A fresh 12-partition, R = 3 topic is
preloaded with 60 000 records; 4 consumers drain it for 40 s while a producer adds R
records/s; capacity is their consume rate from 10 s after they start.

| Producer during the measurement | Group rate | Per consumer | CV of 1 s rates |
|---|---|---|---|
| none | 1 538 /s | 385 /s | 0.05 |
| 760 /s | 1 252 /s | 313 /s | 0.07 |
| 1 140 /s | 1 264 /s | 316 /s | 0.18 |

**Value used: `CONSUMER_CAPACITY` = 315 records/s per consumer.** Every experiment runs
its consumers while a producer writes, so the capacity under load is the right one.

**Why load lowers it.** 2 000 µs per record alone would give about 470 /s; the sleep
overshoots by ~0.1 ms, and the consumer commits synchronously every 100 records
(LLD §10.6), which blocks its processing thread for one OffsetCommit round trip. The
coordinator makes each commit durable with an fsync (`synchronous = FULL`, LLD §7.4). On a
single host the brokers' segment flushes share that disk, so the fsync waits behind them:
the drop from 385 to ~315 /s is ~60 ms more per commit. Measured directly on the host
(unthrottled producer, one consumer): commit round-trip median 43 ms with the brokers'
`log.flush.policy=none`, 265 ms with the default `interval`. A consumer that does no work
per record is entirely bound by this, which is why the experiments do not use one.

## Derived settings

| Setting | Value | Why |
|---|---|---|
| Loads | `rate_at PERCENT CONSUMERS` = PERCENT % × CONSUMERS × 315 | One definition for every script |
| E1, E2, E6 | 70 %, 50 %, 50 % of 4 consumers | LLD §16.2 |
| E4 | 70 % instead of 80 % | R-25: 3 survivors have 75 % of the capacity, below 95 % of an 80 % load |
| E9 | 65 % | R-25: a backlog large enough to measure its drain (15–20 s) |
| E7 | 300 % | LLD §16.2 |
| E8 | 30 % of one consumer | LLD §16.2 |
| E5 zombie | commits every 50 records or 200 ms | After every record (as in F-FEN), the rejoined zombie fell behind a live producer and the group never drained |
| Session timeout in kill scenarios | heartbeat = session / 3 | Keeps three heartbeats per session at every E3 sweep point |
| Warm-up | 30 s (20 s in E3/E7 sweeps) | Consumers join and the latency transient settles in ~15 s (E1 figures) |

## Checks against the calibration

* **E9** (producer at 65 % = 819 /s, one of 4 consumers killed): while the backlog drained,
  the 3 survivors processed 949 /s, i.e. **316 per consumer**, matching the calibrated
  315; their spare capacity, 949 − 819 = 130 /s, matches the predicted drain of 126 /s.
* **E1**, run before this recalibration at 1 064 /s (70 % of the no-load 380 /s): the
  group consumed 1 074 /s, CV 0.05. Below the true capacity under load (4 × 315 = 1 260 /s),
  as 70 % should be.

## Caveats

* The capacity is a property of this host's disk: on a faster one, or with the coordinator
  on its own disk, commits are cheaper and capacity under load rises towards 385 /s.
* `sleep_for` overshoots more when the host is busy; capacities measured with other work
  running on the machine come out lower.
