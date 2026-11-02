# elog — a distributed event log with dynamic consumer rebalancing

A Kafka-inspired event log in C++20, built as a course project: a partitioned, replicated,
append-only log; a coordinator that runs consumer groups and commits their offsets; and
rebalancing with **epoch fencing**, so a consumer that was declared dead and wakes up can
never move its group's progress backwards.

| | |
|---|---|
| Brokers | partitions as segmented logs with sparse indexes and crash recovery; leader/follower replication with an in-sync replica set, a high watermark and leader epochs |
| Coordinator | topics and partition placement, broker liveness and leader election, consumer groups (round-robin or sticky assignment), committed offsets in SQLite, fenced by ownership epoch |
| Clients | a producer, and a consumer whose bounded in-flight queue turns into fetch credit (backpressure) |
| Tools | `elog-producer`, `elog-consumer`, `elogctl` |

## Build

Needs Linux x86-64, GCC 13 (or Clang 17), CMake ≥ 3.25 and Ninja. Third-party libraries
(spdlog, nlohmann/json, msgpack-c, crc32c, SQLite) are downloaded and pinned by CMake.

```bash
cmake --preset dev && cmake --build --preset dev            # Debug + ASan + UBSan
cmake --preset release && cmake --build --preset release    # optimised, for experiments
```

Binaries land in `build/<preset>/bin/`: `elog-coordinator`, `elog-broker`,
`elog-producer`, `elog-consumer`, `elogctl`.

## Run a cluster by hand

```bash
B=build/dev/bin
$B/elog-coordinator --set db.path=/tmp/elog/c.db --set metrics.dir=/tmp/elog &
for i in 1 2 3; do
  $B/elog-broker --set broker.id=$i --set listen.port=910$i --set advertised.host=127.0.0.1 \
      --set coordinator.address=127.0.0.1:9000 --set data.dir=/tmp/elog/b$i \
      --set metrics.dir=/tmp/elog &
done
$B/elogctl --coordinator 127.0.0.1:9000 health --brokers 3 --timeout-ms 10000   # waits for all three
$B/elogctl --coordinator 127.0.0.1:9000 create-topic orders -p 6 -r 3 --min-isr 2

$B/elog-producer --coordinator 127.0.0.1:9000 --topic orders --count 10000 --out /tmp/elog
$B/elog-consumer --coordinator 127.0.0.1:9000 --topic orders --group g --id c1 \
    --count 10000 --out /tmp/elog
$B/elogctl --coordinator 127.0.0.1:9000 describe-group g
```

Every setting has a default and can be given with `--set key=value` or `--config FILE`;
`--help` lists them. Each process writes CSV metrics and events to its `metrics.dir`; the
tools write what they produced and processed to `--out`.

## Test

The tests start the real binaries and check behaviour from the outside (no unit tests):

```bash
python3 -m pip install -r requirements.txt
pytest tests/functional -m "not slow"        # ~6 min against build/dev
pytest tests/functional                      # also the slow, repeated scenarios
ELOG_BUILD_DIR=build/tsan setarch -R pytest tests/functional -m "not slow"   # ThreadSanitizer
```

They cover storage recovery, produce and fetch, consumer groups, **fencing** (F-FEN-1, and
F-FEN-2 showing that without fencing the same scenario does regress), replication and
leader failover, backpressure, at-least-once with a deterministic crash, and sticky
assignment. Correctness is judged by the shared checks in `analysis/elog_analysis/checks.py`.

## Experiments

E1–E9 (SRS §9.1) run every process in its own capped Docker container:

```bash
make e5                      # builds the images, then runs E5 (fencing) 5 times
make e8 ARGS="--repeat 1"    # options go to the script
```

Each run writes CSVs, logs, `run.json`, figures and a `summary.json` of pass/fail checks to
`runs/<UTC time>-eN/`. Loads are shares of a measured consumer capacity; see
[experiments/CALIBRATION.md](experiments/CALIBRATION.md). The campaign's results are in
[results/summary.md](results/summary.md).

For a ten-minute live demonstration of fencing and crash duplicates:
`experiments/demo.sh`.

## Layout

```
src/common       types, Result, sockets, config, logging, metrics
src/protocol     frames, record batches, message codecs
src/storage      segments, sparse index, partition log, checkpoints, recovery
src/broker       replicas, produce/fetch/replication handlers, ISR, fetcher threads
src/coordinator  event loop, groups, assignors, offsets (SQLite), metadata, election
src/client       producer and consumer library
src/tools        elog-producer, elog-consumer, elogctl
tests/functional pytest scenarios against the real binaries
analysis         correctness checks, timing measurements, plots
experiments      E1–E9 scripts, fault injection, calibration
deploy           Dockerfiles and the compose topology
```

## Documentation

| Document | What it is |
|---|---|
| [docs/requirements.md](docs/requirements.md) | Requirements, invariants and the experiment plan |
| [docs/high-level-design.md](docs/high-level-design.md) | Architecture and decision records |
| [docs/low-level-design.md](docs/low-level-design.md) | The specification the code follows, with refinements in §17 |
| [docs/protocol.md](docs/protocol.md) | Wire protocol reference |
| [docs/storage-format.md](docs/storage-format.md) | On-disk format reference |
| [docs/final-report.md](docs/final-report.md) | Results against every prediction |
