# Final Report — Distributed Event Log with Dynamic Consumer Rebalancing

| | |
|---|---|
| Project | #49, Kafka-inspired distributed event log (C++20) |
| Baseline | [requirements.md](requirements.md) (SRS), [high-level-design.md](high-level-design.md), [low-level-design.md](low-level-design.md) (v1.19) |
| Results | [results/summary.md](../results/summary.md), raw data as described there |
| Date | 2026-11-02 |

## 1. Summary

The system is built as specified: brokers with segmented, crash-recoverable logs and
leader/follower replication over an in-sync replica set; a coordinator that places
partitions, elects leaders, runs consumer groups with round-robin or sticky assignment, and
stores offsets in SQLite behind a single fenced compare-and-set; and clients whose bounded
in-flight queue becomes fetch credit. 31 functional tests start the real binaries and pass
under AddressSanitizer/UBSan and under ThreadSanitizer.

The two headline claims hold.

- **Fencing** (E5): a consumer frozen past its session timeout usually wakes up and commits
  with its old ownership epoch (5 of 6 runs). With fencing, every such commit is refused
  and no committed offset moves backwards. With fencing off, the same commits move offsets
  backwards in every run that sends one.
- **At-least-once** (E8): a consumer crashed between processing and committing causes
  exactly as many duplicates as it had processed since its last commit: never more than the
  commit interval, about half of it on average, and linear in the interval.

Three of the nine predictions did not hold as stated; §4 explains why.

- **E2:** a rebalance after a consumer crash takes the session timeout plus up to one
  heartbeat interval, not plus 500 ms.
- **E4:** sticky assignment moves a quarter of the partitions but recovers no faster,
  because these consumers keep no per-partition state.
- **E9:** the backlog drain is the difference of two large rates, so a 5 % error in the
  calibrated capacity becomes a 40 % error in the drain.

Building and measuring the system also found two defects in the design itself. Both are
recorded and fixed as LLD refinements (R-23, R-26).

## 2. What was built

| Component | Lines (C++) | Highlights |
|---|---|---|
| common | 1 788 | `Result`/`Status`, sockets, config, logging, metrics and events as CSV |
| protocol | 2 749 | framed TCP with CRC32C, record batches shared by wire and disk, JSON and MessagePack codecs |
| storage | 1 454 | segments with sparse indexes, torn-tail recovery, retention, leader-epoch cache, atomic checkpoints |
| broker | 2 405 | replicas, produce with admission control, credit-bounded fetch, follower fetchers, ISR shrink/expand, epoch-based truncation |
| coordinator | 2 985 | single-threaded event loop, groups and assignors, fenced offset commits, metadata, broker liveness, leader election |
| client | 1 826 | batching producer with retries; consumer with credit-based fetching, group membership, commits, crash hook |
| tools | 864 | `elog-producer`, `elog-consumer`, `elogctl` |

Around it: 31 functional tests (1 042 lines of Python), the analysis package shared by
tests and experiments (1 384 lines), and nine one-command experiments with Docker images, a
compose topology and fault injection (678 lines of shell). The work is 167 commits, each
building and passing the tests that existed at the time.

### Changes to the design

The LLD records 26 refinements (§17). Most fill in details the design left open. Four
change behaviour and are worth knowing:

- **R-23** corrects an LLD bug: the follower truncation loop could fail to terminate when
  the new leader never had the follower's latest epoch. Truncation is now one round, to the
  smaller of the two logs' end offsets for the last common epoch (as in Kafka's KIP-279).
- **R-24** defines when the crash hook fires: only after the run's first fully accepted
  commit, so restarts make progress.
- **R-25** lowers E4's load from 80 % to 70 %: after one of four consumers dies, the
  survivors have 75 % of the capacity, so at 80 % the group could never return to 95 % of
  its earlier rate and E4's measure was undefined.
- **R-26** corrects another LLD race. A sticky rebalance applied the new assignment before
  setting the added partitions' start offsets, and the fetcher, still running, could reset
  them to offset 0 in between. The consumer then re-read whole partitions. Commits stayed
  correct, so this cost duplicates, not regressions. The offsets are now fetched first and
  applied with the assignment.

Beyond the LLD, the experiments exposed four more defects, each fixed with its own commit:

- commits slower than the time-based interval made the consumer commit after every record;
- restarted tools truncated their own CSVs;
- the crash hook did not kill a process running as a container's PID 1;
- the init process later added to fix that one absorbed `SIGSTOP`, so the first E5
  campaign run froze nothing (results/summary.md, *Deviations*).

## 3. Verification

| Check | Result |
|---|---|
| Functional suite, dev build (ASan + UBSan) | 26 of 26 default tests pass; 5 slow/repeated scenarios also pass |
| Functional suite, ThreadSanitizer build | 26 of 26 pass; no report in any of 1 035 process logs (rerun after the last client change) |
| Build warnings | none in the dev build; the release build shows GCC 13's known `-Wuninitialized` false positive for a moved `std::variant` |
| Formatting | `clang-format` on every commit |

The tests check behaviour from outside, through `elogctl`, exit codes and the CSVs every
process writes, using the same correctness checks as the experiments: order (INV-01), no
skipped records (INV-05), no lost acknowledged records, duplicate counts and offset
regressions. F-FEN-2 runs the fencing scenario with fencing off and must see a regression,
which shows that F-FEN-1 can fail.

No ThreadSanitizer reports were found, so commit-plan row C114 ("one commit per race
found") produced no commits.

## 4. Experiments against the predictions

All experiments ran in Docker on one host (12 CPUs, 15 GiB, local SSD), each process in a
capped container, from the pinned images. Consumers spend 2 ms per record in their callback;
their capacity with a producer running, **315 records/s each**, was measured first, and
every load is a share of it ([CALIBRATION.md](../experiments/CALIBRATION.md)). The campaign
ran 3 repeats of each experiment (cut line 3 of the implementation plan), 2 of the E3 and E7
sweeps, and 50 crashes per commit interval for E8. E5 was then run twice more, once after
each of the two defects its earlier runs exposed (the init absorbing `SIGSTOP`, then R-26).
Only the last E5 sweep is reported.

| | Prediction (SRS §9.1) | Measured (mean ± SD over repeats) | Verdict |
|---|---|---|---|
| E1 | Stable throughput; order never violated | 882 records/s, CV 0.06; 0 order violations, losses or skips; end-to-end p99 141 ms | holds |
| E2 | Rebalance ≈ session + < 500 ms; no skips | 6.81 ± 0.73 s with a 6 s session (within the bound in 1 of 3); 0 skips | partly |
| E3 | Near-linear in the session timeout; false evictions below ~2 s | R² 0.984, slope 1.10, intercept 0.41 s; no false eviction from 2 to 15 s | holds |
| E4 | Sticky reaches full throughput 40–70 % sooner | round-robin 629 ± 299 ms, sticky 833 ± 503 ms | does not hold |
| E5 ★ | 0 offset regressions with fencing; > 0 without | on: 0 regressions, 6 of 6 stale commits rejected; off: 4 regressions | holds |
| E6 | New leader from the ISR; 0 committed records lost | 3 of 3 from the ISR; 0 lost; election 5.5 s, write gap 14.2 s | holds |
| E7 | Depth and RSS flat at the bound; backlog on disk | depth ≤ Q for Q up to 10 000; RSS ≤ 9.4 MB, growth ≤ 2 MB | holds |
| E8 ★ | Duplicates ≤ interval, mean ≈ half, linear | 200 crashes: max < N, means within 10 % of (N − 1)/2, slope 0.48; duplicates = uncommitted records exactly | holds |
| E9 | Drain ≈ capacity − producer rate | 187 ± 41 /s against 126 /s predicted; 3 × measured capacity − rate predicts each run within 15 % | does not hold as calibrated |

[results/summary.md](../results/summary.md) has the analysis behind every row, the figures and
the deviations. In short:

- **E2: the rebalance time has two parts.** Detection, the session timeout minus the time
  since the dead member's last heartbeat, took 5.3 s. Then a barrier of 1.5 s: survivors
  learn of the rebalance only at their next heartbeat (every 2 s), and the rebalance ends
  when the last of them has rejoined. The SRS bound has no room for the barrier. E3 shows
  the same structure at every timeout (slope 1.1, offset 0.4 s).
- **E4: these consumers keep no state, so eager assignment costs almost nothing.** An eager
  survivor commits, gives up its partitions and resumes them with one fetch. Both arms
  recover within about 1 s of the rebalance, at the resolution of the metrics, and neither
  processes anything twice. Sticky's benefit (3 partitions moved instead of 12) would show
  with per-partition state to rebuild, which the prediction implicitly assumed.
- **E6: the write gap is longer than the election.** The dead broker stays in the ISR of
  the partitions it followed until `replica.lag.time.max.ms` (10 s) expires. A produce to
  those partitions waits for it, and the producer's single sender thread makes every
  partition wait behind them.
- **E9: the drain is a small difference of large rates.** While draining, the survivors
  processed 331 records/s each, 5 % above the calibrated 315/s. That 5 % becomes 40 % of the
  predicted drain. The relation itself holds when the measured capacity is used.

## 5. Requirements coverage

All 106 Must requirements are met, with these partial exceptions:

| Requirement | Status |
|---|---|
| FR-PROD-02 | `NOT_LEADER` does not name the current leader; the producer refreshes metadata instead, which costs one round trip |
| FR-OPS-04 | Key cardinality, rate, payload and seed are configurable; the key distribution is uniform only |
| FR-FAULT-03, NFR-PORT-03 | `tc netem` works (containers get `NET_ADMIN`); the Toxiproxy fallback only adds delay, and only to traffic an experiment routes through it. No experiment in §16.2 uses network faults. |
| NFR-PORT-01 | Built and tested with GCC 13 only, not Clang 17 |
| FR-OFF-01 | Commits carry the partition's ownership epoch rather than the group generation: the design's fencing token (HLD ADR-04), which lets sticky assignment keep a partition's epoch across generations |

Should requirements not met:

| Requirement | Why |
|---|---|
| NFR-TEST-04 (CI) | Cut line 4 of the implementation plan; the suite runs locally under all three builds |
| NFR-SCAL-02 (rebalance vs group size) | No experiment in the plan measures it; E3 varies the session timeout instead |
| NFR-SEC-01 (bind to loopback by default) | Services bind `0.0.0.0` by default so the compose topology works unchanged; `listen.host` restricts it |
| NFR-REPRO-04 (repeats) | Met with 3 repeats instead of the planned 5 (cut line 3) |

Could requirements: unkeyed records are spread round-robin (FR-PART-05); weaker acks
(FR-PROD-04) and multi-node runs (NFR-SCAL-03) were not built.

## 6. Limitations

- **The coordinator is a single point of failure** (L-01). Offsets and group state are in
  SQLite, so a restarted coordinator recovers them (F-GRP-5), but while it is down no group
  can rebalance or commit and no leader can be elected.
- **Security is out of scope** (NFR-SEC-03): no authentication, authorisation or TLS. The
  only protection is input validation: every frame is length-bounded and checksummed before
  parsing, and a malformed one closes the connection without harming the process.
- **One host.** Every number here comes from one machine, where all processes share one
  disk. That couples the coordinator's commit latency to the brokers' writes
  ([CALIBRATION.md](../experiments/CALIBRATION.md)), which a deployment with separate
  disks would not have.
- **Rebalances stop the world.** Every member waits at the barrier until the last one has
  rejoined, so even sticky assignment pauses the whole group for the detection time plus up
  to a heartbeat interval (E2, E4). Incremental cooperative rebalancing, as in Kafka's
  KIP-429, would remove the pause for unaffected partitions.
- **A broker failure stalls writes for longer than the election.** The dead broker stays in
  the ISR of the partitions it followed for `replica.lag.time.max.ms`. The producer's single
  sender thread then makes every partition wait (E6: 14 s against a 5.5 s election).
- **Fencing protects offsets, not side effects.** The new owner processes again whatever
  the zombie processed after its last accepted commit: before the freeze, and after waking
  until it learns of its eviction (E5: 21–35 duplicates per fenced run). At-least-once
  delivery allows this, but the processing callback has to tolerate it.

## 7. Reproducing

```bash
cmake --preset dev && cmake --build --preset dev && pytest tests/functional -m "not slow"
experiments/calibrate.sh          # consumer capacity on your host
experiments/campaign.sh           # E1–E9 and results/, about two hours
experiments/demo.sh               # the live demonstration of E5 and E8
```

Every figure is regenerated from raw CSV by `analysis/plot_eN.py <run directory>`. The raw
data of the reported campaign is archived as described in
[results/summary.md](../results/summary.md).
