# Commit Plan

## Distributed Event Log with Dynamic Consumer Rebalancing

| Field | Value |
|---|---|
| Project | #49 — Distributed Event Log with Dynamic Consumer Rebalancing |
| Document type | Commit Plan (commit-level breakdown of the implementation) |
| Version | 2.0 |
| Status | Active |
| Date | 2026-10-28 |
| Inputs | [implementation-plan.md](implementation-plan.md) v2.0 (tasks T0.1–T8.5), [low-level-design.md](low-level-design.md) v1.1 |

### Revision History

| Version | Date | Changes |
|---|---|---|
| 1.0 | 2026-10-28 | Initial plan (unit-test based) |
| 2.0 | 2026-10-28 | Rewritten for **functional tests only**: no unit or property tests; each commit says how to *see it work*; walking skeleton first; fewer, focused commits |

---

## 1. Purpose

The implementation plan breaks the project into tasks. This document breaks each task into **commits**, each small enough to review in one sitting and focused enough to understand on its own. Read in order, the git history works as a guided tour of the codebase.

## 2. Commit Rules

| # | Rule | Why |
|---|---|---|
| P1 | **One idea per commit.** If the message needs "and", consider splitting. | You can hold the whole change in your head |
| P2 | **Every commit builds**, and all functional tests that exist so far still pass | Any commit can be checked out and run |
| P3 | **A feature lands with the test that shows it working** (or in the next commit of the same task) | "Done" means seen working, not just compiled |
| P4 | **No mixing.** Renames, moves and formatting go in their own commits | A behaviour change never hides in a reformat |
| P5 | **Size:** aim for ≤ 300 changed lines; hard cap 500; vendored files on their own | Review stays under about 20 minutes |
| P6 | **Spec-anchored.** The message names the LLD section it implements | You can read the code next to the pseudocode |
| P7 | **⚠ commits stay small** (≤ 200 lines) | Line-by-line reading is realistic |

## 3. Commit Message Format

```
<type>(<scope>): <imperative summary, ≤ 72 chars> (<task id>)

What:   <1–3 lines: what this commit adds or changes>
Why:    <1–2 lines: which requirement or design point it serves>
Read:   <where to start reading; the one thing most worth checking>
Spec:   LLD §x.y
Check:  <how to see it work: a functional test, a command, or "builds">
```

**Types:** `build`, `ci`, `docs`, `feat`, `test`, `fix`, `refactor`, `chore`.
**Scopes:** `common`, `protocol`, `storage`, `broker`, `coordinator`, `client`, `tools`, `deploy`, `experiments`, `analysis`, `repo`.

**Example:**

```
feat(coordinator): add the fenced offset-commit statement (T4.1)

What:   Repository::commitOffsetFenced() runs the single INSERT…SELECT…
        ON CONFLICT statement and returns whether a row was written.
Why:    FR-OFF-04 — fencing must be one atomic compare-and-set.
Read:   The WHERE clause: owner_member AND owner_epoch must both match.
Spec:   LLD §8.3, HLD §8.2
Check:  builds; exercised by F-FEN-1 two commits later
```

## 4. How to Review a Commit

1. **Read the message.** It says what changed and where to start.
2. **Read the header (`.h`)**: names, responsibilities, thread rules.
3. **Read the implementation** next to the LLD section in `Spec:`.
4. **See it work.** Run the command in `Check:`.

```bash
git log --oneline --reverse                        # the table of contents of the codebase
git show --stat <sha>                              # files touched
git log --reverse --format='%h %s' -- src/storage  # follow one module's story
cmake --build --preset dev && pytest tests/functional -m "not slow"
```

## 5. Instructions for the Coding Agent

Paste this with the task's rows:

```markdown
Produce exactly the commits listed for this task in docs/commit-plan.md, in order.
For each commit:
  1. Implement only what that row describes; do not anticipate later rows.
  2. Follow CLAUDE.md (code-quality guidelines, lock order, LLD names).
  3. Build, run the functional tests that exist so far, and run the row's Check.
  4. Use the message format in commit-plan.md §3 with the row's summary line.
Do not write unit tests. If a row is too big (> 500 lines) or impossible without
a later row, stop and propose a split or reorder instead of improvising.
```

## 6. Reading the Tables

| Column | Meaning |
|---|---|
| **#** | Commit number (C001…), for discussion |
| **Task** | Implementation-plan task |
| **Commit summary** | First line of the commit message (without the task suffix) |
| **What it adds** | Files and concepts introduced |
| **Read carefully / learn** | The one thing to check, and the concept the commit teaches |
| **See it work** | How to observe the change: a functional test (LLD §15.3), a command, or "Builds" when it is used by a later commit |
| **Size** | **S** ≤ 150 lines, **M** ≤ 300, **L** ≤ 500 |

⚠ = correctness-critical: read every line against the LLD. 🎯 = milestone demo.

---

## 7. Commit Sequence


### Chapter 0 — Setup (M0)

*An empty repository becomes a buildable C++20 project with the small building blocks every module uses. Nothing runs yet. These commits are short and easy to read.*

| # | Task | Commit summary | What it adds | Read carefully / learn | See it work | Size |
|---|---|---|---|---|---|---|
| C001 | — | `docs(repo): add requirements, design and planning documents` | `docs/*.md`, `docs/writeup.pdf` | The spec baseline; nothing to review | — | — |
| C002 | T0.1 | `build(repo): add root CMake project and module layout` | Root `CMakeLists.txt`, `src/CMakeLists.txt` documenting the module graph, `elog_common` with its first file (`version.h/.cc`), `.gitignore`. Other modules get their `CMakeLists.txt` with their first source (CMake rejects empty library targets). | Module graph comment matches LLD §2.2 (no cycles) | `cmake -S . -B build && cmake --build build` | S |
| C003 | T0.1 | `build(repo): add dev, release and tsan presets` | `CMakePresets.json`, warning and sanitizer flags | Flags per LLD §2.4 | All three presets configure | S |
| C004 | T0.1 | `build(repo): pin third-party dependencies` | `cmake/Dependencies.cmake` (FetchContent, exact versions) | Every dependency pinned (LLD §2.3); `MSGPACK_NO_BOOST` set | Clean build downloads and compiles them | S |
| C005 | T0.1 | `chore(repo): add clang-format configuration` | `.clang-format` | — | `clang-format --dry-run` works | S |
| C006 | T0.2 | ⚠ `docs(repo): add CLAUDE.md agent guide` | `CLAUDE.md`: commands, code-quality guidelines, lock order, spec and commit rules | Matches implementation-plan §3–4; this is what every agent reads | — | S |
| C007 | T0.3 | `feat(common): add core types and Result` | `types.h` (Offset, Epoch, TopicPartition…), `error_code.h` (LLD §4.4 values, R-9), `result.h` (`Result<T>`, `Status`) | **Learn:** the error-handling style used everywhere | Builds | S |
| C008 | T0.3 | `feat(common): add time helpers and config loader` | `time.h`, `config.h/.cc` (defaults < file < `--set`, unknown key fails, ranges) | Timeouts use the monotonic clock only | Builds | M |
| C009 | T0.3 | `feat(common): add structured logging setup` | `logging.h/.cc` (spdlog, key=value, node and component fields) | — | Builds | S |
| C010 | T0.3 | `feat(common): add Socket and Listener` | `socket.h/.cc`: connect, `readExact`, `writeAll`, timeouts, accept | `readExact` loops on short reads and `EINTR` | Builds | M |
| C011 | T0.3 | `feat(common): add BoundedQueue` | `bounded_queue.h` (blocking push/pop with timeout, `freeSlots`, `removeIf`, `close`) | **Learn:** the bounded buffer behind all backpressure. Check the condition-variable predicates. | Builds | S |
| C012 | T0.3 | `feat(common): add metrics and events CSV writers` | Counter/gauge/histogram registry, periodic CSV flush, events writer | CSV columns exactly as LLD §13.1 (tests and analysis read them) | Builds | M |
| C013 | T0.4 | `feat(common): add byte-order helpers and CRC32C` | `bytes.h`, `crc32c.h` | — | Builds | S |
| C014 | T0.4 | `feat(protocol): add API keys and error codes` | `api_keys.h`, `error_codes.h` (`errorName`, `isRetriable`; the enum is in common, R-9) | Numbers match LLD §4.3–4.4 exactly | Builds | S |
| C015 | T0.4 | `feat(protocol): add frame encoder and decoder` | `frame.h/.cc` with magic, version, CRC and size checks | **Learn:** the wire envelope. Oversize is rejected *before* allocating. | Builds; first exercised by F-PF-4 | M |

### Chapter 1 — Walking Skeleton (M1)

*The thinnest system that works end to end: one broker, one coordinator, a producer and a consumer. By the last commit, a million records flow through and a functional test proves it.*

| # | Task | Commit summary | What it adds | Read carefully / learn | See it work | Size |
|---|---|---|---|---|---|---|
| C016 | T1.1 | `feat(protocol): define the record batch header layout` | `record_batch.h`: field offsets as constants, header encode/decode | Offsets match the LLD §4.5 table | Builds | S |
| C017 | T1.1 | ⚠ `feat(protocol): add RecordBatchBuilder` | Builds a batch of MessagePack records and its CRC | CRC covers bytes `[21, end)` only | Builds | M |
| C018 | T1.1 | ⚠ `feat(protocol): add RecordBatchView with CRC validation` | `parse()`, accessors, `forEachRecord` | Every length is bounds-checked before use (input is untrusted) | Builds | M |
| C019 | T1.1 | ⚠ `feat(protocol): add stampBatch and sliceBatch` | Leader stamps offset and epoch; fetch slices batches to honour credit | **Learn:** why the CRC skips `baseOffset`/`leaderEpoch`. Check slice offsets and recomputed CRC. | Exercised by F-PF-1 | M |
| C020 | T1.2 | `feat(storage): add Segment with append and read` | `segment.h/.cc`: one `.log` file, `pwrite` at the tail, `readAt` | **Learn:** a segment is just concatenated batches | Builds | S |
| C021 | T1.2 | ⚠ `feat(storage): add minimal PartitionLog` | Single-segment log: leader append (assigns offsets), read up to a bound | `leo_` published with `release` *after* the write (LLD §11.4) | Exercised by F-PF-1 | M |
| C022 | T1.3 | `feat(protocol): add control messages with JSON codec` | Metadata, CreateTopic, RegisterBroker, BrokerHeartbeat, LeaderAndIsr | Field names match LLD §4.6; bad input → `INVALID_REQUEST` | Builds | M |
| C023 | T1.3 | `feat(protocol): add Produce and Fetch with MessagePack codec` | Structs and codec; records carried as opaque bytes | — | Builds | M |
| C024 | T1.4 | `feat(broker): add server, connection handler and dispatcher` | `elog-broker` binary, accept loop, thread per connection, request dispatch | **Learn:** the request/response loop. Malformed frames close the connection. | `elog-broker --help` runs | M |
| C025 | T1.4 | `feat(broker): add ReplicaManager and leader-only Replica` | Partition map; `Replica` with log and HW (with R=1, HW follows LEO) | Lock use matches LLD §11.2 | Builds | M |
| C026 | T1.4 | `feat(broker): add produce with bounded admission` | `ProduceHandler`: leader/epoch checks, admission semaphore, append, commit wait | **Learn:** how a full queue becomes TCP backpressure. The admission slot is released on every path. | Exercised by F-PF-1 | M |
| C027 | T1.4 | ⚠ `feat(broker): add credit-exact fetch with long-poll` | `HwNotifier`, `FetchHandler` (read to HW, slice to credit, wait for data) | Σ records ≤ credit; never reads above HW; re-checks after each wake-up | F-PF-2 | M |
| C028 | T1.4 | `feat(broker): add graceful shutdown` | Connection registry; close sockets, join threads on SIGTERM | No thread outlives the server (ASan clean) | Broker exits cleanly on Ctrl-C | S |
| C029 | T1.5 | `feat(coordinator): add SQLite wrapper and schema` | RAII `Db`/`Stmt`/`Txn`, pragmas, DDL from LLD §8.2 | Transactions roll back on error | Builds | M |
| C030 | T1.5 | ⚠ `feat(coordinator): add the single-threaded event loop` | `elog-coordinator` binary; connection threads enqueue requests, one loop thread handles them; ticker | **Learn:** serialise instead of lock (ADR-02). Connection threads never touch state. | `elog-coordinator --help` runs | M |
| C031 | T1.5 | `feat(coordinator): add topic creation and metadata` | `MetadataManager`: CreateTopic (placement, idempotent), Metadata | Persisted before reply | F-PF-3 | M |
| C032 | T1.5 | `feat(coordinator): add broker registration and LeaderAndIsr push` | `BrokerLiveness` (register, heartbeat), `BrokerPusher` thread | Pusher never touches coordinator state | Builds | M |
| C033 | T1.5 | `feat(broker): register with the coordinator and apply LeaderAndIsr` | `CoordinatorAgent` (register, heartbeat), leader-role handling | Older-epoch `LeaderAndIsr` ignored | Broker shows as registered in logs | M |
| C034 | T1.6 | `chore(common): vendor MurmurHash3` | `murmur3.h/.cc` (public domain, unchanged) | Vendored code untouched | Builds | S |
| C035 | T1.6 | `feat(client): add RPC helper, metadata cache and partitioner` | Per-broker connection with correlation IDs; `MetadataCache`; `partitionFor` | Correlation ID checked on every response | Builds | M |
| C036 | T1.6 | `feat(client): add Producer` | Bounded accumulator, sender thread, retries, one in-flight batch per partition | **Learn:** how order survives retries (INV-01) | Exercised by F-PF-1 | M |
| C037 | T1.6 | `feat(client): add manual-assignment consumer` | Fetcher thread, bounded in-flight queue, processing loop | Credit = free slots, computed just before each fetch | Exercised by F-PF-1 | M |
| C038 | T1.7 | `feat(tools): add elog-producer` | Keyed synthetic workload (rate, keys, payload, seed); writes `produced-*.csv` | Record value format per LLD §4.5 | Run by hand against a broker | M |
| C039 | T1.7 | `feat(tools): add elog-consumer` | Manual mode; writes `processed-*.csv` offset ranges | Ranges flushed regularly | Run by hand | S |
| C040 | T1.7 | `feat(tools): add elogctl` | `create-topic`, `describe-topic`, `dump`, `health` | Output is machine-readable (tests parse it) | Run by hand | M |
| C041 | T1.8 | `test(functional): add pytest harness` | `tests/functional/elog_test/` (Cluster, tool wrappers, `wait_until`), `conftest.py` | No fixed sleeps; processes always cleaned up | `pytest --collect-only` | M |
| C042 | T1.8 | ⚠ `feat(analysis): add CSV loaders and order/loss checks` | `analysis/elog_analysis/` loaders, `check_order`, `check_committed_loss` | These define "correct" for tests *and* experiments | Used by F-PF-1 | M |
| C043 | T1.8 | `test(functional): add produce/fetch tests` | F-PF-1 … F-PF-4 | F-PF-4 really sends garbage and an oversized frame | 🎯 **M1 demo:** `pytest -k F_PF` | M |
| C044 | T1.8 | `ci(repo): build and run functional tests on push` | `.github/workflows/ci.yml` (dev build, clang-format, fast suite) | — | CI green | S |

### Chapter 2 — Durable Storage (M2)

*The single-file log grows into a real storage engine: many segments, an index, crash recovery and retention.*

| # | Task | Commit summary | What it adds | Read carefully / learn | See it work | Size |
|---|---|---|---|---|---|---|
| C045 | T2.1 | `feat(storage): add SparseIndex` | In-memory entries + `.index` file, `floor` lookup, validation | Floor at exact hit, between entries, before the first entry | Builds | M |
| C046 | T2.1 | ⚠ `feat(storage): roll segments and read across them` | Multi-segment `PartitionLog`, `should_roll`, index-assisted reads from a segment snapshot | No lock held during reads; never reads past the bound | F-STO-1, F-STO-2 | M |
| C047 | T2.1 | `test(functional): add segment rolling tests` | F-STO-1, F-STO-2 | — | `pytest -k F_STO_1 or F_STO_2` | S |
| C048 | T2.2 | `feat(storage): add atomic checkpoint files and meta.properties` | tmp → fsync → rename → fsync dir; broker-id guard | **Learn:** the atomic-rename pattern | Builds | S |
| C049 | T2.2 | `feat(broker): add scheduler with flush and checkpoint jobs` | `BrokerScheduler`; flush policies; recovery point; HW checkpoint | Recovery point advances only after a successful flush | Checkpoint files appear in the data dir | M |
| C050 | T2.3 | ⚠ `feat(storage): recover a torn log tail on startup` | Scan from recovery point; stop at the first bad batch; truncate | **Learn:** crash recovery. Every failure mode exits the scan safely. | F-STO-3, F-STO-4 | M |
| C051 | T2.3 | ⚠ `feat(storage): rebuild indexes and clean orphan files on startup` | Index rebuild, orphan and `.tmp` removal | Order of recovery steps per LLD §5.6 | F-STO-5 | S |
| C052 | T2.3 | `feat(tools): add elogctl log-verify` | Offline read-only check of a data directory | Never writes to the data dir | Run on a data dir | S |
| C053 | T2.3 | `test(functional): add crash-recovery tests` | F-STO-3 … F-STO-5 | F-STO-4 cuts bytes at random positions of the last batch | `pytest -k F_STO` | M |
| C054 | T2.4 | `feat(storage): add size and time retention` | `applyRetention` scheduled job | Active segment never deleted; log start advances | F-STO-6 | S |
| C055 | T2.4 | `test(functional): add retention test` | F-STO-6 | — | 🎯 **M2 demo:** `pytest -k F_STO` | S |

### Chapter 3 — Consumer Groups (M3)

*Consumers form a group, share the partitions, survive one of them dying, and remember where they were.*

| # | Task | Commit summary | What it adds | Read carefully / learn | See it work | Size |
|---|---|---|---|---|---|---|
| C056 | T3.1 | `feat(protocol): add consumer-group messages` | JoinGroup, Heartbeat, LeaveGroup, OffsetCommit, OffsetFetch, DescribeGroup | Per-partition error fields present | Builds | M |
| C057 | T3.2 | `feat(coordinator): add repository for groups, ownership and offsets` | Tables `consumer_group`, `partition_ownership`, `committed_offset`; DAO methods | Generation + ownership diff saved in **one** transaction | Builds | M |
| C058 | T3.2 | `feat(coordinator): add the group model and join handling` | `Group`, `Member`, member IDs, Empty → PreparingRebalance, deferred join replies | **Learn:** the group state machine (HLD §10.1) | Builds | M |
| C059 | T3.2 | `feat(coordinator): add Assignor interface and round-robin` | Interface + `RoundRobinAssignor` | Deterministic and balanced | Builds | S |
| C060 | T3.2 | ⚠ `feat(coordinator): complete rebalances with ownership epochs` | Barrier, `completeRebalance`, generation bump, epoch rule | **Learn:** ownership epochs. Epoch changes only for moved partitions (or all, if eager). Persisted *before* replying. | F-GRP-1 | M |
| C061 | T3.2 | ⚠ `feat(coordinator): handle heartbeats, leaves and timeouts` | `onHeartbeat`, `onLeave`, `onTick` evictions | Leave rebalances at once; members waiting in Join are not evicted | F-GRP-2, F-GRP-3 | M |
| C062 | T3.3 | `feat(coordinator): add offset commit, offset fetch and DescribeGroup` | `OffsetManager` (unfenced until Chapter 4, marked `TODO(T4.1)`); read-only DescribeGroup | — | Builds | S |
| C063 | T3.1 | `feat(tools): add elogctl describe-group` | Prints group state, members, ownership, epochs, committed offsets | Machine-readable output | Run by hand | S |
| C064 | T3.4 | `feat(client): add membership client with heartbeat thread` | `AssignmentState`, `MembershipClient` (join, leave, heartbeat loop setting flags) | Heartbeat thread never changes the assignment | Builds | M |
| C065 | T3.4 | ⚠ `feat(client): add group consumer loop with eager rebalance` | `OffsetCommitter`, `Consumer::run`, commit → revoke → rejoin → fetch offsets | Commit before rejoin; buffered records of revoked partitions dropped | F-GRP-1 … F-GRP-4 | M |
| C066 | T3.4 | `feat(tools): add group mode to elog-consumer` | Group, timeouts, commit-interval flags | — | Run by hand | S |
| C067 | T3.6 | ⚠ `feat(analysis): add skip and duplicate checks` | `check_no_skips`, `count_duplicates` | Skips measured against the *produced* range, not the consumer's view | Used by F-GRP-2 | S |
| C068 | T3.6 | `test(functional): add consumer-group tests` | F-GRP-1 … F-GRP-4 | — | `pytest -k F_GRP` | M |
| C069 | T3.5 | `feat(coordinator): restore group state on restart` | Reload generations and ownership; broker grace period | Groups come back Empty with their generation | F-GRP-5 | S |
| C070 | T3.5 | `test(functional): add coordinator-restart test` | F-GRP-5 | — | 🎯 **M3 demo:** `pytest -k F_GRP` | S |

### Chapter 4 — Fencing (M4) ★

*The centrepiece. A consumer that was declared dead wakes up and tries to commit, and is stopped by one SQL statement.*

| # | Task | Commit summary | What it adds | Read carefully / learn | See it work | Size |
|---|---|---|---|---|---|---|
| C071 | T4.1 | ⚠ `feat(coordinator): add the fenced offset-commit statement` | `Repository::commitOffsetFenced` (LLD §8.3) | **Learn:** the whole fencing guarantee is this one statement. One row changed means accepted. | Builds | S |
| C072 | T4.1 | ⚠ `feat(coordinator): enforce fencing on offset commits` | `OffsetManager` uses the CAS; `STALE_EPOCH`; `coordinator.fencing.enabled`; fencing events and metrics | No in-memory membership pre-check (HLD §8.2) | Exercised by F-FEN-1 | S |
| C073 | T4.2 | ⚠ `feat(client): revoke and rejoin on STALE_EPOCH` | `on_fenced` path | All buffered records dropped; member ID reset | Exercised by F-FEN-1 | S |
| C074 | T4.3 | `feat(analysis): add offset-regression check` | `count_offset_regressions` from commit events | — | Used by F-FEN-1/2 | S |
| C075 | T4.3 | `test(functional): add zombie-consumer fencing tests` | F-FEN-1 (fenced) and F-FEN-2 (unfenced, must regress) | F-FEN-2 proves the test can fail | 🎯 **M4 demo:** `pytest -k F_FEN` | M |
| C076 | T4.3 | `docs(repo): add fencing walkthrough` | `docs/reviews/fencing-walkthrough.md`: your trace of the path, client → event loop → SQL → reply | Your own understanding, written down | — | S |

### Chapter 5 — Replication (M5)

*Each partition gets three copies. Records commit only when the in-sync replicas have them, and a dead leader is replaced without losing committed data.*

| # | Task | Commit summary | What it adds | Read carefully / learn | See it work | Size |
|---|---|---|---|---|---|---|
| C077 | T5.1 | `feat(protocol): add replication messages` | ReplicaFetch, OffsetsForLeaderEpoch, AlterIsr, full LeaderAndIsr | — | Builds | M |
| C078 | T5.2 | ⚠ `feat(broker): add leader and follower roles` | `LeaderState`, `FollowerProgress`, `becomeLeader` / `becomeFollower` | Parked produces fail with `NOT_LEADER` on a role change | Builds | M |
| C079 | T5.2 | ⚠ `feat(broker): advance HW over the in-sync replicas` | `maybeAdvanceHw` over ISR ∪ pending, with minISR | **Learn:** the commit rule (ADR-03, ADR-13) | Exercised by F-REP-1 | S |
| C080 | T5.3 | ⚠ `feat(broker): serve replica fetches on the leader` | `ReplicaFetchHandler`: implicit ack, caught-up tracking | Caught-up logic exactly as LLD §6.7 | Exercised by F-REP-1 | M |
| C081 | T5.3 | `feat(broker): add follower fetcher threads` | `ReplicaFetcherThread`, `ReplicaFetcherManager` | Threads stopped cleanly when roles change | F-REP-1 | M |
| C082 | T5.3 | `test(functional): add replica convergence test` | F-REP-1 | — | `pytest -k F_REP_1` | S |
| C083 | T5.4 | ⚠ `feat(storage): add leader-epoch cache and log truncation` | `LeaderEpochCache`, `PartitionLog::truncateTo` | Truncation on batch boundaries; epoch cache trimmed with the log | Builds | M |
| C084 | T5.4 | ⚠ `feat(broker): truncate followers by leader epoch` | `OffsetsForLeaderEpoch` handler and follower truncation loop | **Learn:** why HW-based truncation can lose data (ADR-10). Never truncates below HW. | F-REP-5 | M |
| C085 | T5.5 | ⚠ `feat(broker): propose ISR shrink and expansion` | Scheduled shrink check; expansion on caught-up fetch; `pendingIsr` | Shrink is *proposed*, not applied locally | Exercised by F-REP-3 | S |
| C086 | T5.5 | ⚠ `feat(broker): send AlterIsr and apply the committed ISR` | `CoordinatorAgent::alterIsr`, `onIsrCommitted` | No network call under the partition lock | Exercised by F-REP-3 | S |
| C087 | T5.5 | ⚠ `feat(coordinator): handle AlterIsr` | Epoch-checked, persisted before reply | Rejects stale epoch or non-leader sender | F-REP-3, F-REP-4 | S |
| C088 | T5.5 | `test(functional): add ISR change tests` | F-REP-3, F-REP-4 | — | `pytest -k "F_REP_3 or F_REP_4"` | S |
| C089 | T5.6 | ⚠ `feat(coordinator): elect new leaders when a broker dies` | Broker timeout, ISR-only election, epoch bump, push; full state for re-registering brokers | **Learn:** ISR-only election. No unclean election; persisted before push. | F-REP-2 | M |
| C090 | T5.6 | `feat(client): recover from leader changes` | Metadata refresh and retry on `NOT_LEADER`, `FENCED_LEADER_EPOCH`, socket errors | Bounded retries with backoff | F-REP-2 | S |
| C091 | T5.6 | `test(functional): add leader failover tests` | F-REP-2, F-REP-5 | Loss check compares producer acks with `elogctl dump` | 🎯 **M5 demo:** `pytest -k F_REP` | M |

### Chapter 6 — Backpressure and At-Least-Once (M6)

*Show that memory stays flat under overload, and count exactly how many duplicates a crash causes.*

| # | Task | Commit summary | What it adds | Read carefully / learn | See it work | Size |
|---|---|---|---|---|---|---|
| C092 | T6.1 | `feat(client): add flow-control metrics` | Consumer `inflight_depth`, `fetch_credit`, `consumer_lag_records`, `process_rss_bytes` | Sampled at flush, not per record | Exercised by F-BP-1 | S |
| C093 | T6.1 | `feat(broker): add broker and producer metrics` | `pending_produce`, throttle counters, LEO/HW/ISR/log-size gauges | Labels per LLD §13.2 | Visible in `metrics-*.csv` | S |
| C094 | T6.2 | `feat(client): add --crash-after-process hook` | Fixed `k` or seeded random; fires after processing, before commit | **Learn:** the at-least-once window | Exercised by F-ALO-1 | S |
| C095 | T6.3 | `test(functional): add backpressure and crash-duplicate tests` | F-BP-1, F-ALO-1 | F-ALO-1 expects *exactly* k duplicates | 🎯 **M6 demo:** `pytest -k "F_BP or F_ALO"` | M |

### Chapter 7 — Sticky Assignment and Experiments (M7)

*The smarter assignor, then the machinery that turns the working system into measured results.*

| # | Task | Commit summary | What it adds | Read carefully / learn | See it work | Size |
|---|---|---|---|---|---|---|
| C096 | T7.1 | ⚠ `feat(coordinator): add StickyAssignor` | LLD §9.3 algorithm | **Learn:** the optimality argument. Extra quota goes to members that already own the most. | Exercised by F-ASG-1 | M |
| C097 | T7.1 | `feat(client): keep retained partitions running in sticky rebalances` | Non-eager branch of `on_rebalance` | No pause and no offset fetch for retained partitions | F-ASG-1 | S |
| C098 | T7.1 | `test(functional): add assignment tests` | F-ASG-1, F-ASG-2 | — | `pytest -k F_ASG` | S |
| C099 | T7.2 | `build(deploy): add build and runtime images` | `Dockerfile.build` (pinned toolchain), `Dockerfile.runtime` | Toolchain versions pinned | `docker build` succeeds | S |
| C100 | T7.2 | `build(deploy): add compose topology` | `compose.yaml`: 3 brokers, coordinator, 8 consumers, producer, optional Toxiproxy; caps and volumes | Resource caps per HLD §16.1 | `docker compose up` → `elogctl health` | M |
| C101 | T7.3 | `feat(experiments): add run directory and metadata helpers` | `lib/common.sh`, `lib/run_meta.py` (`run.json`) | `run.json` records git SHA, seed and effective config | Creates a run dir | S |
| C102 | T7.3 | `feat(experiments): add fault-injection helpers` | `lib/faults.sh`: signals, `tc netem`, Toxiproxy fallback, `faults.csv` | Each action timestamped *before* it runs | Dry run logs faults | M |
| C103 | T7.4 | `feat(analysis): add timing measurements` | Rebalance time, time to full throughput, drain rate | Window definitions per LLD §13.4 | Used by E2–E4, E9 | M |
| C104 | T7.5 | `feat(experiments): add E1 steady state and make targets` | `e1_steady_state.sh`, `plot_e1.py`, `Makefile` | Template for the other experiments; review once, carefully | `make e1` | M |
| C105 | T7.5 | `feat(experiments): add E2 consumer kill` | Script + plot | — | `make e2` | S |
| C106 | T7.5 | `feat(experiments): add E3 session-timeout sweep` | Script + plot | Sweep values per LLD §16.2 | `make e3` | S |
| C107 | T7.5 | `feat(experiments): add E4 eager vs. sticky` | Script + plot | Same seed and workload for both arms | `make e4` | S |
| C108 | T7.5 | `feat(experiments): add E5 zombie fencing` | Script + plot | Unfenced run clearly labelled | `make e5` | S |
| C109 | T7.5 | `feat(experiments): add E6 leader failover` | Script + plot | — | `make e6` | S |
| C110 | T7.5 | `feat(experiments): add E7 overload backpressure` | Script + plot (Q sweep) | — | `make e7` | S |
| C111 | T7.5 | `feat(experiments): add E8 crash duplicates` | Script + plot (N sweep, histogram, linear fit) | Time-based commit trigger disabled | `make e8` | S |
| C112 | T7.5 | `feat(experiments): add E9 backlog drain` | Script + plot | — | `make e9` | S |
| C113 | T7.6 | `docs(experiments): record calibration` | `experiments/CALIBRATION.md`; tuned workload rates | Defaults change only with data behind them | — | S |

### Chapter 8 — Wrap-Up (M8)

*Check for races once, run everything for real, and write it up.*

| # | Task | Commit summary | What it adds | Read carefully / learn | See it work | Size |
|---|---|---|---|---|---|---|
| C114 | T8.1 | ⚠ `fix(<scope>): <race description> — one commit per race found` | Minimal fix | Message quotes the ThreadSanitizer report; the fix targets the cause | Suite passes under `tsan` | S |
| C115 | T8.2 | `docs(experiments): add campaign results summary` | `results/summary.md`, aggregated CSVs and figures (raw data archived outside git, linked by checksum) | Repository stays small | — | M |
| C116 | T8.3 | `docs(repo): add README` | Build, run, demo, layout, links to docs | A fresh clone works from the README alone | Follow it on a clean checkout | S |
| C117 | T8.3 | `docs(repo): add protocol and storage-format references` | `docs/protocol.md`, `docs/storage-format.md` | Consistent with the code as built | — | M |
| C118 | T8.5 | `feat(experiments): add live demo script` | `experiments/demo.sh` (E5 + E8, narrated) | Runs in under 10 minutes | `./experiments/demo.sh` | S |
| C119 | T8.4 | `docs(repo): add final report` | `docs/final-report.md` | Every SRS §9.1 prediction addressed | — | L |

---

## 8. Summary

| Chapter | Milestone | Commits | ⚠ Critical |
|---|---|---|---|
| Chapter 0 — Setup | M0 | 15 | 1 |
| Chapter 1 — Walking Skeleton | M1 | 29 | 7 |
| Chapter 2 — Durable Storage | M2 | 11 | 3 |
| Chapter 3 — Consumer Groups | M3 | 15 | 4 |
| Chapter 4 — Fencing | M4 | 6 | 3 |
| Chapter 5 — Replication | M5 | 15 | 9 |
| Chapter 6 — Backpressure and At-Least-Once | M6 | 4 | 0 |
| Chapter 7 — Sticky Assignment and Experiments | M7 | 18 | 1 |
| Chapter 8 — Wrap-Up | M8 | 6 | 1 |
| **Total** | | **119** (+ one per race fix) | **29** |

### 8.1 Core Reading Path

To understand the key ideas without reading every commit, read these in order:

| Concept | Commits |
|---|---|
| Wire and record format | C015, C019 |
| Log on disk | C021, C046, C050 |
| Serving data | C026, C027 |
| Single-threaded coordinator | C030 |
| Consumer groups | C060, C065 |
| **Fencing** | C071, C072, C075 |
| Replication | C079, C084, C089 |
| Sticky assignment | C096 |

---

## 9. Changing This Plan

1. **Splitting** a commit is always fine: use letter suffixes (C042a, C042b); never renumber.
2. **Merging or moving** commits is your call; note it in the table with a strike-through.
3. **Unplanned commits** (bug fixes, missed pieces) take a letter suffix after the commit they relate to, or `C2xx` if unrelated.
4. Once a task is merged, its rows are final.

