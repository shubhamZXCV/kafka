# Software Requirements Specification

## Distributed Event Log with Dynamic Consumer Rebalancing

| Field | Value |
|---|---|
| Project | #49 — Distributed Event Log with Dynamic Consumer Rebalancing |
| Course | Distributed Systems |
| Document type | Functional & Non-Functional Requirements Specification |
| Version | 1.0 |
| Status | Draft for review |
| Date | 2026-10-26 |
| Source | [writeup.pdf](writeup.pdf) (Project Write-Up, 2026-10-16) |

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Overall Description](#2-overall-description)
3. [System Architecture Overview](#3-system-architecture-overview)
4. [Functional Requirements](#4-functional-requirements)
5. [Non-Functional Requirements](#5-non-functional-requirements)
6. [Correctness Invariants](#6-correctness-invariants)
7. [External Interface Requirements](#7-external-interface-requirements)
8. [Environment & Platform Requirements](#8-environment--platform-requirements)
9. [Verification & Acceptance](#9-verification--acceptance)
10. [Traceability Matrix](#10-traceability-matrix)
11. [Deliverables](#11-deliverables)
12. [Risks, Assumptions & Known Limitations](#12-risks-assumptions--known-limitations)
13. [Glossary](#13-glossary)

---

## 1. Introduction

### 1.1 Purpose

This document specifies the functional and non-functional requirements for a Kafka-inspired distributed event log: a partitioned, replicated, append-only commit log with consumer groups, persistent offsets, and dynamic partition rebalancing. It is the reference for implementation, testing and evaluation, and is the baseline against which the final report measures what was built.

### 1.2 Scope

**In scope**

- Topic/partition model with key-hash partitioning and per-partition ordering.
- Durable, offset-addressable segment storage with a sparse index, segment rolling and retention.
- Leader/follower replication with an in-sync replica (ISR) set and quorum acknowledgement.
- Consumer groups with a group coordinator, heartbeats, session timeouts and rebalancing.
- Generation-based fencing of offset commits.
- Persistent offset storage and resume-from-last-committed-offset.
- Credit-based backpressure from consumers to brokers to producers.
- At-least-once delivery semantics, with measured duplication.
- Fault injection, metrics, and nine scripted evaluation experiments (E1–E9).

**Out of scope**

- Exactly-once semantics / transactional producers.
- Coordinator high availability (the coordinator is a documented single point of failure).
- Authentication, authorization, and encryption in transit or at rest.
- Multi-datacenter replication, log compaction, schema registry, and wire compatibility with Apache Kafka.
- Production-grade throughput optimisation; throughput is reported as context only.

### 1.3 Intended Audience

| Reader | Uses this document to |
|---|---|
| Implementers | Know what each component must do and the constraints it must meet |
| Test authors | Derive unit, integration, property-based and fault-injection tests |
| Course staff / reviewers | Assess completeness and judge results against stated predictions |

### 1.4 Requirement Conventions

- **Identifiers.** `FR-<AREA>-NN` for functional, `NFR-<CATEGORY>-NN` for non-functional, `INV-NN` for correctness invariants.
- **Priority (MoSCoW).**
  - **Must** — required for the project to be considered complete.
  - **Should** — expected; omission needs a written justification in the final report.
  - **Could** — desirable if time permits.
- **Keywords.** "shall" denotes a mandatory requirement; "should" a recommended one; "may" an optional one.
- **Verification method.** **T** = automated test, **D** = scripted demonstration, **A** = analysis/measurement, **I** = inspection/review.

---

## 2. Overall Description

### 2.1 Product Perspective

The system is a self-contained, from-scratch implementation in C++20. It does not depend on or interoperate with Apache Kafka. Redpanda (a C++ reimplementation of Kafka) is cited as precedent for the language choice. All components run as ordinary OS processes communicating over TCP and can run on a single laptop.

### 2.2 Product Functions (Summary)

1. Producers publish keyed records to a topic.
2. Records are routed to partitions by key hash and appended to the partition leader's log.
3. Followers replicate from the leader; records commit once a quorum of the ISR has persisted them.
4. Consumers in a group are each assigned an exclusive set of partitions and fetch committed records only.
5. The coordinator detects membership changes and failures and rebalances partitions, incrementing a generation each time.
6. Consumers commit offsets; commits from stale generations are rejected.
7. Consumers advertise fetch credit; saturated consumers stop fetching, throttling the pipeline back to producers.

### 2.3 User Classes

| Actor | Description |
|---|---|
| Producer client | Application that publishes records through the client library |
| Consumer client | Group member that fetches, processes, and commits offsets |
| Operator / experimenter | Starts clusters, injects faults, runs experiments, collects metrics |
| Test harness | Automated suites (unit, integration, property-based, fault-injection) |

### 2.4 Design and Implementation Constraints

| ID | Constraint |
|---|---|
| C-01 | Implementation language shall be C++20, built with GCC 13+ or Clang 17+. |
| C-02 | Build system shall be CMake 3.25+, using FetchContent for third-party dependencies. |
| C-03 | Networking shall use blocking POSIX sockets with one thread per connection. |
| C-04 | The coordinator shall be single-threaded, processing all requests in order from one inbound queue. |
| C-05 | Log segments shall be hand-built append-only files written with `pwrite`. |
| C-06 | Committed offsets shall be stored in SQLite. |
| C-07 | Control-plane messages shall be encoded as JSON (nlohmann/json); record payloads as MessagePack. |
| C-08 | No GPUs, licensed software, or external datasets; all workloads are synthetic. |

---

## 3. System Architecture Overview

```
                 ┌────────────────────────────────────────────┐
                 │            Group Coordinator               │
                 │  (single-threaded, inbound request queue)  │
                 │  membership · heartbeats · assignment ·    │
                 │  generations · leader appointment          │
                 │            ┌───────────────┐               │
                 │            │ SQLite offset │               │
                 │            │     store     │               │
                 │            └───────────────┘               │
                 └───────▲──────────────────────▲─────────────┘
                         │ join/heartbeat/      │ leader appointment,
                         │ commit/leave         │ ISR updates
                         │                      │
 ┌──────────┐  produce   │   ┌──────────────────┴──────────────────┐
 │ Producer │──────────────▶ │ Broker 1 (leader P0)  ◀── replicate ─┤
 │ (keyed)  │            │   │ Broker 2 (leader P1)  ── pull ──────▶│
 └──────────┘            │   │ Broker 3 (leader P2)                 │
                         │   │  segments + sparse index per replica │
                         │   └──────────────────▲──────────────────┘
                         │                      │ fetch(credit) →
                         │                      │ ← committed records
                 ┌───────┴──────────────────────┴───────┐
                 │  Consumer group (C1 … Cn)            │
                 │  bounded in-flight queue per consumer │
                 └──────────────────────────────────────┘
```

| Component | Responsibility |
|---|---|
| **Broker** | Hosts partition replicas; serves produce (as leader), replication fetch (as leader) and consumer fetch; manages segments, indices and retention. |
| **Group coordinator** | Tracks group membership and liveness, computes assignments, issues generations, validates and persists offset commits, appoints partition leaders, and tracks ISR membership. |
| **Offset store** | SQLite database holding `(group, topic, partition) → (offset, generation)`; performs fenced compare-and-set. |
| **Client library** | Producer (partitioner, batching, acks) and consumer (group protocol, fetch with credit, bounded queue, commit). |
| **Tooling** | Fault injection, workload generators, metric export, plotting, demo scripts. |

---

## 4. Functional Requirements

### 4.1 Topics and Partitioning — `FR-PART`

| ID | Requirement | Priority | Verify |
|---|---|---|---|
| FR-PART-01 | The system shall support creating a topic with a configurable partition count `P ≥ 1` and replication factor `R ≥ 1`. | Must | T |
| FR-PART-02 | The producer shall map each keyed record to a partition with a deterministic hash function: `partition = hash(key) mod P`. | Must | T |
| FR-PART-03 | All records with the same key shall go to the same partition for the lifetime of the topic (fixed `P`). | Must | T |
| FR-PART-04 | Each partition shall assign records strictly increasing, gap-free 64-bit offsets starting at 0. | Must | T |
| FR-PART-05 | Records without a key may be distributed across partitions (e.g., round-robin). | Could | T |
| FR-PART-06 | Changing the partition count of an existing topic is not supported; the system shall reject such requests with an explicit error. | Should | T |

### 4.2 Log Storage — `FR-LOG`

| ID | Requirement | Priority | Verify |
|---|---|---|---|
| FR-LOG-01 | Each partition replica shall be stored as an ordered sequence of append-only segment files, named by base offset. | Must | T, I |
| FR-LOG-02 | Appends shall be performed with `pwrite` to the active segment only; closed segments are immutable. | Must | I |
| FR-LOG-03 | Each segment shall have a sparse offset index mapping a subset of offsets to byte positions, written at a configurable interval (bytes or records). | Must | T |
| FR-LOG-04 | A read at an arbitrary offset shall locate the segment by base offset, use the sparse index for the nearest preceding entry, and scan forward to the target. | Must | T |
| FR-LOG-05 | The active segment shall roll to a new segment when it reaches a configurable size or age limit. | Must | T |
| FR-LOG-06 | The broker shall support size-based and time-based retention, deleting whole closed segments only. | Must | T |
| FR-LOG-07 | Every record batch on disk shall carry a CRC32C checksum, verified on read and on recovery. | Must | T |
| FR-LOG-08 | On startup the broker shall recover each partition by validating the tail of the active segment, truncating any torn or checksum-failing trailing bytes, and rebuilding the index if it is missing or inconsistent. | Must | T |
| FR-LOG-09 | The fsync policy shall be configurable (e.g., per batch, per interval, or never) and recorded in every experiment's metadata. | Must | I, A |
| FR-LOG-10 | Reads beyond the log end offset or below the log start offset shall return a defined `OFFSET_OUT_OF_RANGE` error. | Must | T |

### 4.3 Produce Path — `FR-PROD`

| ID | Requirement | Priority | Verify |
|---|---|---|---|
| FR-PROD-01 | The producer shall send each record to the current leader of its target partition. | Must | T |
| FR-PROD-02 | A broker that is not leader for the requested partition shall reply `NOT_LEADER` and include the current leader if known; the producer shall refresh metadata and retry. | Must | T |
| FR-PROD-03 | The producer shall support an acknowledgement mode in which success is returned only after the record is committed (quorum of ISR has persisted it). | Must | T |
| FR-PROD-04 | The producer may support weaker acknowledgement modes (leader-only, fire-and-forget) for comparison. | Could | T |
| FR-PROD-05 | The producer shall batch records per partition with a configurable batch size and linger time. | Should | T |
| FR-PROD-06 | The producer shall retry transient failures with bounded, configurable retries and backoff, and surface a terminal error after retries are exhausted. | Must | T |
| FR-PROD-07 | The producer shall respect broker backpressure: when the broker throttles or withholds acknowledgements, the producer shall block or slow down rather than buffer without limit. | Must | T, D |

### 4.4 Replication — `FR-REPL`

| ID | Requirement | Priority | Verify |
|---|---|---|---|
| FR-REPL-01 | Each partition shall have exactly one leader and `R − 1` followers, placed on distinct brokers. | Must | T |
| FR-REPL-02 | Partition leaders shall be appointed by the coordinator; each appointment shall carry a monotonically increasing leader epoch. | Must | T |
| FR-REPL-03 | Followers shall pull records from the leader and acknowledge the offset they have persisted. | Must | T |
| FR-REPL-04 | The leader shall maintain an in-sync replica (ISR) set, removing followers that fall behind beyond a configurable lag time and re-adding them once they catch up. | Must | T |
| FR-REPL-05 | The leader shall advance the partition high watermark (HW) only when a quorum of the ISR has persisted the record. | Must | T |
| FR-REPL-06 | On leader failure the coordinator shall elect a new leader from the ISR only, and increment the leader epoch. | Must | T, D |
| FR-REPL-07 | Brokers shall reject produce and replication requests carrying a stale leader epoch. | Must | T |
| FR-REPL-08 | A follower that rejoins after a leader change shall truncate any uncommitted suffix that diverges from the new leader before resuming replication. | Must | T |
| FR-REPL-09 | When the ISR shrinks below the configured minimum in-sync count, the leader shall reject committed-ack produce requests with `NOT_ENOUGH_REPLICAS` instead of acknowledging them. | Should | T |

### 4.5 Consumer Fetch — `FR-FETCH`

| ID | Requirement | Priority | Verify |
|---|---|---|---|
| FR-FETCH-01 | Consumers shall only receive records at offsets below the partition high watermark (committed records). | Must | T |
| FR-FETCH-02 | A consumer shall fetch only from partitions currently assigned to it under its current generation. | Must | T |
| FR-FETCH-03 | Within a partition, records shall be delivered to the consumer in offset order. | Must | T |
| FR-FETCH-04 | Each fetch request shall carry an explicit credit (remaining capacity of the consumer's in-flight queue); the broker shall return no more than that credit. | Must | T |
| FR-FETCH-05 | A consumer whose in-flight queue is full shall stop issuing fetches until capacity frees up. | Must | T, D |
| FR-FETCH-06 | The broker shall support long-polling: a fetch with no available records may wait up to a configurable maximum before returning empty. | Should | T |

### 4.6 Consumer Groups and Coordination — `FR-GRP`

| ID | Requirement | Priority | Verify |
|---|---|---|---|
| FR-GRP-01 | A consumer shall join a group by sending a join request to the coordinator with its group ID and a member ID. | Must | T |
| FR-GRP-02 | Consumers shall send heartbeats at a configurable interval, below the session timeout. | Must | T |
| FR-GRP-03 | The coordinator shall declare a member failed if no heartbeat arrives within the configurable session timeout. | Must | T, D |
| FR-GRP-04 | The coordinator shall trigger a rebalance when a member (a) joins, (b) leaves gracefully, or (c) is declared failed. | Must | T, D |
| FR-GRP-05 | A graceful leave shall trigger an immediate rebalance without waiting for the session timeout. | Must | T |
| FR-GRP-06 | Each rebalance shall produce a new group generation number, strictly greater than all previous generations for the group. | Must | T |
| FR-GRP-07 | Every partition shall be assigned to exactly one live member per generation; no partition shall be assigned to two members at once. | Must | T |
| FR-GRP-08 | Each member shall learn its assignment and generation in the rebalance response, and stop fetching from partitions it no longer owns. | Must | T |
| FR-GRP-09 | The coordinator shall process all join, leave, heartbeat and commit requests one at a time from a single inbound queue, so generation updates are atomic without locks. | Must | I, T |
| FR-GRP-10 | Heartbeats or commits from a member that is no longer in the group shall be rejected with `UNKNOWN_MEMBER`; the member must rejoin. | Must | T |
| FR-GRP-11 | When there are more members than partitions, surplus members shall receive an empty assignment and remain idle. | Must | T |

### 4.7 Partition Assignment Strategies — `FR-ASSIGN`

| ID | Requirement | Priority | Verify |
|---|---|---|---|
| FR-ASSIGN-01 | The coordinator shall implement a **round-robin (eager)** assignor: on rebalance, all partitions are revoked and redistributed round-robin across members. | Must | T |
| FR-ASSIGN-02 | The coordinator shall implement a **sticky** assignor that keeps as many existing partition→member pairs as possible while keeping the assignment balanced. | Must | T |
| FR-ASSIGN-03 | Under the sticky assignor, a member that keeps a partition across a rebalance shall continue consuming it without re-fetching its committed offset. | Should | T |
| FR-ASSIGN-04 | The assignment strategy shall be selectable by configuration, without recompiling, so the two can be compared under identical conditions. | Must | D |
| FR-ASSIGN-05 | Both assignors shall be deterministic for a given membership set and prior assignment. | Must | T |
| FR-ASSIGN-06 | Every assignment produced shall be balanced: the per-member partition counts differ by at most one. | Must | T |

### 4.8 Offset Management and Fencing — `FR-OFF`

> This is the correctness centrepiece of the project.

| ID | Requirement | Priority | Verify |
|---|---|---|---|
| FR-OFF-01 | Consumers shall commit offsets to the coordinator as `(group, topic, partition, offset, generation, member)`. | Must | T |
| FR-OFF-02 | The coordinator shall accept a commit only if the committing member owns the partition in the current generation. | Must | T, D |
| FR-OFF-03 | A commit carrying an older generation shall be rejected with a `STALE_EPOCH` error and shall not modify stored state. | Must | T, D |
| FR-OFF-04 | Fencing shall be a single transactional compare-and-set in SQLite, conditioned on `(partition, generation)`, so no interleaving can apply a stale commit. | Must | T, I |
| FR-OFF-05 | The committed offset for a partition shall never decrease. | Must | T |
| FR-OFF-06 | Committed offsets shall be durable: they survive coordinator restart. | Must | T |
| FR-OFF-07 | On assignment, a consumer shall resume from the last committed offset for each newly owned partition, or from a configurable reset policy (earliest/latest) when none exists. | Must | T |
| FR-OFF-08 | A consumer shall commit an offset only after the records up to that offset have been processed (process-then-commit). | Must | T, I |
| FR-OFF-09 | The commit interval (records or time) shall be configurable. | Must | T |
| FR-OFF-10 | Fencing shall be switchable off by configuration **only** for experiment E5's control run, and every run with it disabled shall be clearly marked in logs and metric output. | Must | D, I |
| FR-OFF-11 | A consumer receiving `STALE_EPOCH` shall stop processing the affected partitions, discard its in-flight records for them, and rejoin the group. | Must | T |

### 4.9 Backpressure — `FR-BP`

| ID | Requirement | Priority | Verify |
|---|---|---|---|
| FR-BP-01 | Each consumer shall buffer fetched records in an in-flight queue with a configurable maximum size. | Must | T |
| FR-BP-02 | The consumer shall advertise remaining queue capacity as credit in every fetch request (see FR-FETCH-04). | Must | T |
| FR-BP-03 | Brokers shall bound per-connection and per-partition in-memory buffers; under consumer saturation, unconsumed data shall stay in the on-disk log and not accumulate in memory. | Must | T, A |
| FR-BP-04 | Brokers shall apply backpressure to producers when bounded buffers are full, by withholding acknowledgements or rejecting requests with a retriable throttle error. | Must | T, D |
| FR-BP-05 | No component shall have an unbounded in-memory queue on the data path. | Must | I |

### 4.10 Delivery Semantics — `FR-DEL`

| ID | Requirement | Priority | Verify |
|---|---|---|---|
| FR-DEL-01 | The system shall provide at-least-once delivery: every committed record is delivered to the group at least once. | Must | T, D |
| FR-DEL-02 | After a consumer crash, processing shall resume from the last committed offset; no committed record shall be skipped. | Must | T, D |
| FR-DEL-03 | Records processed but not yet committed at crash time may be redelivered; the number of duplicates per crash shall not exceed the commit interval. | Must | D, A |
| FR-DEL-04 | The consumer shall support a `--crash-after-process` flag that terminates the process deterministically after processing and before committing, to reproduce duplicates on demand. | Must | D |

### 4.11 Fault Injection — `FR-FAULT`

| ID | Requirement | Priority | Verify |
|---|---|---|---|
| FR-FAULT-01 | Tooling shall freeze a consumer or broker with `SIGSTOP` and resume it with `SIGCONT` (simulating GC pauses / stalls). | Must | D |
| FR-FAULT-02 | Tooling shall crash any process with `SIGKILL`. | Must | D |
| FR-FAULT-03 | Tooling shall inject network latency, loss and partitions with `tc netem` when `NET_ADMIN` is available, and with Toxiproxy otherwise. | Must | D |
| FR-FAULT-04 | Every injected fault shall be logged with a timestamp, so it can be lined up with metric timelines. | Must | I |

### 4.12 Observability and Metrics — `FR-OBS`

| ID | Requirement | Priority | Verify |
|---|---|---|---|
| FR-OBS-01 | All components shall emit structured logs (spdlog) with timestamp, component, node ID, and relevant group/partition/generation/epoch fields. | Must | I |
| FR-OBS-02 | The system shall export metrics as CSV, including at least: produce/consume throughput, end-to-end latency, rebalance start/end, generation changes, committed offsets, rejected stale commits, offset regressions, duplicate count, in-flight queue depth, process RSS, consumer lag/backlog, ISR size, and leader changes. | Must | A |
| FR-OBS-03 | Plotting scripts (pandas + matplotlib) shall generate every figure in the final report from raw CSV, with no manual steps. | Must | D |
| FR-OBS-04 | Every experiment run shall record its full configuration (topology, timeouts, assignor, fsync policy, commit interval, fault schedule, git commit hash) next to its metrics. | Must | I |

### 4.13 Operations and Tooling — `FR-OPS`

| ID | Requirement | Priority | Verify |
|---|---|---|---|
| FR-OPS-01 | Each broker shall run as an independent process with its own data directory and port, configured by file and/or CLI flags. | Must | D |
| FR-OPS-02 | A Docker Compose definition shall bring up a representative cluster (3 brokers, 1 coordinator, 4–8 consumers, producers) with per-container resource caps. | Must | D |
| FR-OPS-03 | Each experiment E1–E9 shall be runnable with a single command that sets up, runs, injects faults, collects metrics and tears down. | Must | D |
| FR-OPS-04 | A synthetic workload generator shall produce keyed records with configurable rate, key cardinality, key distribution and payload size. | Must | D |

---

## 5. Non-Functional Requirements

### 5.1 Reliability and Durability — `NFR-REL`

| ID | Requirement | Target / Metric | Priority | Verify |
|---|---|---|---|---|
| NFR-REL-01 | Committed records survive the loss of any single broker at `R = 3`. | 0 committed records lost across all E6 runs | Must | T, D |
| NFR-REL-02 | Committed offsets never move backwards with fencing enabled. | 0 offset regressions across all E5 runs | Must | T, D |
| NFR-REL-03 | No committed record is skipped across crash-and-recovery. | 0 skipped records across all E2/E8 runs | Must | T, D |
| NFR-REL-04 | Broker restart recovers a consistent log after `SIGKILL` at any point. | 100% of recovery tests pass; no checksum failures past the recovered end | Must | T |
| NFR-REL-05 | Duplicate deliveries per consumer crash are bounded. | ≤ commit interval; mean ≈ half the commit interval | Must | A |

### 5.2 Availability and Recovery — `NFR-AVL`

| ID | Requirement | Target / Metric | Priority | Verify |
|---|---|---|---|---|
| NFR-AVL-01 | Rebalance completes promptly after a consumer failure. | Rebalance time ≈ session timeout + < 500 ms | Must | A (E2) |
| NFR-AVL-02 | Rebalance time scales predictably with session timeout. | Near-linear over 2–15 s | Should | A (E3) |
| NFR-AVL-03 | False evictions are characterised. | False evictions measured and reported; expected only below ~2 s session timeout | Should | A (E3) |
| NFR-AVL-04 | Sticky assignment recovers faster than eager. | Sticky reaches full throughput 40–70% faster | Should | A (E4) |
| NFR-AVL-05 | Leader failover elects a new leader from the ISR and resumes writes. | New leader elected; produce resumes without manual action | Must | D (E6) |
| NFR-AVL-06 | Backlog drains at the expected rate after rebalance. | Drain rate ≈ consumer capacity − producer rate | Should | A (E9) |

### 5.3 Performance and Resource Usage — `NFR-PERF`

| ID | Requirement | Target / Metric | Priority | Verify |
|---|---|---|---|---|
| NFR-PERF-01 | Steady-state throughput is stable. | No sustained downward trend or periodic stalls across an E1 run | Must | A (E1) |
| NFR-PERF-02 | Memory stays bounded under sustained overload. | In-flight depth and RSS flat at their configured bound with producer at 3× consumer capacity | Must | A (E7) |
| NFR-PERF-03 | Overload backlog accumulates on disk, not in memory. | Consumer lag grows on disk while broker RSS stays flat | Must | A (E7) |
| NFR-PERF-04 | Latency measurements are not distorted by runtime pauses. | Native C++, no GC; latency reported as p50/p95/p99 | Should | A |
| NFR-PERF-05 | Absolute throughput is reported as context only. | Reported with fsync policy and host specs; not a pass/fail criterion | — | A |

### 5.4 Correctness Under Concurrency — `NFR-CONC`

| ID | Requirement | Target / Metric | Priority | Verify |
|---|---|---|---|---|
| NFR-CONC-01 | The codebase is free of data races. | Clean ThreadSanitizer run of the full test suite in CI | Must | T |
| NFR-CONC-02 | The codebase is free of memory errors and undefined behaviour. | Clean ASan + UBSan runs in development | Must | T |
| NFR-CONC-03 | Concurrency bugs found in testing can be reproduced deterministically. | `rr` recording available for race replay | Should | I |

### 5.5 Scalability — `NFR-SCAL`

| ID | Requirement | Target / Metric | Priority | Verify |
|---|---|---|---|---|
| NFR-SCAL-01 | The system supports the representative topology on one host. | 3 brokers + 1 coordinator + 4–8 consumers + producers (~30 connections) | Must | D |
| NFR-SCAL-02 | Rebalance behaviour is characterised against group size. | Rebalance time plotted against group size | Should | A |
| NFR-SCAL-03 | The system can optionally be distributed across physical nodes. | 3–4 RCE nodes; same binaries and configs, only network addresses differ | Could | D |

### 5.6 Maintainability and Code Quality — `NFR-MAINT`

| ID | Requirement | Priority | Verify |
|---|---|---|---|
| NFR-MAINT-01 | Code shall be formatted with clang-format and pass clang-tidy with the project configuration; CI shall enforce both. | Must | I |
| NFR-MAINT-02 | Broker, coordinator, client library and storage engine shall be separate CMake targets with defined interfaces. | Must | I |
| NFR-MAINT-03 | The repository shall build from a clean checkout with one CMake configure + build command, fetching dependencies with FetchContent. | Must | D |
| NFR-MAINT-04 | Public interfaces, wire protocol and on-disk format shall be documented in the repository. | Must | I |
| NFR-MAINT-05 | Assignment strategies shall share one assignor interface so new strategies can be added without changing the coordinator core. | Should | I |

### 5.7 Testability — `NFR-TEST`

| ID | Requirement | Priority | Verify |
|---|---|---|---|
| NFR-TEST-01 | Unit tests (GoogleTest/Catch2) shall cover storage, index, partitioner, assignors, fencing and protocol framing. | Must | T |
| NFR-TEST-02 | Property-based tests (RapidCheck) shall check the correctness invariants of [Section 6](#6-correctness-invariants) over randomised operation sequences. | Must | T |
| NFR-TEST-03 | Integration tests shall start multi-process clusters and run fault scenarios automatically. | Must | T |
| NFR-TEST-04 | CI shall run the full suite on every push to the main branch and on every pull request. | Must | I |
| NFR-TEST-05 | Timing-dependent logic (heartbeats, session timeouts, ISR lag) shall use an injectable clock, so tests can run deterministically. | Should | I |

### 5.8 Reproducibility — `NFR-REPRO`

| ID | Requirement | Priority | Verify |
|---|---|---|---|
| NFR-REPRO-01 | All experiments shall run under the same containerised, resource-capped conditions, so comparisons are like for like. | Must | I |
| NFR-REPRO-02 | Container images used to build and evaluate shall pin compiler and dependency versions. | Must | I |
| NFR-REPRO-03 | Workload generators and randomised tests shall take a seed, and record it in run metadata. | Must | I |
| NFR-REPRO-04 | Each experiment shall be repeated enough times to report variance (mean and spread, or confidence intervals). | Should | A |
| NFR-REPRO-05 | Raw CSV data for every reported figure shall be committed or archived with the final report. | Must | I |

### 5.9 Portability — `NFR-PORT`

| ID | Requirement | Priority | Verify |
|---|---|---|---|
| NFR-PORT-01 | The system shall build and run on Linux x86-64 with GCC 13+ or Clang 17+. | Must | D |
| NFR-PORT-02 | Where the host toolchain is too old, a pinned build container shall produce identical binaries. | Must | D |
| NFR-PORT-03 | Network fault injection shall fall back from `tc netem` to Toxiproxy when `NET_ADMIN` is unavailable, with no change to experiment scripts beyond a flag. | Must | D |

### 5.10 Security — `NFR-SEC`

| ID | Requirement | Priority | Verify |
|---|---|---|---|
| NFR-SEC-01 | Services shall bind to loopback or the container network by default. | Should | I |
| NFR-SEC-02 | All network input shall be length-bounded and checksum-validated before parsing; malformed frames shall close the connection without crashing the process. | Must | T |
| NFR-SEC-03 | Authentication, authorisation and TLS are explicitly out of scope; the final report shall say so. | Must | I |

---

## 6. Correctness Invariants

These invariants are binary pass/fail. They shall be asserted in the automated test suite (property-based where possible) and checked in the relevant experiments.

| ID | Invariant | Checked by |
|---|---|---|
| INV-01 | **Per-partition order.** Within a partition, consumers observe records in strictly increasing offset order, and same-key records keep produce order. | E1, property tests |
| INV-02 | **Single ownership.** At no generation is any partition assigned to more than one consumer. | E2, E4, property tests |
| INV-03 | **Monotonic committed offset.** With fencing enabled, the committed offset for a `(group, partition)` never decreases. | E5, property tests |
| INV-04 | **Fencing.** Every commit carrying a generation older than the current one is rejected and leaves state unchanged. | E5, unit tests |
| INV-05 | **No skipped records.** After any crash/recovery, every record below the high watermark is delivered at least once to the group. | E2, E8, E9 |
| INV-06 | **Committed durability.** At `R = 3`, no record below the high watermark is lost when any single broker fails. | E6 |
| INV-07 | **Committed-only reads.** No consumer observes a record at or above the high watermark. | E6, unit tests |
| INV-08 | **Monotonic generation / epoch.** Group generations and leader epochs strictly increase. | Unit and property tests |
| INV-09 | **Bounded memory.** In-flight queue depth never exceeds its configured bound. | E7, unit tests |
| INV-10 | **Bounded duplication.** Duplicates caused by one consumer crash do not exceed the commit interval. | E8 |

---

## 7. External Interface Requirements

### 7.1 Wire Protocol

| ID | Requirement | Priority |
|---|---|---|
| IF-01 | Every message shall be framed as a 4-byte big-endian length prefix, followed by the body and a CRC32C checksum. | Must |
| IF-02 | Control-plane bodies (join, leave, heartbeat, commit, metadata, leader appointment) shall be JSON. | Must |
| IF-03 | Record payloads on the produce/fetch/replication path shall be MessagePack. | Must |
| IF-04 | Every request shall carry a request type, a correlation ID and a protocol version; responses shall echo the correlation ID. | Must |
| IF-05 | Errors shall be returned as typed codes. The minimum set is: `NOT_LEADER`, `STALE_EPOCH`, `UNKNOWN_MEMBER`, `REBALANCE_IN_PROGRESS`, `OFFSET_OUT_OF_RANGE`, `NOT_ENOUGH_REPLICAS`, `THROTTLED`, `CORRUPT_MESSAGE`. | Must |
| IF-06 | A maximum frame size shall be configured and enforced; larger frames are rejected. | Must |

### 7.2 Request Catalogue (minimum)

| Request | From → To | Purpose |
|---|---|---|
| `Metadata` | Client → Broker/Coordinator | Discover topics, partitions, leaders, epochs |
| `Produce` | Producer → Leader | Append records |
| `ReplicaFetch` | Follower → Leader | Pull records; report persisted offset |
| `Fetch` | Consumer → Leader | Fetch committed records with credit |
| `JoinGroup` | Consumer → Coordinator | Join group; receive assignment and generation |
| `Heartbeat` | Consumer → Coordinator | Liveness; learn about pending rebalance |
| `LeaveGroup` | Consumer → Coordinator | Graceful departure |
| `OffsetCommit` | Consumer → Coordinator | Fenced commit `(partition, offset, generation)` |
| `OffsetFetch` | Consumer → Coordinator | Read last committed offset |
| `LeaderAndIsr` | Coordinator ↔ Broker | Leader appointment, epoch and ISR updates |

### 7.3 On-Disk Formats

| ID | Requirement | Priority |
|---|---|---|
| IF-07 | The segment file format (record batch header, offsets, timestamps, key/value lengths, CRC32C) shall be versioned and documented. | Must |
| IF-08 | The sparse index format (relative offset → byte position) shall be documented, and rebuildable from the segment alone. | Must |
| IF-09 | The SQLite offset-store schema shall be documented, including the compare-and-set statement used for fencing. | Must |

### 7.4 Configuration Interface

| ID | Requirement | Priority |
|---|---|---|
| IF-10 | Every tunable referenced in this document (partitions, replication factor, min ISR, segment size/age, retention, index interval, fsync policy, session timeout, heartbeat interval, ISR lag time, assignor, commit interval, in-flight queue size, batch size, linger, retries, fencing toggle) shall be configurable without recompilation. | Must |
| IF-11 | Effective configuration shall be logged at startup. | Must |

---

## 8. Environment & Platform Requirements

### 8.1 Hardware

| Resource | Minimum | Recommended |
|---|---|---|
| CPU | 4 cores | 8 cores |
| Memory | 8 GB | 16 GB |
| Storage | 20 GB free **local SSD** | 20 GB+ local SSD |

> A local SSD is mandatory. Segments are fsynced, and a network-mounted home directory would distort durability and latency measurements.

### 8.2 Software

| Layer | Technology |
|---|---|
| Language / build | C++20 (GCC 13+ / Clang 17+), CMake 3.25+ with FetchContent |
| Transport | POSIX sockets, blocking, thread-per-connection; 4-byte BE length prefix, CRC32C |
| Encoding | nlohmann/json (control plane), MessagePack (records) |
| Log storage | Custom append-only segments via `pwrite`, sparse index, rolling, size/time retention |
| Offset store | SQLite |
| Coordination | Self-built single-instance group coordinator |
| Replication | Leader/follower, ISR, quorum ack, coordinator-appointed leaders |
| Assignment | Round-robin (eager) and sticky |
| Fault injection | `SIGSTOP`, `SIGKILL`, `--crash-after-process`, `tc netem` / Toxiproxy |
| Testing | GoogleTest/Catch2, RapidCheck, ASan + UBSan, ThreadSanitizer (CI), `rr` |
| Observability | spdlog, CSV export, pandas + matplotlib |
| Workflow | Git/GitHub + CI, clang-format/clang-tidy, Docker Compose |

### 8.3 Privileges and Optional Infrastructure

- **`NET_ADMIN`** is needed inside containers for `tc netem`. Without it, use Toxiproxy (see NFR-PORT-03).
- **Research Computing Environment (optional):** 3–4 nodes × 8 cores × 16 GB, used only to raise the fidelity of the scaled runs. **No deliverable depends on cluster access**; without it, those runs fall back to one host with emulated latency at reduced scale.

---

## 9. Verification & Acceptance

### 9.1 Experiment Plan

Each experiment shall be scripted, runnable with one command (FR-OPS-03), and produce raw CSV plus generated figures.

| # | Experiment | Demonstrates | Acceptance criterion |
|---|---|---|---|
| E1 | Steady-state produce/consume | Partitioning, replication | Stable throughput; per-partition order never violated (INV-01) |
| E2 | Kill one consumer | Coordination | Rebalance time ≈ session timeout + < 500 ms; no records skipped |
| E3 | Sweep session timeout 2–15 s | Coordination | Near-linear rebalance time; false evictions measured, expected below ~2 s |
| E4 | Eager vs. sticky assignment | Coordination | Sticky reaches full throughput 40–70% faster |
| E5 ★ | Freeze consumer (`SIGSTOP`), let it wake (`SIGCONT`) | Fencing | **0 offset regressions with fencing; > 0 without** |
| E6 | Kill partition leader | Replication | New leader elected from the ISR; 0 committed records lost |
| E7 | Producer at 3× consumer capacity | Backpressure | In-flight depth and RSS flat at bound; backlog on disk |
| E8 ★ | Crash between process and commit | At-least-once | **Duplicates ≤ commit interval, mean ≈ half**; linear in commit interval |
| E9 | Backlog drain after E2 | Coordination | Drain rate ≈ consumer capacity − producer rate |

★ Headline demonstrations, to be shown live.

### 9.2 Acceptance Criteria (Project-Level)

The project is accepted when all of the following hold:

1. All **Must** requirements are implemented, or their absence is justified in the final report.
2. All correctness invariants INV-01…INV-10 hold in the automated test suite, which passes in CI under ThreadSanitizer.
3. E5 and E8 are demonstrated live, with the results above.
4. Every figure in the final report can be regenerated from committed raw CSV by the plotting scripts.
5. Every quantitative prediction in §9.1 is evaluated against measurement. **Predictions the measurements contradict are reported as findings, with analysis, not hidden or treated as failures.**

---

## 10. Traceability Matrix

| Goal (from write-up) | Requirements | Invariants | Experiments |
|---|---|---|---|
| G1 — Partitioned, replicated, durable log with per-partition ordering | FR-PART-\*, FR-LOG-\*, FR-PROD-\*, FR-REPL-\*, NFR-REL-01, NFR-REL-04 | INV-01, INV-06, INV-07 | E1, E6 |
| G2 — Consumer groups with reassignment on join/leave/timeout | FR-GRP-\*, FR-ASSIGN-\*, NFR-AVL-01…04 | INV-02, INV-08 | E2, E3, E4 |
| G3 — Generation-based fencing of offset commits | FR-OFF-02…05, FR-OFF-10, FR-OFF-11, NFR-REL-02 | INV-03, INV-04 | E5 |
| G4 — Persistent offsets, resume without skipping; bounded queues with credit backpressure | FR-OFF-06…09, FR-FETCH-04/05, FR-BP-\*, NFR-PERF-02/03 | INV-05, INV-09 | E2, E7, E9 |
| G5 — Demonstrate and quantify at-least-once duplication | FR-DEL-\*, NFR-REL-05 | INV-05, INV-10 | E8 |
| G6 — Measure rebalance time, drain rate, duplication, latency; compare eager vs. sticky | FR-OBS-\*, FR-OPS-\*, NFR-AVL-\*, NFR-REPRO-\* | — | E2–E4, E8, E9 |

---

## 11. Deliverables

| # | Deliverable | Related requirements |
|---|---|---|
| D1 | Documented C++20 repository with CMake build: broker, coordinator, client library | NFR-MAINT-\*, IF-07…09 |
| D2 | Nine scripted demo scenarios (E1–E9), each runnable with one command | FR-OPS-03 |
| D3 | Automated unit, integration and property-based test suite, run in CI under ThreadSanitizer | NFR-TEST-\*, NFR-CONC-01 |
| D4 | Raw CSV metric exports, with plotting scripts that generate every figure | FR-OBS-02/03, NFR-REPRO-05 |
| D5 | Final report evaluating measurements against the predictions | §9.2 |
| D6 | Live demonstration of fencing (E5) and duplicate delivery (E8) | FR-OFF-\*, FR-DEL-\* |

---

## 12. Risks, Assumptions & Known Limitations

### 12.1 Known Limitations (by design)

| ID | Limitation | Mitigation / Disclosure |
|---|---|---|
| L-01 | The group coordinator is a single point of failure. | Documented in the final report; offsets persist in SQLite, so a restarted coordinator recovers committed state. |
| L-02 | Delivery is at-least-once, not exactly-once. | Duplication is bounded and quantified (E8); consumers needing idempotence must provide it. |
| L-03 | Thread-per-connection does not scale to thousands of connections. | Adequate for the ~30-connection target; chosen to avoid coroutine lifetime hazards. |
| L-04 | Single-host evaluation shares CPU and disk between nodes. | Resource-capped containers, emulated latency, and like-for-like conditions; throughput reported as context only. |

### 12.2 Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Lab compilers are too old for C++20 | High | Medium | Pinned build container (NFR-PORT-02) |
| `NET_ADMIN` is unavailable | Medium | Low | Toxiproxy fallback (NFR-PORT-03) |
| Loopback hides real replication lag | Medium | Medium | Emulated latency; optional RCE multi-node runs |
| Timing-sensitive tests are flaky in CI | Medium | Medium | Injectable clock (NFR-TEST-05); `rr` replay |
| Contention from fsync dominates measurements | Medium | Low | Record fsync policy per run; keep it fixed within each comparison |
| No RCE allocation | Medium | Low | No deliverable depends on it |

### 12.3 Assumptions

- Clocks on a single host are close enough for timeouts; no cross-host clock sync is assumed beyond NTP.
- Process freeze (`SIGSTOP`) is a faithful stand-in for GC pauses and network stalls.
- Synthetic workloads are representative enough to demonstrate the protocol properties under study.

---

## 13. Glossary

| Term | Definition |
|---|---|
| **Assignment** | The mapping of partitions to consumers within a group for a given generation. |
| **At-least-once** | Delivery guarantee where every committed record is processed one or more times. |
| **Commit interval** | Number of records (or time) between consecutive offset commits by a consumer. |
| **Committed record** | A record persisted by a quorum of the ISR, i.e., below the high watermark. |
| **Committed offset** | The position stored in the offset store from which a group resumes a partition. |
| **Credit** | Remaining capacity of a consumer's in-flight queue, sent in each fetch request. |
| **Eager assignment** | Rebalance that revokes all partitions and reassigns from scratch (round-robin here). |
| **Fencing** | Rejecting operations from a participant whose generation/epoch is out of date. |
| **Generation** | Monotonically increasing number identifying one group assignment. |
| **High watermark (HW)** | The highest offset known committed on a partition; consumers read only below it. |
| **ISR** | In-sync replica set: replicas close enough to the leader to count toward the commit quorum. |
| **Leader epoch** | Monotonically increasing number identifying one leadership term of a partition. |
| **Offset regression** | A committed offset moving backwards — the failure fencing exists to prevent. |
| **Rebalance** | Recomputing partition assignment after group membership changes. |
| **Segment** | An append-only file holding a contiguous range of a partition's records. |
| **Session timeout** | Time without a heartbeat after which the coordinator declares a member failed. |
| **Sparse index** | Index of some offsets to byte positions, used to locate reads within a segment. |
| **Sticky assignment** | Rebalance that keeps existing partition ownership where possible while staying balanced. |
| **Zombie consumer** | A consumer declared dead (e.g., after a long pause) that resumes and tries to act on stale ownership. |
