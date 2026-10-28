# CLAUDE.md — Agent Guide

Kafka-inspired distributed event log in C++20: a partitioned, replicated append-only log,
a coordinator for consumer groups, and dynamic rebalancing with fencing. Course project,
one developer, code written mostly by coding agents. **Working code that is easy to read
matters more than anything else.**

## The Spec

| Document | Use it for |
|---|---|
| `docs/low-level-design.md` (LLD) | **The specification.** Names, byte formats, pseudocode, SQL, config keys, lock order |
| `docs/commit-plan.md` | The exact commits to produce, in order, and the message format |
| `docs/implementation-plan.md` | Tasks, milestones, code-quality guidelines (§4) |
| `docs/high-level-design.md` | Architecture and the ADRs behind the design |
| `docs/requirements.md` | FR / NFR / INV IDs that commits and comments cite |

- Implement what the LLD says, using its names. Do not invent behaviour it does not describe.
- **If the LLD is wrong or incomplete, stop and report.** The fix goes into LLD §17
  (Refinements) in the same commit as, or before, the code that needs it.

## Commands

```bash
cmake --preset dev && cmake --build --preset dev        # Debug + ASan + UBSan (default)
cmake --preset release && cmake --build --preset release  # RelWithDebInfo, for experiments
cmake --preset tsan && cmake --build --preset tsan      # ThreadSanitizer, before submission

pytest tests/functional -m "not slow"                   # default functional suite
pytest tests/functional                                  # includes slow / repeated scenarios
ELOG_BUILD_DIR=build/tsan pytest tests/functional        # run the suite against another build

clang-format -i <files>                                  # style is in .clang-format
clang-format --dry-run --Werror <files>                  # check only
```

Build output goes to `build/<preset>/`; every executable lands in `build/<preset>/bin/`.

## Working on a Commit

Commits come from `docs/commit-plan.md`. For each row:

1. Implement **only** what the row describes. Do not anticipate later rows.
2. Build with the `dev` preset with no new warnings, and run the functional tests that exist so far.
3. Run the row's "See it work" check.
4. Run `clang-format` on the files you touched.
5. Commit with the format in commit-plan §3 (`What / Why / Read / Spec / Check`), using the
   row's summary line plus the task suffix, e.g. `feat(common): add BoundedQueue (T0.3)`.

Rules (commit-plan §2): one idea per commit; every commit builds; a feature lands with
the functional test that shows it working (or in the next commit of the same task);
no mixing renames or reformatting with behaviour changes; aim for ≤ 300 changed lines,
hard cap 500; ⚠ commits ≤ 200 lines.

If a row is too big or cannot be done without a later row, **stop and propose a split
or reorder** instead of improvising. Merge with rebase or fast-forward, never squash.

**No unit tests.** Correctness is shown by functional tests in `tests/functional/`
(LLD §15), which start the real binaries and check behaviour from outside.

## Code Quality

| Area | Rule |
|---|---|
| Names | Use the LLD's names for classes, methods and config keys. Names say what something *is*: `highWatermark`, not `hw2` or `tmp`. |
| Size | Functions fit on one screen (about 40 lines). Files under about 400 lines. One main class per header. |
| Structure | One responsibility per class. Handlers go parse → validate → act → reply, in that order and visibly. |
| Comments | Explain *why*, not *what*. Cite the source of every non-obvious rule, e.g. `// LLD §6.4: HW over ISR ∪ pending (ADR-13)`. |
| Errors | Return `Result<T>` / `Status` with an `ErrorCode`. No exceptions across module boundaries. Never swallow an error; log it or return it. |
| Concurrency | Each class states its thread rules in a header comment. Follow the lock order below. No network I/O under a partition lock. |
| Simplicity | No speculative abstractions, template tricks or macros where a plain function works. Duplicate twice before abstracting. |
| Logging | One structured log line per significant event (`key=value`). No logging in per-record hot loops. |
| Formatting | `clang-format` on every commit. |
| Dependencies | Only the libraries in LLD §2.3 (already in `cmake/Dependencies.cmake`). Do not add others. |

## Lock Order (LLD §11.2)

```
ReplicaManager::mapMu_  →  Replica::mu_  →  PartitionLog::segMu_
                                         →  HwNotifier::mu_
```

- Never acquire a lock to the **left** of one already held.
- Never hold two `Replica::mu_` at once.
- Never make a network call while holding `Replica::mu_` (send `AlterIsr` after releasing it).
- Put a short comment at each lock site naming its position in this order.
- Coordinator state (`GroupManager`, `OffsetManager`, `MetadataManager`, `BrokerLiveness`,
  `Repository`) is touched **only** on the coordinator event loop thread (LLD §11.3).
- Timeouts use the monotonic clock only.

## Source Layout (LLD §2)

```
src/<module>/include/elog/<module>/*.h   public headers, included as "elog/<module>/x.h"
src/<module>/*.cc                        implementation
```

Allowed module dependencies (also listed in `src/CMakeLists.txt`):

```
common      → (none)              coordinator → protocol
protocol    → common              client      → protocol
storage     → common, protocol    tools       → client, storage
broker      → storage, protocol
```

- A module's `CMakeLists.txt` and its `add_subdirectory` line are added in the commit that
  adds its first source file (CMake rejects empty libraries).
- Call `elog_set_warnings(<target>)` on every target you add.
- Includes are grouped by `.clang-format`: C system, C++ standard, third-party, then `"elog/..."`.
