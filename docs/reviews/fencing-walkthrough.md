# Fencing Walkthrough

> **Draft written by the coding agent from the code as of M4.** The commit plan (C076)
> asks for *your own* trace of this path. Read it next to the code, then rewrite it in your
> own words: that is the point of this document.

## The question

A consumer is paused (GC pause, `SIGSTOP`, a stalled VM) for longer than its session
timeout. The coordinator declares it dead and gives its partitions to another member, who
processes further and commits higher offsets. Then the paused consumer — the *zombie* —
wakes up, finishes the record it was processing and commits its old, lower position. What
stops that commit from moving the group's committed offset backwards?

**Answer: one SQL statement, which writes the offset only if the committer is the
partition's current owner at exactly the ownership epoch it sends.**

## 1. Where the epoch comes from

Every partition owned by a group has an ownership row `(owner_member, owner_epoch)` in
`partition_ownership`.

- `GroupManager::ownershipDiff` (`src/coordinator/group_manager.cc:341`) decides, at each
  rebalance, which partitions get a new epoch: every partition that changes owner (and every
  partition, under the eager round-robin assignor) gets `epoch = new generation`.
- `GroupManager::completeRebalance` (`group_manager.cc:299`) saves the new generation and the
  whole diff with `Repository::saveGeneration` (`src/coordinator/repository.cc:215`) in **one
  transaction, before any JoinGroup reply is sent** (P4). A consumer therefore never acts on
  an epoch the coordinator could forget.
- The JoinGroup reply carries each assigned partition with its epoch; the consumer keeps it
  in `AssignmentState` and sends it with every commit.

Removing a member does **not** touch ownership rows (`GroupManager::removeMember`,
`group_manager.cc:173`). So between "the zombie was evicted" and "its partitions were
reassigned" the zombie still matches its rows — but nothing else can have committed for
those partitions in that window either, so a commit then is harmless. The moment the
partitions are reassigned, their epoch changes and the zombie no longer matches.

## 2. The path of one commit

```
consumer (processing thread)
  Consumer::doCommit                     src/client/consumer.cc:158
  OffsetCommitter::commit                src/client/offset_committer.cc:33
      OffsetCommit { group, member, [(tp, offset, epoch)] }  ──TCP──►
coordinator
  CoordinatorServer::handle              src/coordinator/coordinator_server.cc:68
      (connection thread: queue the request, wait for the reply — never touches state)
  EventLoop::dispatch → OffsetCommit     src/coordinator/event_loop.cc:126
  OffsetManager::onCommit                src/coordinator/offset_manager.cc:14
      one transaction for the whole request
  OffsetManager::commitEntry             offset_manager.cc:43
  Repository::commitOffsetFenced         src/coordinator/repository.cc:260
      INSERT INTO committed_offset (…)
      SELECT :g, :t, :p, :offset, :epoch, :now FROM partition_ownership AS o
      WHERE o.group_id = :g AND o.topic = :t AND o.partition = :p
        AND o.owner_member = :member AND o.owner_epoch = :epoch
      ON CONFLICT (group_id, topic, partition) DO UPDATE SET …
      changes() == 1 ⇒ accepted          repository.cc:285
      changes() == 0 ⇒ STALE_EPOCH       offset_manager.cc:60  (+ commit_fenced event)
  ◄── OffsetCommitResponse { per partition: NONE | STALE_EPOCH }
consumer
  STALE_EPOCH ⇒ membership_->fenced      src/client/consumer.cc:177
  Consumer::onFenced                     consumer.cc:142
      drop every buffered record, revoke everything, reset member ID, join as new member
```

## 3. Why this is enough

- **The check and the write are one statement.** The `SELECT … WHERE owner_member AND
  owner_epoch` produces a row only for the current owner at the current epoch; with no row
  there is nothing to insert or update. No interleaving of requests can make SQLite store a
  stale commit, even if the coordinator's single-thread rule were broken later.
- **There is no in-memory pre-check.** `OffsetManager` deliberately does not ask
  `GroupManager` whether the member still exists. Fencing lives in exactly one place, so it
  cannot disagree with itself, and a zombie sees `STALE_EPOCH` (what E5 measures), not
  `UNKNOWN_MEMBER`.
- **Epochs never go backwards**, even across coordinator restarts: groups are restored with
  their generation (`GroupManager::load`), so the next rebalance produces generation + 1.

## 4. What the tests show

- **F-FEN-1** (`tests/functional/test_fencing.py`): the zombie's commit produces a
  `commit_fenced` event, the accepted commits never decrease (0 regressions), and the zombie
  rejoins under a new member ID; its log says `fenced reason=stale_epoch`.
- **F-FEN-2**: the same scenario with `coordinator.fencing.enabled=false` regresses an
  offset — proof that F-FEN-1 is not passing by accident.
