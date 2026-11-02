#!/usr/bin/env bash
# Live demonstration of the two headline results (SRS D6), in under 10 minutes:
#   1. E5 fencing: a frozen consumer wakes up and tries to commit with a stale epoch.
#   2. E8 at-least-once: a consumer crashes between processing and committing.
# Each part runs a shortened experiment, then shows its figures and checks.
#
#     experiments/demo.sh            # run from the repository root; needs Docker

source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"

say() {
    printf '\n\033[1m== %s\033[0m\n' "$*"
}

pause() {
    read -r -p "   [press Enter to continue] " _ </dev/tty 2>/dev/null || true
}

latest() {
    ls -d "$ELOG_ROOT"/runs/*-"$1" | tail -1
}

say "Building the images (cached after the first time)"
make -C "$ELOG_ROOT" images >/dev/null

say "Part 1 · Fencing (E5)"
cat <<'TEXT'
   Two consumers share 4 partitions. consumer-1 is frozen with SIGSTOP for twice its 6 s
   session timeout, so the group evicts it and consumer-2 takes over its partitions under a
   new ownership epoch. Then consumer-1 is thawed. Usually it commits the records it
   processed before the freeze, with its old epoch: a stale commit. (If its heartbeat
   reports the eviction first, it drops them and rejoins without committing, and that run
   tests nothing, so each arm runs twice.) Arms: fencing on, and fencing OFF as the control.
TEXT
pause
FAULT_AT_S=20 POST_S=15 "$ELOG_ROOT/experiments/e5_zombie_fencing.sh" --repeat 2 2>&1 |
    grep -E "PASS|FAIL"
E5="$(latest e5)"
echo "   Figures: $E5/e5_commits.png (committed offsets around the pause, both runs)"
echo "            $E5/e5_regressions.png"
column -s, -t "$E5/e5_runs.csv"
pause

say "Part 2 · At-least-once under a crash (E8)"
cat <<'TEXT'
   One consumer commits every N records. It is made to crash after a random k < N records
   past its last commit, and restarted; the next consumer redoes exactly those k records.
   10 crashes each for N = 10 and N = 100: duplicates never exceed N and average about N/2.
TEXT
pause
INTERVALS="10 100" CRASHES=10 "$ELOG_ROOT/experiments/e8_crash_duplicates.sh" 2>&1 |
    grep -E "PASS|FAIL"
E8="$(latest e8)"
echo "   Figures: $E8/e8_histograms.png, $E8/e8_mean.png"

say "Done. Full experiments: make e1 … make e9"
