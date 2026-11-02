#!/usr/bin/env bash
# E3 session-timeout sweep (LLD §16.2): the E2 scenario at session timeouts of 2, 3, 4, 6,
# 8, 10 and 15 s. Rebalance time should grow near-linearly with the session timeout
# (NFR-AVL-02); false evictions are counted, expected only below ~2 s (NFR-AVL-03).
#
#     experiments/e3_session_sweep.sh [--repeat K] [--seed S]
#
# SESSIONS overrides the sweep (seconds, space-separated). Every run of one invocation goes
# under runs/<UTC-ts>-e3/, which plot_e3.py summarises.

source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
source "$ELOG_ROOT/experiments/lib/faults.sh"
source "$ELOG_ROOT/experiments/lib/kill_consumer.sh"
parse_args "$@"

SWEEP_DIR="${RUNS_ROOT:-$ELOG_ROOT/runs}/$(date -u +%Y%m%dT%H%M%SZ)-e3"
export RUNS_ROOT="$SWEEP_DIR"
export WARMUP_S="${WARMUP_S:-20}" FAULT_AT_S="${FAULT_AT_S:-40}"

for repeat in $(seq 1 "$REPEAT"); do
    for session_s in ${SESSIONS:-2 3 4 6 8 10 15}; do
        # Long enough after the kill for the eviction, the rebalance and the drain.
        SESSION_MS=$((session_s * 1000)) POST_S=$((session_s + 25)) \
            kill_consumer_run "e3-s${session_s}-r${repeat}"
    done
done
plot plot_e3.py "$SWEEP_DIR"
