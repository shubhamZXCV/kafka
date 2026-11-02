#!/usr/bin/env bash
# E4 eager vs. sticky (LLD §16.2, R-25): the E2 scenario at 70 % load, once with the
# round-robin (eager) and once with the sticky assignor per repeat, same seed and workload.
# Sticky should reach full throughput 40–70 % sooner after the rebalance (SRS §9.1).
#
#     experiments/e4_eager_vs_sticky.sh [--repeat K] [--seed S]
#
# Every run of one invocation goes under runs/<UTC-ts>-e4/, which plot_e4.py summarises.

source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
source "$ELOG_ROOT/experiments/lib/faults.sh"
source "$ELOG_ROOT/experiments/lib/kill_consumer.sh"
parse_args "$@"

SWEEP_DIR="$ELOG_ROOT/runs/$(date -u +%Y%m%dT%H%M%SZ)-e4"
export RUNS_ROOT="$SWEEP_DIR" LOAD_PCT=70

for repeat in $(seq 1 "$REPEAT"); do
    for assignor in roundrobin sticky; do
        ASSIGNOR="$assignor" kill_consumer_run "e4-${assignor}-r${repeat}"
    done
done
plot plot_e4.py "$SWEEP_DIR"
