#!/usr/bin/env bash
# E8 crash duplicates (LLD §16.2), a headline demonstration: 3 brokers, 4 partitions, one
# consumer at 30 % load with the time-based commit trigger off. It crashes after a random
# k ~ Uniform[0, N) records past its last commit (--crash-after-process=random), is
# restarted, and that repeats CRASHES times (50) for each commit interval N in {10, 50, 100,
# 500}. Duplicates per crash must never exceed N, average about N/2, and grow linearly in N
# (FR-DEL-03, INV-10); nothing is skipped (INV-05).
#
#     experiments/e8_crash_duplicates.sh [--repeat K] [--seed S]
#
# --repeat repeats the whole sweep (default 1: the 50 crashes are the repetition).
# INTERVALS and CRASHES override the sweep. Runs go under runs/<UTC-ts>-e8/.

source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
REPEAT=1
parse_args "$@"

SWEEP_DIR="$ELOG_ROOT/runs/$(date -u +%Y%m%dT%H%M%SZ)-e8"
export RUNS_ROOT="$SWEEP_DIR"
PARTITIONS=4
CRASHES="${CRASHES:-50}"
export TOPIC=t GROUP=g RATE="$(rate_at 30 1)"
# A short session, so each restarted consumer is not kept waiting for its dead predecessor.
COMMON_ARGS="--process-us $PROCESS_US --session-timeout-ms 2000 --heartbeat-interval-ms 600"

# Runs consumer-1 with $1 as its extra options until it exits (at most $2 s), then keeps
# its log: compose replaces the container on the next start.
run_consumer_once() {
    export CONSUMER_ARGS="$COMMON_ARGS $1"
    start consumer-1
    timeout "$2" docker wait "$(compose ps -a -q consumer-1)" >/dev/null || true
    compose logs --no-color consumer-1 >> "$RUN_DIR/crashes.log" 2>&1
}

for repeat in $(seq 1 "$REPEAT"); do
    for interval in ${INTERVALS:-10 50 100 500}; do
        export PRODUCER_ARGS="--keys 1000 --seed $((SEED + repeat - 1))"
        new_run_dir "e8-n${interval}-r${repeat}"
        write_run_json commit_interval="$interval" crashes="$CRASHES" rate="$RATE" \
            partitions="$PARTITIONS" consumers=1 load_pct=30
        cluster_up
        elogctl create-topic "$TOPIC" -p "$PARTITIONS" -r 3 --min-isr 2
        mark warmup
        start producer
        for crash in $(seq 1 "$CRASHES"); do
            run_consumer_once "--commit-interval-records $interval --commit-interval-ms -1 \
--crash-after-process random --seed $((SEED * 100000 + interval * 100 + crash))" 120
        done
        mark drain
        stop_services producer
        export CONSUMER_ARGS="$COMMON_ARGS --commit-interval-records $interval"
        start consumer-1
        wait_drained "$TOPIC" "$GROUP" "$PARTITIONS" 300
        stop_services consumer-1
        compose logs --no-color consumer-1 >> "$RUN_DIR/crashes.log" 2>&1
        cluster_down
    done
done
plot plot_e8.py "$SWEEP_DIR"
