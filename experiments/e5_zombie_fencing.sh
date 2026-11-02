#!/usr/bin/env bash
# E5 zombie fencing (LLD §16.2), the headline demonstration: 3 brokers, 4 partitions, 2
# consumers at 50 % load. consumer-1 is the zombie: slow, and it commits after every record.
# It is frozen (SIGSTOP) at 30 s for twice the session timeout, so the group evicts it and
# consumer-2 takes its partitions over, then thawed (SIGCONT) with records still queued.
# With fencing on its stale commits are rejected: 0 offset regressions. The control run,
# fencing OFF, must show regressions, which proves the experiment can fail.
#
#     experiments/e5_zombie_fencing.sh [--repeat K] [--seed S]
#
# Every run of one invocation goes under runs/<UTC-ts>-e5/, which plot_e5.py summarises.

source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
source "$ELOG_ROOT/experiments/lib/faults.sh"
parse_args "$@"

SWEEP_DIR="$ELOG_ROOT/runs/$(date -u +%Y%m%dT%H%M%SZ)-e5"
export RUNS_ROOT="$SWEEP_DIR"
PARTITIONS=4
SESSION_MS=6000
FAULT_AT_S="${FAULT_AT_S:-30}"
POST_S="${POST_S:-30}"
export TOPIC=t GROUP=g RATE="$(rate_at 50 2)"
export CONSUMER_ARGS="--process-us $PROCESS_US --session-timeout-ms $SESSION_MS"
# The zombie commits often (every 50 records or 200 ms), so a commit follows soon after it
# wakes; after every record would leave it too slow to keep up once it rejoins.
export CONSUMER_1_ARGS="--commit-interval-records 50 --commit-interval-ms 200"

run() {  # run FENCING(true|false)
    local label; label=$([ "$1" = true ] && echo fenced || echo unfenced)
    export COORDINATOR_ARGS="--set coordinator.fencing.enabled=$1"
    export PRODUCER_ARGS="--keys 1000 --seed $SEED"
    new_run_dir "e5-$label-r$2"
    write_run_json fencing="$1" rate="$RATE" partitions="$PARTITIONS" consumers=2 \
        session_ms="$SESSION_MS" zombie=consumer-1 pause_s=$((2 * SESSION_MS / 1000))
    cluster_up
    elogctl create-topic "$TOPIC" -p "$PARTITIONS" -r 3 --min-isr 2
    mark warmup
    start consumer-1 consumer-2 producer
    sleep "$FAULT_AT_S"
    fault_pause consumer-1
    sleep $((2 * SESSION_MS / 1000))
    fault_resume consumer-1
    sleep "$POST_S"
    mark drain
    stop_services producer
    wait_drained "$TOPIC" "$GROUP" "$PARTITIONS" 300
    stop_services consumer-1 consumer-2
    cluster_down
}

for repeat in $(seq 1 "$REPEAT"); do
    run true "$repeat"
    run false "$repeat"
done
plot plot_e5.py "$SWEEP_DIR"
