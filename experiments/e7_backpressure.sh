#!/usr/bin/env bash
# E7 overload backpressure (LLD §16.2): 3 brokers, 12 partitions, 4 consumers, the producer
# at 3x their capacity, for in-flight queue sizes Q of 100, 1 000 and 10 000. The queue
# never holds more than Q records and consumer memory stays flat; the backlog waits on the
# brokers' disks instead (FR-BP-01/02).
#
#     experiments/e7_backpressure.sh [--repeat K] [--seed S]
#
# QUEUES overrides the sweep. Every run of one invocation goes under runs/<UTC-ts>-e7/,
# which plot_e7.py summarises. The overload never drains, so consumers stop with the producer.

source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
parse_args "$@"

SWEEP_DIR="$ELOG_ROOT/runs/$(date -u +%Y%m%dT%H%M%SZ)-e7"
export RUNS_ROOT="$SWEEP_DIR"
PARTITIONS=12
CONSUMERS="consumer-1 consumer-2 consumer-3 consumer-4"
WARMUP_S="${WARMUP_S:-20}"
MEASURE_S="${MEASURE_S:-60}"
export TOPIC=t GROUP=g RATE="$(rate_at 300 4)"

for repeat in $(seq 1 "$REPEAT"); do
    for queue in ${QUEUES:-100 1000 10000}; do
        export CONSUMER_ARGS="--process-us $PROCESS_US --inflight $queue"
        export PRODUCER_ARGS="--keys 1000 --seed $((SEED + repeat - 1))"
        new_run_dir "e7-q${queue}-r${repeat}"
        write_run_json queue="$queue" rate="$RATE" load_pct=300 partitions="$PARTITIONS" \
            consumers=4 warmup_s="$WARMUP_S" measure_s="$MEASURE_S"
        cluster_up
        elogctl create-topic "$TOPIC" -p "$PARTITIONS" -r 3 --min-isr 2
        mark warmup
        start $CONSUMERS producer
        sleep "$WARMUP_S"
        mark measure
        sleep "$MEASURE_S"
        mark stop
        stop_services producer $CONSUMERS
        cluster_down
    done
done
plot plot_e7.py "$SWEEP_DIR"
