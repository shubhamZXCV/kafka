#!/usr/bin/env bash
# E1 steady state (LLD §16.2): 3 brokers, R = 3 with min ISR 2, 12 partitions, 4 consumers,
# the producer at 70 % of the consumers' measured capacity (experiments/CALIBRATION.md),
# no faults.
# Shows stable throughput, and that per-partition order is never violated (INV-01).
#
#     experiments/e1_steady_state.sh [--repeat K] [--seed S]
#
# WARMUP_S (30), MEASURE_S (60) and RATE override the defaults.

source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
parse_args "$@"

PARTITIONS=12
CONSUMERS="consumer-1 consumer-2 consumer-3 consumer-4"
WARMUP_S="${WARMUP_S:-30}"
MEASURE_S="${MEASURE_S:-60}"
export TOPIC=t GROUP=g RATE="${RATE:-$(rate_at 70 4)}"
export CONSUMER_ARGS="--process-us $PROCESS_US"

for i in $(seq 1 "$REPEAT"); do
    new_run_dir e1
    export PRODUCER_ARGS="--keys 1000 --seed $((SEED + i - 1))"
    write_run_json repeat="$i" rate="$RATE" partitions="$PARTITIONS" consumers=4 \
        warmup_s="$WARMUP_S" measure_s="$MEASURE_S"
    cluster_up
    elogctl create-topic "$TOPIC" -p "$PARTITIONS" -r 3 --min-isr 2
    mark warmup
    start $CONSUMERS producer
    sleep "$WARMUP_S"
    mark measure
    sleep "$MEASURE_S"
    mark drain
    stop_services producer
    wait_drained "$TOPIC" "$GROUP" "$PARTITIONS"
    stop_services $CONSUMERS
    dump_log "$TOPIC" "$PARTITIONS"
    cluster_down
    python3 "$ELOG_ROOT/analysis/plot_e1.py" "$RUN_DIR"
done
