#!/usr/bin/env bash
# Measures the 4 consumers' capacity (records/s at --process-us PROCESS_US) that the
# experiments' loads are shares of (experiments/CALIBRATION.md). For each producer rate in
# RATES, a fresh 12-partition topic is preloaded with a backlog, then the consumers drain
# it while the producer adds RATE records/s; capacity = their consume rate meanwhile.
#
#     experiments/calibrate.sh          # results in runs/<UTC-ts>-calibration/

source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"

RATES="${RATES:-0 760 1140}"
BACKLOG=60000
MEASURE_S=40
CONSUMERS="consumer-1 consumer-2 consumer-3 consumer-4"
export CONSUMER_ARGS="--process-us $PROCESS_US"

new_run_dir calibration
write_run_json process_us="$PROCESS_US" rates="$RATES" backlog="$BACKLOG" measure_s="$MEASURE_S"
cluster_up
for rate in $RATES; do
    export TOPIC="t$rate" GROUP="g$rate"
    elogctl create-topic "$TOPIC" -p 12 -r 3 --min-isr 2
    export RATE=0 COUNT="$BACKLOG" PRODUCER_ARGS="--keys 1000"
    start producer
    timeout 120 docker wait "$(compose ps -a -q producer)" >/dev/null
    export RATE="$rate" COUNT=0
    [ "$rate" = 0 ] || start producer
    mark "consume-$rate"
    start $CONSUMERS
    sleep "$MEASURE_S"
    mark "stop-$rate"
    stop_services producer $CONSUMERS
done
cluster_down
python3 "$ELOG_ROOT/analysis/calibration.py" "$RUN_DIR"
