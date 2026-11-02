#!/usr/bin/env bash
# E6 leader failover (LLD §16.2): 3 brokers, R = 3 with min ISR 2, 12 partitions, 4 consumers
# at 50 % load; producer acks wait for the commit. The broker leading partition 0 is
# SIGKILLed at 60 s. A new leader must come from the ISR, and no acknowledged record may be
# lost (FR-REPL, INV-03).
#
#     experiments/e6_leader_failover.sh [--repeat K] [--seed S]

source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
source "$ELOG_ROOT/experiments/lib/faults.sh"
parse_args "$@"

PARTITIONS=12
CONSUMERS="consumer-1 consumer-2 consumer-3 consumer-4"
WARMUP_S="${WARMUP_S:-30}"
FAULT_AT_S="${FAULT_AT_S:-60}"
POST_S="${POST_S:-60}"
export TOPIC=t GROUP=g RATE="$(rate_at 50 4)"
export CONSUMER_ARGS="--process-us $PROCESS_US"

# The id of the broker leading partition 0, from describe-topic.
leader_of_partition_0() {
    python3 -c 'import json, sys
print(next(p["leader"] for p in json.load(sys.stdin)["partitions"] if p["partition"] == 0))'
}

for i in $(seq 1 "$REPEAT"); do
    export PRODUCER_ARGS="--keys 1000 --seed $((SEED + i - 1))"
    new_run_dir e6
    write_run_json repeat="$i" rate="$RATE" partitions="$PARTITIONS" consumers=4 min_isr=2 \
        warmup_s="$WARMUP_S" fault_at_s="$FAULT_AT_S" post_s="$POST_S"
    cluster_up
    elogctl create-topic "$TOPIC" -p "$PARTITIONS" -r 3 --min-isr 2
    mark warmup
    start $CONSUMERS producer
    sleep "$WARMUP_S"
    mark measure
    sleep $((FAULT_AT_S - WARMUP_S))
    elogctl describe-topic "$TOPIC" > "$RUN_DIR/topic-before-kill.json"
    fault_kill "broker-$(leader_of_partition_0 < "$RUN_DIR/topic-before-kill.json")"
    sleep "$POST_S"
    mark drain
    stop_services producer
    wait_drained "$TOPIC" "$GROUP" "$PARTITIONS" 300
    stop_services $CONSUMERS
    elogctl describe-topic "$TOPIC" > "$RUN_DIR/topic-after.json"
    dump_log "$TOPIC" "$PARTITIONS"
    cluster_down
    plot plot_e6.py "$RUN_DIR"
done
