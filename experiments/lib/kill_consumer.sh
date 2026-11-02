# The "kill one consumer" scenario shared by E2, E3, E4 and E9 (LLD §16.2): 3 brokers, a
# 12-partition R = 3 topic, consumer-1..4 and a producer at LOAD_PCT % of their capacity;
# consumer-2 is SIGKILLed FAULT_AT_S seconds after the start and the run continues for
# POST_S seconds, then the producer stops and the group drains. Source after common.sh
# and faults.sh, then:
#
#     kill_consumer_run NAME
#
# Settings (defaults): LOAD_PCT (50), SESSION_MS (6000; heartbeat a third of it),
# ASSIGNOR (sticky), WARMUP_S (30), FAULT_AT_S (60), POST_S (60).

kill_consumer_run() {
    local name="$1" consumers="consumer-1 consumer-2 consumer-3 consumer-4" partitions=12
    local load="${LOAD_PCT:-50}" session="${SESSION_MS:-6000}" assignor="${ASSIGNOR:-sticky}"
    local warmup="${WARMUP_S:-30}" fault_at="${FAULT_AT_S:-60}" post="${POST_S:-60}"
    export TOPIC=t GROUP=g RATE="$(rate_at "$load" 4)"
    export COORDINATOR_ARGS="--set group.default.assignor=$assignor"
    local heartbeat=$((session / 3))
    export CONSUMER_ARGS="--process-us $PROCESS_US --session-timeout-ms $session --heartbeat-interval-ms $heartbeat"
    export PRODUCER_ARGS="--keys 1000 --seed $SEED"
    new_run_dir "$name"
    write_run_json load_pct="$load" rate="$RATE" session_ms="$session" assignor="$assignor" \
        partitions="$partitions" consumers=4 killed=consumer-2 warmup_s="$warmup" \
        fault_at_s="$fault_at" post_s="$post"
    cluster_up
    elogctl create-topic "$TOPIC" -p "$partitions" -r 3 --min-isr 2
    mark warmup
    start $consumers producer
    sleep "$warmup"
    mark measure
    sleep $((fault_at - warmup))
    fault_kill consumer-2
    sleep "$post"
    mark drain
    stop_services producer
    wait_drained "$TOPIC" "$GROUP" "$partitions" 300
    stop_services $consumers
    cluster_down
}
