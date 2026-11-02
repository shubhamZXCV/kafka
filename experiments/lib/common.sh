# Shared by every experiments/eN_*.sh (LLD §16.1). Source it, then:
#
#     parse_args "$@"                  # --repeat K (default 5), --seed S (default 1)
#     new_run_dir e1                   # runs/<UTC-ts>-e1/, exported as RUN_DIR
#     write_run_json key=value ...     # run.json: git SHA, seed, parameters, host
#     cluster_up                       # coordinator + 3 brokers, waits for elogctl health
#     elogctl create-topic t -p 12 -r 3
#     start producer consumer-1        # workload services; mark measure: phases.csv
#     stop_services producer           # SIGTERM: final commits, files flushed
#     wait_drained t g 12              # every partition committed up to its HW
#     dump_log t 12                    # log-t-<p>.jsonl, for the order and loss checks
#     cluster_down                     # collects logs, then docker compose down -v
#
# The containers read COORDINATOR_ARGS, BROKER_ARGS, PRODUCER_ARGS, CONSUMER_ARGS, TOPIC,
# GROUP, RATE and COUNT (see deploy/compose.yaml); export them before cluster_up.

set -euo pipefail

ELOG_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export ELOG_UID="$(id -u)" ELOG_GID="$(id -g)"
REPEAT=5
SEED=1

# Consumer capacity model (R-22, E7's "callback sleep"): each record costs PROCESS_US in the
# callback, which makes a consumer process about CONSUMER_CAPACITY records/s on the
# reference host (experiments/CALIBRATION.md). Workloads are a share of that capacity.
PROCESS_US=2000
CONSUMER_CAPACITY=380

# rate_at PERCENT CONSUMERS: the producer rate for PERCENT % of CONSUMERS' capacity.
rate_at() {
    echo $(($1 * $2 * CONSUMER_CAPACITY / 100))
}

parse_args() {
    while [ $# -gt 0 ]; do
        case "$1" in
            --repeat) REPEAT="$2"; shift 2 ;;
            --seed) SEED="$2"; shift 2 ;;
            *) echo "unknown argument: $1 (expected --repeat K, --seed S)" >&2; exit 2 ;;
        esac
    done
}

compose() {
    docker compose -f "$ELOG_ROOT/deploy/compose.yaml" -p elog "$@"
}

# runs/<UTC timestamp>-<name>/; the timestamp keeps repeated runs apart and sorted.
new_run_dir() {
    RUN_DIR="$ELOG_ROOT/runs/$(date -u +%Y%m%dT%H%M%SZ)-$1"
    mkdir -p "$RUN_DIR"
    export RUN_DIR
    echo "run directory: $RUN_DIR"
}

write_run_json() {
    python3 "$ELOG_ROOT/experiments/lib/run_meta.py" "$RUN_DIR" "seed=$SEED" "$@"
}

elogctl() {
    compose exec -T coordinator elogctl --coordinator coordinator:9000 "$@"
}

# Starts the coordinator and the brokers and waits until all three are registered. If the
# script then fails, cluster_down still runs (EXIT trap), so no container is left behind.
cluster_up() {
    trap cluster_down EXIT
    compose up -d coordinator broker-1 broker-2 broker-3
    for _ in $(seq 1 60); do
        if elogctl health >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "cluster not healthy after 60 s" >&2
    elogctl health >&2 || true
    return 1
}

# Starts workload services (producer, consumer-1 ...).
start() {
    compose --profile workload up -d --no-deps "$@"
}

# SIGTERM, so consumers make their final commit and flush their CSVs.
stop_services() {
    compose stop -t 20 "$@" >/dev/null 2>&1
}

# phases.csv: when a phase (warm-up, measure, ...) began, for the plots.
mark() {
    local file="$RUN_DIR/phases.csv"
    [ -f "$file" ] || echo "ts_ms,phase" > "$file"
    echo "$(now_ms),$1" >> "$file"
}

# Waits up to $4 s (default 120) until group $2 has committed every one of the $3
# partitions of topic $1 up to its high watermark.
wait_drained() {
    local deadline=$((SECONDS + ${4:-120}))
    while [ $SECONDS -lt $deadline ]; do
        if python3 - "$3" 3<<<"$(elogctl describe-topic "$1")" 4<<<"$(elogctl describe-group "$2")" <<'PY'
import json, os, sys
hw = {p["partition"]: p["highWatermark"] for p in json.load(os.fdopen(3))["partitions"]}
committed = {p["partition"]: p["committedOffset"] for p in json.load(os.fdopen(4))["partitions"]}
sys.exit(0 if len(hw) == int(sys.argv[1]) and all(committed.get(p) == o for p, o in hw.items())
         else 1)
PY
        then
            return 0
        fi
        sleep 2
    done
    echo "group $2 did not drain $1 within ${4:-120} s" >&2
    return 1
}

# Saves the committed log of each partition of $1 (0 .. $2-1) as log-<topic>-<p>.jsonl.
dump_log() {
    for p in $(seq 0 $(($2 - 1))); do
        elogctl dump "$1" "$p" > "$RUN_DIR/log-$1-$p.jsonl"
    done
}

# Seconds since the epoch, in ms, as every CSV uses.
now_ms() {
    date +%s%3N
}

# Saves every container's log into the run directory, then removes containers and volumes.
cluster_down() {
    trap - EXIT
    for service in $(compose --profile workload ps -a --services 2>/dev/null); do
        compose logs --no-color "$service" > "$RUN_DIR/$service.log" 2>&1 || true
    done
    compose --profile workload --profile toxiproxy down -v --remove-orphans >/dev/null 2>&1
}
