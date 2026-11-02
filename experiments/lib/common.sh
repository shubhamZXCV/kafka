# Shared by every experiments/eN_*.sh (LLD §16.1). Source it, then:
#
#     parse_args "$@"                  # --repeat K (default 5), --seed S (default 1)
#     new_run_dir e1                   # runs/<UTC-ts>-e1/, exported as RUN_DIR
#     write_run_json key=value ...     # run.json: git SHA, seed, parameters, host
#     cluster_up                       # coordinator + 3 brokers, waits for elogctl health
#     elogctl create-topic t -p 12 -r 3
#     cluster_down                     # collects logs, then docker compose down -v
#
# The containers read COORDINATOR_ARGS, BROKER_ARGS, PRODUCER_ARGS, CONSUMER_ARGS, TOPIC,
# GROUP, RATE and COUNT (see deploy/compose.yaml); export them before cluster_up.

set -euo pipefail

ELOG_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export ELOG_UID="$(id -u)" ELOG_GID="$(id -g)"
REPEAT=5
SEED=1

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

# Starts the coordinator and the brokers and waits until all three are registered.
cluster_up() {
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

# Seconds since the epoch, in ms, as every CSV uses.
now_ms() {
    date +%s%3N
}

# Saves every container's log into the run directory, then removes containers and volumes.
cluster_down() {
    for service in $(compose --profile workload ps -a --services 2>/dev/null); do
        compose logs --no-color "$service" > "$RUN_DIR/$service.log" 2>&1 || true
    done
    compose --profile workload --profile toxiproxy down -v --remove-orphans >/dev/null 2>&1
}
