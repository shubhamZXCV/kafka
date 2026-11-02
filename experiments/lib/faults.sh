# Fault injection for the experiments (LLD §14), sourced after common.sh. Every action is
# appended to $RUN_DIR/faults.csv *before* it runs, so the analysis measures from the
# moment the fault was issued. With DRY_RUN=1 actions are only logged.
#
#     fault_kill consumer-2           # SIGKILL (crash)
#     fault_pause consumer-1          # SIGSTOP (freeze) ... fault_resume consumer-1 (SIGCONT)
#     fault_restart consumer-2        # start a killed service again, same command
#     fault_netem broker-1 delay 5ms 1ms loss 0.1%     # tc netem on its eth0
#     fault_netem_clear broker-1

DRY_RUN="${DRY_RUN:-0}"

# faults.csv: ts_ms,action,target,params (LLD §13.1).
fault_log() {
    local file="$RUN_DIR/faults.csv"
    [ -f "$file" ] || echo "ts_ms,action,target,params" > "$file"
    echo "$(now_ms),$1,$2,${3:-}" >> "$file"
}

fault_signal() {  # action, signal, service
    fault_log "$1" "$3" "$2"
    [ "$DRY_RUN" = 1 ] || compose kill -s "$2" "$3" >/dev/null
}

fault_kill() { fault_signal kill SIGKILL "$1"; }
fault_pause() { fault_signal pause SIGSTOP "$1"; }
fault_resume() { fault_signal resume SIGCONT "$1"; }

fault_restart() {
    fault_log restart "$1"
    [ "$DRY_RUN" = 1 ] || compose --profile workload start "$1" >/dev/null
}

# tc runs as root: the containers' own user does not hold NET_ADMIN. If tc is unavailable
# (no NET_ADMIN), only a delay can be emulated, by a Toxiproxy latency toxic on a proxy
# named after the target; the experiment must route that traffic through Toxiproxy.
fault_netem() {
    local target="$1"; shift
    fault_log netem "$target" "$*"
    [ "$DRY_RUN" = 1 ] && return 0
    if ! compose exec -T -u 0 "$target" tc qdisc replace dev eth0 root netem "$@"; then
        echo "tc netem failed on $target; falling back to Toxiproxy (delay only)" >&2
        fault_log toxiproxy_latency "$target" "$2"
        compose exec -T toxiproxy /toxiproxy-cli toxic add -t latency \
            -a "latency=${2%ms}" "$target"
    fi
}

fault_netem_clear() {
    fault_log netem_clear "$1"
    [ "$DRY_RUN" = 1 ] || compose exec -T -u 0 "$1" tc qdisc del dev eth0 root 2>/dev/null || true
}
