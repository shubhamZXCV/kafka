#!/usr/bin/env bash
# E9 backlog drain (LLD §16.2, R-25): the E2 scenario with the producer held at 65 % of the 4
# consumers' capacity. After consumer-2's partitions move, the backlog they built up should
# drain at about the 3 survivors' capacity minus the producer rate (NFR-AVL-06).
#
#     experiments/e9_backlog_drain.sh [--repeat K] [--seed S]

source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
source "$ELOG_ROOT/experiments/lib/faults.sh"
source "$ELOG_ROOT/experiments/lib/kill_consumer.sh"
parse_args "$@"

export LOAD_PCT=65 POST_S="${POST_S:-90}"
for _ in $(seq 1 "$REPEAT"); do
    kill_consumer_run e9
    plot plot_e9.py "$RUN_DIR"
done
