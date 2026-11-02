#!/usr/bin/env bash
# E2 consumer kill (LLD §16.2): 4 consumers at 50 % load, consumer-2 SIGKILLed at 60 s,
# session timeout 6 s. Rebalance time should be about the session timeout + < 500 ms, and
# no record is skipped (NFR-AVL-01, INV-05).
#
#     experiments/e2_consumer_kill.sh [--repeat K] [--seed S]

source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
source "$ELOG_ROOT/experiments/lib/faults.sh"
source "$ELOG_ROOT/experiments/lib/kill_consumer.sh"
parse_args "$@"

for _ in $(seq 1 "$REPEAT"); do
    kill_consumer_run e2
    plot plot_e2.py "$RUN_DIR"
done
