#!/usr/bin/env bash
# The experiment campaign (implementation plan T8.2): E1–E9 in order, with the repeat counts
# below (cut line 3: 3 repeats instead of 5), then analysis/summarize.py, which writes
# results/ from this campaign's runs. About two hours on the reference host.
#
#     experiments/campaign.sh [--seed S]

source "$(dirname "${BASH_SOURCE[0]}")/lib/common.sh"
parse_args "$@"

START="$(date -u +%Y%m%dT%H%M%SZ)"
make -C "$ELOG_ROOT" images
for spec in "e1_steady_state 3" "e2_consumer_kill 3" "e3_session_sweep 2" \
            "e4_eager_vs_sticky 3" "e5_zombie_fencing 3" "e6_leader_failover 3" \
            "e7_backpressure 2" "e8_crash_duplicates 1" "e9_backlog_drain 3"; do
    read -r script repeats <<<"$spec"
    echo "== $script ($repeats repeats) $(date -u +%H:%M:%S)"
    "$ELOG_ROOT/experiments/$script.sh" --repeat "$repeats" --seed "$SEED" ||
        echo "$script did not finish" >&2
done
python3 "$ELOG_ROOT/analysis/summarize.py" "$ELOG_ROOT/runs" "$START" "$ELOG_ROOT/results"
