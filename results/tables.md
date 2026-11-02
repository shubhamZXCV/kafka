| Experiment | Check | Passed | Detail |
|---|---|---|---|
| E1 | no acked record lost | 3/3 | 0 partitions with losses · 0 partitions with losses · 0 partitions with losses |
| E1 | no skipped records (INV-05) | 3/3 | 0 partitions with gaps · 0 partitions with gaps · 0 partitions with gaps |
| E1 | order (INV-01) | 3/3 | 0 violations · 0 violations · 0 violations |
| E1 | stable throughput | 3/3 | CV 0.039 (limit 0.2) · CV 0.037 (limit 0.2) · CV 0.105 (limit 0.2) |
| E2 | no skipped records (INV-05) | 3/3 | 0 partitions with gaps · 0 partitions with gaps · 0 partitions with gaps |
| E2 | order within an epoch (INV-01) | 3/3 | 0 violations · 0 violations · 0 violations |
| E2 | rebalance within session + 500 ms (NFR-AVL-01) | 1/3 | 7590 ms with a 6000 ms session · 6133 ms with a 6000 ms session · 6704 ms with a 6000 ms session |
| E3 | near-linear (NFR-AVL-02) | 1/1 | R² 0.984 (≥ 0.95), slope 1.10, intercept 0.41 s |
| E3 | no false evictions from 3 s (NFR-AVL-03) | 1/1 | 0 at ≥ 3 s |
| E4 | sticky 40–70 % faster (SRS §9.1) | 0/1 | -32% faster: 833 ms vs 629 ms |
| E5 | fenced: 0 regressions (SRS §9.1) | 1/1 | 0 |
| E5 | fenced: every stale commit rejected | 1/1 | 6 of 6 rejected; 3 of 3 runs sent one |
| E5 | no skipped records (INV-05) | 1/1 | 0 partitions with gaps |
| E5 | unfenced control: regressions > 0 | 1/1 | 4 in total; 2 of 3 runs sent a stale commit |
| E6 | 0 committed records lost (INV-03) | 3/3 | 0 partitions with losses · 0 partitions with losses · 0 partitions with losses |
| E6 | new leader from the ISR | 3/3 | broker 1 killed (ISR [1, 2, 3]), new leader 2 · broker 1 killed (ISR [1, 2, 3]), new leader 2 · broker 1 killed (ISR [1, 2, 3]), new leader 2 |
| E6 | no skipped records (INV-05) | 3/3 | 0 partitions with gaps · 0 partitions with gaps · 0 partitions with gaps |
| E7 | RSS flat (FR-BP-02) | 1/1 | max growth 2.3 MB (limit 8 MB) |
| E7 | backlog grows on disk | 1/1 | 0 runs without |
| E7 | depth ≤ Q (FR-BP-01) | 1/1 | 0 runs over their Q |
| E8 | duplicates = k from the hook | 1/1 | 15532 measured, 15532 k |
| E8 | duplicates ≤ N (INV-10) | 1/1 | N=10: max 9, N=50: max 48, N=100: max 99, N=500: max 488 |
| E8 | linear in N | 1/1 | slope 0.479 |
| E8 | mean ≈ (N − 1) / 2 | 1/1 | N=10: 4.9, N=50: 22.6, N=100: 44.8, N=500: 238.4 |
| E8 | no skipped records (INV-05) | 1/1 | 0 partitions with gaps |
| E9 | drain ≈ capacity − producer rate (NFR-AVL-06) | 1/3 | 212 /s measured, 126 /s predicted (±25%) · 210 /s measured, 126 /s predicted (±25%) · 140 /s measured, 126 /s predicted (±25%) |

| Experiment | Value | Mean | SD | n |
|---|---|---|---|---|
| E1 | consume_rate_cv | 0.06051 | 0.0386 | 3 |
| E1 | consume_rate_mean | 882.2 | 0.642 | 3 |
| E1 | duplicates | 0 | 0 | 3 |
| E1 | e2e_p50_ms_median | 32.5 | 0.331 | 3 |
| E1 | e2e_p99_ms_median | 140.5 | 3.91 | 3 |
| E1 | produced_records | 8.067e+04 | 950 | 3 |
| E2 | barrier_ms | 1489 | 472 | 3 |
| E2 | detection_ms | 5258 | 445 | 3 |
| E2 | drain_rate | 286.8 | 7.62 | 3 |
| E2 | duplicates | 0 | 0 | 3 |
| E2 | false_evictions | 0 | 0 | 3 |
| E2 | rebalance_ms | 6809 | 734 | 3 |
| E2 | session_ms | 6000 | 0 | 3 |
| E2 | time_to_full_throughput_ms | 675 | 404 | 3 |
| E3 | intercept_s | 0.4104 |  | 1 |
| E3 | r2 | 0.9844 |  | 1 |
| E3 | runs | 14 |  | 1 |
| E3 | slope | 1.096 |  | 1 |
| E4 | faster_fraction | -0.3249 |  | 1 |
| E4 | roundrobin_duplicates | 0 |  | 1 |
| E4 | roundrobin_moved | 12 |  | 1 |
| E4 | roundrobin_ttft_ms | 629 |  | 1 |
| E4 | sticky_duplicates | 0 |  | 1 |
| E4 | sticky_moved | 3 |  | 1 |
| E4 | sticky_ttft_ms | 833.3 |  | 1 |
| E5 | fenced_duplicates | 83 |  | 1 |
| E5 | fenced_fenced_commits | 6 |  | 1 |
| E5 | fenced_regressions | 0 |  | 1 |
| E5 | fenced_skips | 0 |  | 1 |
| E5 | fenced_stale_commits | 6 |  | 1 |
| E5 | unfenced_duplicates | 46 |  | 1 |
| E5 | unfenced_fenced_commits | 0 |  | 1 |
| E5 | unfenced_regressions | 4 |  | 1 |
| E5 | unfenced_skips | 0 |  | 1 |
| E5 | unfenced_stale_commits | 4 |  | 1 |
| E6 | acked_records | 7.772e+04 | 1.05e+03 | 3 |
| E6 | duplicates | 0 | 0 | 3 |
| E6 | election_ms | 5516 | 447 | 3 |
| E6 | killed_broker | 1 | 0 | 3 |
| E6 | new_leader | 2 | 0 | 3 |
| E6 | produce_p99_max_ms | 1.471e+04 | 3.44e+03 | 3 |
| E6 | write_gap_ms | 1.422e+04 | 3.3e+03 | 3 |
| E7 | q10000_consume_rate | 1295 |  | 1 |
| E7 | q10000_max_depth | 9999 |  | 1 |
| E7 | q10000_rss_growth_mb | 1.951 |  | 1 |
| E7 | q10000_rss_mb | 9.391 |  | 1 |
| E7 | q1000_consume_rate | 1309 |  | 1 |
| E7 | q1000_max_depth | 1000 |  | 1 |
| E7 | q1000_rss_growth_mb | 0.08789 |  | 1 |
| E7 | q1000_rss_mb | 5.878 |  | 1 |
| E7 | q100_consume_rate | 1313 |  | 1 |
| E7 | q100_max_depth | 100 |  | 1 |
| E7 | q100_rss_growth_mb | 0.01562 |  | 1 |
| E7 | q100_rss_mb | 5.387 |  | 1 |
| E8 | intercept | -1.348 |  | 1 |
| E8 | n100_crashes | 50 |  | 1 |
| E8 | n100_max | 99 |  | 1 |
| E8 | n100_mean | 44.76 |  | 1 |
| E8 | n10_crashes | 50 |  | 1 |
| E8 | n10_max | 9 |  | 1 |
| E8 | n10_mean | 4.92 |  | 1 |
| E8 | n500_crashes | 50 |  | 1 |
| E8 | n500_max | 488 |  | 1 |
| E8 | n500_mean | 238.4 |  | 1 |
| E8 | n50_crashes | 50 |  | 1 |
| E8 | n50_max | 48 |  | 1 |
| E8 | n50_mean | 22.56 |  | 1 |
| E8 | slope | 0.4788 |  | 1 |
| E9 | barrier_ms | 1255 | 468 | 3 |
| E9 | capacity_per_consumer | 315 | 0 | 3 |
| E9 | detection_ms | 5288 | 391 | 3 |
| E9 | drain_rate | 187.1 | 41 | 3 |
| E9 | drain_rate_predicted | 126 | 0 | 3 |
| E9 | effective_capacity_per_consumer | 331.3 | 13.6 | 3 |
| E9 | false_evictions | 0 | 0 | 3 |
| E9 | rate | 819 | 0 | 3 |
| E9 | rebalance_ms | 6605 | 470 | 3 |
| E9 | survivors_rate_while_draining | 994 | 40.9 | 3 |
| E9 | time_to_full_throughput_ms | 628 | 606 | 3 |
