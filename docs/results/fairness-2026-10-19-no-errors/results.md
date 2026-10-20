## Fairness experiment

Command: `RUNS=5 BIG=3000 SMALL=100 SMALL_DELAY=5 SEED=42 ERROR_RATE=0 CRASH_RATE=0 scripts/fairness-experiment.sh`

Tenant A: one batch of 3000 requests. Tenants B, C, D: one batch of 100 requests each, submitted 5s after A's.
Fake model: 50–500 ms per request, error rate 0, cut-off-reply rate 0, seed 42. 16 workers, window of 32.
5 runs per mode, same seed and arrival timing; values are median [min–max] over the runs.
Every time is measured from that tenant's own submission (when its batch was created), in seconds.

| Metric | Fair scheduling | First come, first served |
|---|---|---|
| Small tenants: batch done, from its submission (slowest of B, C, D) | 7.67 [7.62–7.75] | 53.85 [53.84–54.04] |
| Small tenants: wait from submission until a request's model call starts, p50 | 3.73 [3.71–3.78] | 50.74 [50.69–50.8] |
| Small tenants: same wait, p99 | 7.14 [7.13–7.18] | 53.28 [53.26–53.32] |
| Small tenants: wait from submission until the first request gets a slot (slowest) | 0.04 [0.03–0.05] | 51.36 [51.25–51.43] |
| Big tenant: batch done, from its submission | 58.92 [58.92–59.14] | 53.57 [53.52–53.61] |
| Total throughput: all requests / (last batch done − first batch submitted), requests/s | 56 [55.8–56] | 56 [55.9–56] |
| Busy workers, average over the run (of 16) | 15.7 [15.6–15.7] | 15.7 [15.6–15.7] |
| Idle worker-seconds over the run | 20 [19–24] | 19 [18–21] |
| Busy workers, average over the last 10 s of the run | 15.3 [15.2–15.5] | 15.5 [15.4–15.6] |

Jobs retried per run (same seed, so this should match across runs): 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.

```
Median time from submission until done (seconds)
small, fair      7.7  #####
small, fifo     53.9  ####################################
big,   fair     58.9  ########################################
big,   fifo     53.6  ####################################
```
