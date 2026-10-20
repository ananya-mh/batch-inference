## Fairness experiment

Command: `RUNS=5 BIG=3000 SMALL=100 SMALL_DELAY=5 SEED=42 ERROR_RATE=0.02 CRASH_RATE=0.01 scripts/fairness-experiment.sh`

Tenant A: one batch of 3000 requests. Tenants B, C, D: one batch of 100 requests each, submitted 5s after A's.
Fake model: 50–500 ms per request, error rate 0.02, cut-off-reply rate 0.01, seed 42. 16 workers, window of 32.
5 runs per mode, same seed and arrival timing; values are median [min–max] over the runs.
Every time is measured from that tenant's own submission (when its batch was created), in seconds.

| Metric | Fair scheduling | First come, first served |
|---|---|---|
| Small tenants: batch done, from its submission (slowest of B, C, D) | 7.97 [7.92–7.98] | 56.47 [55.86–56.9] |
| Small tenants: wait from submission until a request's model call starts, p50 | 3.95 [3.93–4] | 52.37 [52.33–52.41] |
| Small tenants: same wait, p99 | 7.4 [7.36–7.41] | 55.08 [55.02–55.09] |
| Small tenants: wait from submission until the first request gets a slot (slowest) | 0.07 [0.01–0.08] | 53 [52.93–53.11] |
| Big tenant: batch done, from its submission | 65.55 [65.36–66.15] | 59.91 [59.73–59.94] |
| Total throughput: all requests / (last batch done − first batch submitted), requests/s | 50.3 [49.9–50.5] | 53.7 [53.3–54.2] |
| Busy workers, average over the run (of 16) | 14.7 [14.5–14.8] | 15.7 [15.6–15.9] |
| Idle worker-seconds over the run | 87 [76–101] | 22 [8–24] |
| Busy workers, average over the last 10 s of the run | 7.7 [6.8–8.4] | 14.2 [13.7–15.3] |

Jobs retried per run (same seed, so this should match across runs): 92, 92, 92, 92, 92, 92, 92, 92, 92, 92.

```
Median time from submission until done (seconds)
small, fair      8.0  #####
small, fifo     56.5  ##################################
big,   fair     65.5  ########################################
big,   fifo     59.9  ####################################
```
