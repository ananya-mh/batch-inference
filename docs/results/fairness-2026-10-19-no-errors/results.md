## Fairness experiment

Command: `RUNS=5 BIG=3000 SMALL=100 SMALL_DELAY=5 SEED=42 ERROR_RATE=0 CRASH_RATE=0 scripts/fairness-experiment.sh`

Tenant A: one batch of 3000 requests. Tenants B, C, D: one batch of 100 requests each, submitted 5s after A's.
Fake model: 50–500 ms per request, error rate 0, cut-off-reply rate 0, seed 42. 16 workers, window of 32.
5 runs per mode, same seed and arrival timing; values are median [min–max] over the runs.
Every time is measured from that tenant's own submission (when its batch was created), in seconds.

| Metric | Fair scheduling | First come, first served |
|---|---|---|
| Small tenants: batch done, from its submission (slowest of B, C, D) | 8.84 [8.84–8.97] | 63.25 [63.24–63.26] |
| Small tenants: wait from submission until a request's model call starts, p50 | 4.26 [4.26–4.32] | 59.6 [59.56–59.63] |
| Small tenants: same wait, p99 | 8.29 [8.19–8.39] | 62.65 [62.59–62.66] |
| Small tenants: wait from submission until the first request gets a slot (slowest) | 0.03 [0.01–0.03] | 60.31 [60.18–60.37] |
| Big tenant: batch done, from its submission | 68.31 [68.12–68.33] | 62.01 [61.95–62.03] |
| Total throughput: all requests / (last batch done − first batch submitted), requests/s | 48.3 [48.3–48.4] | 48.3 [48.3–48.3] |
| Busy workers, average over the run (of 16) | 13.4 [13.3–13.5] | 13.5 [13.4–13.5] |
| Idle worker-seconds over the run | 178 [167–181] | 172 [171–179] |
| Busy workers, average over the last 10 s of the run | 13.2 [12.7–13.5] | 13.1 [12.9–13.4] |

Jobs retried per run (same seed, so this should match across runs): 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.

```
Median time from submission until done (seconds)
small, fair      8.8  #####
small, fifo     63.2  #####################################
big,   fair     68.3  ########################################
big,   fifo     62.0  ####################################
```
