## Fairness experiment

Command: `RUNS=5 BIG=3000 SMALL=100 SMALL_DELAY=5 SEED=42 ERROR_RATE=0.02 CRASH_RATE=0.01 scripts/fairness-experiment.sh`

Tenant A: one batch of 3000 requests. Tenants B, C, D: one batch of 100 requests each, submitted 5s after A's.
Fake model: 50–500 ms per request, error rate 0.02, cut-off-reply rate 0.01, seed 42. 16 workers, window of 32.
5 runs per mode, same seed and arrival timing; values are median [min–max] over the runs.
Every time is measured from that tenant's own submission (when its batch was created), in seconds.

| Metric | Fair scheduling | First come, first served |
|---|---|---|
| Small tenants: batch done, from its submission (slowest of B, C, D) | 9.17 [9.05–9.18] | 66.04 [65.23–66.07] |
| Small tenants: wait from submission until a request's model call starts, p50 | 4.48 [4.47–4.49] | 61.56 [61.46–61.71] |
| Small tenants: same wait, p99 | 8.61 [8.51–8.61] | 64.68 [64.53–64.73] |
| Small tenants: wait from submission until the first request gets a slot (slowest) | 0.07 [0.04–0.08] | 62.23 [62.17–62.32] |
| Big tenant: batch done, from its submission | 73.52 [73.32–73.72] | 66.8 [66.65–66.82] |
| Total throughput: all requests / (last batch done − first batch submitted), requests/s | 44.9 [44.8–45] | 46.4 [46.4–47] |
| Busy workers, average over the run (of 16) | 13.1 [13–13.1] | 13.5 [13.4–13.7] |
| Idle worker-seconds over the run | 215 [213–218] | 174 [164–185] |
| Busy workers, average over the last 10 s of the run | 9 [8.5–9.2] | 12.5 [12.1–13.1] |

Jobs retried per run (same seed, so this should match across runs): 92, 92, 92, 92, 92, 92, 92, 92, 92, 92.

```
Median time from submission until done (seconds)
small, fair      9.2  #####
small, fifo     66.0  ####################################
big,   fair     73.5  ########################################
big,   fifo     66.8  ####################################
```
