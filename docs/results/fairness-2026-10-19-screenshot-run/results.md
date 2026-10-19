## Fairness experiment

Command: `RUNS=1 BIG=3000 SMALL=100 SMALL_DELAY=5 SEED=42 ERROR_RATE=0.02 CRASH_RATE=0.01 scripts/fairness-experiment.sh`

Tenant A: one batch of 3000 requests. Tenants B, C, D: one batch of 100 requests each, submitted 5s after A's.
Fake model: 50–500 ms per request, error rate 0.02, cut-off-reply rate 0.01, seed 42. 16 workers, window of 32.
1 runs per mode, same seed and arrival timing; values are median [min–max] over the runs.
Every time is measured from that tenant's own submission (when its batch was created), in seconds.

| Metric | Fair scheduling | First come, first served |
|---|---|---|
| Small tenants: batch done, from its submission (slowest of B, C, D) | 9.14 [9.14–9.14] | 66.25 [66.25–66.25] |
| Small tenants: wait from submission until a request's model call starts, p50 | 4.46 [4.46–4.46] | 61.53 [61.53–61.53] |
| Small tenants: same wait, p99 | 8.48 [8.48–8.48] | 64.65 [64.65–64.65] |
| Small tenants: wait from submission until the first request gets a slot (slowest) | 0.06 [0.06–0.06] | 62.21 [62.21–62.21] |
| Big tenant: batch done, from its submission | 73.73 [73.73–73.73] | 66.65 [66.65–66.65] |
| Total throughput: all requests / (last batch done − first batch submitted), requests/s | 44.8 [44.8–44.8] | 46.3 [46.3–46.3] |
| Busy workers, average over the run (of 16) | 13.1 [13.1–13.1] | 13.4 [13.4–13.4] |
| Idle worker-seconds over the run | 211 [211–211] | 187 [187–187] |
| Busy workers, average over the last 10 s of the run | 8.6 [8.6–8.6] | 12.2 [12.2–12.2] |

Jobs retried per run (same seed, so this should match across runs): 92, 92.

```
Median time from submission until done (seconds)
small, fair      9.1  #####
small, fifo     66.2  ####################################
big,   fair     73.7  ########################################
big,   fifo     66.7  ####################################
```
