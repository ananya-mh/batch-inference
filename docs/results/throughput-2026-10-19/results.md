## Throughput

Command: `REQUESTS=10000 RUNS=3 scripts/throughput.sh`, commit `d41e264`, run on 2026-10-19.
Near-instant fake model (1–5 ms, no errors), one batch of 10000 requests at a time, 3 batches per setting; median [min–max].

| Setting | RIVER_FETCH_COOLDOWN | WORKERS | SCHEDULER_WINDOW | Requests/s | Busy workers (average) |
|---|---|---|---|---|---|
| defaults (River's 100 ms fetch cooldown) | 100ms | 16 | 32 | 157 [157–157] | 1.3 of 16 |
| fetch cooldown 50 ms | 50ms | 16 | 32 | 311 [311–312] | 1.2 of 16 |
| fetch cooldown 10 ms | 10ms | 16 | 32 | 1416 [1388–1453] | 8.6 of 16 |
| fetch cooldown 1 ms | 1ms | 16 | 32 | 2203 [2113–2236] | 11.7 of 16 |
| fetch cooldown 1 ms, window 128 | 1ms | 16 | 128 | 2105 [2079–2154] | 11.9 of 16 |
| fetch cooldown 1 ms, 64 workers, window 128 | 1ms | 64 | 128 | 3278 [3244–3340] | 36.6 of 64 |
