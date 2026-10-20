## API load test

Command: `VUS=200 DURATION=60s REQUESTS=2000 scripts/load-test.sh`, commit `15a795f`, run on 2026-10-20, k6 grafana/k6:2.2.0.
200 simulated clients for 60s, each polling a random tenant's batch status and listing its batches, pausing 0.1–0.5 s between rounds, while three tenants' batches of 2000 requests ran (compose.yaml's settings).

| Endpoint | Requests | Requests/s | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) | Errors |
|---|---|---|---|---|---|---|---|
| `GET /v1/batches/{id}` (status) | 39593 | 655 | 0.5 | 1.2 | 8.7 | 48.6 | 0.00% |
| `GET /v1/batches?limit=20` (list) | 39593 | 655 | 1.2 | 6.5 | 14.1 | 76.9 | 0.00% |

Meanwhile the batches kept running: 3390 requests finished during the load; 3 of 3 batches were still in progress at the end.
