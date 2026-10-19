| Upload a 90.4 MB, 440,000-line file (checked line by line as it streams in) | 3.35 s median [3.26–3.36], 27 MB/s; server memory peaked at 64 MiB | 3 uploads | `scripts/measure.sh upload` |
| Batch status (`GET /v1/batches/{id}`) on a 50,000-request batch, while it runs | p50 5.7 ms, p90 6.8 ms, p99 12.1 ms, max 35.6 ms | 600 requests (200 per batch, 3 batches) | `scripts/measure.sh status` |
| Batch status on a 50,000-request batch, after it completes | p50 5.2 ms, p90 6.1 ms, p99 9.1 ms, max 13.1 ms | 600 requests (200 per batch, 3 batches) | `scripts/measure.sh status` |
| Write the results file of a completed 50,000-request batch (inside Postgres) | 0.851 s median [0.769–0.885] | 3 batches | `scripts/measure.sh status` |
| Cancel → `cancelled`, window of 32 full, fake model 50–500 ms | 0.455 s median [0.34–0.57]; the cancel call itself returns in 24 ms; 11.5 [9–15] requests were mid-call | 10 cancels | `scripts/measure.sh cancel` |
| Deadline → `expired` (server running), 2,000-request batch with a 10 s window | 21.5 ms median [2.7–34] | 5 batches | `scripts/measure.sh expiry` |
| Overdue batch after a restart (server killed mid-batch, down until 10 s past the deadline): server start → `expired` | 0.71 s median [0.71–0.72] | 3 crash tests, 500 requests each | `scripts/measure.sh overdue` |
