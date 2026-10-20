# Experiment results

Raw output of the measurement scripts, copied unchanged from their `results/` folders. The main README's fairness numbers come from the first two folders, its Grafana screenshots from the third, and its throughput, other measurements, and API load test from the rest.

| Folder | Command |
|--------|---------|
| [`fairness-2026-10-19-normal-rates/`](fairness-2026-10-19-normal-rates/) | `RUNS=5 BIG=3000 SMALL=100 SMALL_DELAY=5 SEED=42 ERROR_RATE=0.02 CRASH_RATE=0.01 scripts/fairness-experiment.sh` at commit `9274f49` (fetch cooldown 10 ms) |
| [`fairness-2026-10-19-no-errors/`](fairness-2026-10-19-no-errors/) | `RUNS=5 BIG=3000 SMALL=100 SMALL_DELAY=5 SEED=42 ERROR_RATE=0 CRASH_RATE=0 scripts/fairness-experiment.sh` at commit `9274f49` (fetch cooldown 10 ms) |
| [`fairness-2026-10-19-screenshot-run/`](fairness-2026-10-19-screenshot-run/) | `RUNS=1 scripts/fairness-experiment.sh`, with `docker compose --profile monitoring up` running; the run behind the README's Grafana screenshots (run tags `fair-1-1792442151` and `fifo-1-1792442227`), taken while the fetch cooldown was still River's 100 ms |
| [`throughput-2026-10-19/`](throughput-2026-10-19/) | `REQUESTS=10000 RUNS=3 scripts/throughput.sh` at commit `d41e264`: requests per second under different River fetch cooldowns, worker counts and windows |
| [`measurements-2026-10-19/`](measurements-2026-10-19/) | `scripts/measure.sh` at commit `d41e264`: upload, status latency, results file write, cancel, expiry, overdue after restart; `results.md` is the table, the `.txt` files are the per-run numbers |
| [`load-test-2026-10-19-50-clients/`](load-test-2026-10-19-50-clients/) | `VUS=50 DURATION=60s REQUESTS=2000 scripts/load-test.sh` at commit `15a795f`: `results.md` is the table, `k6-summary.json` is k6's full output |
| [`load-test-2026-10-19-200-clients/`](load-test-2026-10-19-200-clients/) | `VUS=200 DURATION=60s REQUESTS=2000 scripts/load-test.sh` at commit `15a795f` |

All ran on 2026-10-19 on a laptop (Docker Desktop on macOS, 10 CPUs) with `compose.yaml`'s settings: 16 workers, a window of 32, fake model at 50–500 ms per request.

## Files in the fairness folders

- **`results.md`**: the summary table (median and range per mode) and the bar chart, exactly as the script printed them.
- **`runs.csv`**: one row per run (fair and first come, first served, alternating: 5 of each in the first two folders, 1 of each in the screenshot run). Times are seconds, measured from each tenant's own submission:
  - `small_completion_s`: until the slowest of the three small tenants' batches was done
  - `small_wait_p50_s`, `small_wait_p99_s`: per small-tenant request, from its batch's submission until its model call started
  - `small_first_slot_s`: until the first request of the slowest small tenant got a slot
  - `big_completion_s`: until the big tenant's batch was done
  - `throughput_rps`: all requests / (last batch done − first batch submitted)
  - `arrival_gap_s`: how long after the big batch the small ones were actually submitted
  - `retried_jobs`: jobs that needed more than one attempt (identical in every run, since the seed is the same)
  - `busy_workers_mean`, `idle_worker_s`, `busy_workers_last10s`: from the busy-worker samples below, over the run's start to end
- **`workers-<mode>-<run>.csv`**: busy-worker samples taken during the run, about five a second, with no header. Columns: Unix time (seconds, from the database clock), busy workers (River jobs of this run in the `running` state), and jobs waiting for a retry. Sampling keeps going for up to a second after the run ends; the summaries in `runs.csv` only use samples between the run's first submission and its last completion.
