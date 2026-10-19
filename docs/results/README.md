# Experiment results

Raw output of `scripts/fairness-experiment.sh`, copied unchanged from its `results/` folder. The main README's fairness numbers come from the first two; the third is the run its Grafana screenshots were taken from.

| Folder | Command |
|--------|---------|
| [`fairness-2026-10-19-normal-rates/`](fairness-2026-10-19-normal-rates/) | `RUNS=5 BIG=3000 SMALL=100 SMALL_DELAY=5 SEED=42 ERROR_RATE=0.02 CRASH_RATE=0.01 scripts/fairness-experiment.sh` |
| [`fairness-2026-10-19-no-errors/`](fairness-2026-10-19-no-errors/) | `RUNS=5 BIG=3000 SMALL=100 SMALL_DELAY=5 SEED=42 ERROR_RATE=0 CRASH_RATE=0 scripts/fairness-experiment.sh` |
| [`fairness-2026-10-19-screenshot-run/`](fairness-2026-10-19-screenshot-run/) | `RUNS=1 scripts/fairness-experiment.sh`, with `docker compose --profile monitoring up` running; the run behind the README's Grafana screenshots (run tags `fair-1-1792442151` and `fifo-1-1792442227`) |

All three ran on 2026-10-19 on a laptop (Docker Desktop on macOS, 10 CPUs) with `compose.yaml`'s settings: 16 workers, a window of 32, fake model at 50–500 ms per request.

## Files in each folder

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
