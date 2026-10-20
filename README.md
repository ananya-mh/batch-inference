# batch-inference

[![CI](https://github.com/ananya-mh/batch-inference/actions/workflows/ci.yml/badge.svg)](https://github.com/ananya-mh/batch-inference/actions/workflows/ci.yml)

A multi-tenant batch inference service in Go. Upload a file of AI requests, and the service runs them in the background on a shared model and gives back a results file. It follows the [OpenAI Batch API](https://platform.openai.com/docs/guides/batch), so the official OpenAI SDKs work against it unchanged, and it can send work to any OpenAI-compatible model server (vLLM, Ollama, or a hosted provider). It is built to be fair across tenants, never lose work, never save a result twice, and survive crashes.

**Status:** work in progress. Done: files, batches, workers, results files, cancel, expiry, fair scheduling across tenants, Prometheus metrics with a Grafana dashboard, and CI. Still to come: the full-size load test and a run on a real GPU.

## Run it

```sh
docker compose up                        # Postgres, the server on :8080, and a fake model on :8000
docker compose --profile monitoring up   # the same, plus Prometheus on :9090 and Grafana on :3000
go test -race ./...                      # needs the Postgres from docker compose running
```

The fake model pretends to be an AI model, so no GPU is needed: it waits 50 to 500 ms per request, fails 2% of requests, and cuts off 1% of replies halfway, so retries are exercised all the time. Development tenants and API keys (`sk-dev-tenant-a` to `sk-dev-tenant-d`) are set in `compose.yaml`.

**CI** ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs on every push and pull request: gofmt, `go vet`, and `go test -race ./...` against a real Postgres (started with `max_connections=300`, as in `compose.yaml`); shellcheck on the scripts; and both crash tests (`scripts/crash-test.sh 500` and `--expiry 500`) against the full Docker Compose stack.

## Use it with the OpenAI SDK

```python
from openai import OpenAI

client = OpenAI(base_url="http://localhost:8080/v1", api_key="sk-dev-tenant-a")

batch_file = client.files.create(file=open("tickets.jsonl", "rb"), purpose="batch")
batch = client.batches.create(
    input_file_id=batch_file.id,
    endpoint="/v1/chat/completions",
    completion_window="24h",
)
# ... later
batch = client.batches.retrieve(batch.id)
print(batch.status, batch.request_counts)
results = client.files.content(batch.output_file_id).text
```

Each line of the input file is one request, in OpenAI's format:

```json
{"custom_id": "ticket-1", "method": "POST", "url": "/v1/chat/completions", "body": {"model": "fake", "messages": [{"role": "user", "content": "Classify: my invoice is wrong"}]}}
```

## API

| Endpoint | What it does |
|----------|--------------|
| `POST /v1/files` | Upload a batch input file (multipart, `purpose=batch`). Checked line by line as it streams in; errors name the line, e.g. `line 3: duplicate custom_id "a"`. Up to 100 MB. |
| `GET /v1/files/{id}` | A file's details. |
| `GET /v1/files/{id}/content` | Download a file (input, results, or errors), streamed. |
| `POST /v1/batches` | Start a batch from an uploaded file. |
| `GET /v1/batches/{id}` | Status and progress (`request_counts`: total, completed, failed). |
| `GET /v1/batches` | List your batches, newest first, with OpenAI-style paging (`limit`, `after`, `has_more`). |
| `POST /v1/batches/{id}/cancel` | Cancel a batch. |

Requests use `Authorization: Bearer <api key>`. A tenant only ever sees its own files and batches; another tenant's ID gets the same 404 as one that doesn't exist.

Results come back in two files, in OpenAI's format: `output_file_id` (successful requests) and `error_file_id` (failed, cancelled, and expired requests; `null` if there are none). Every `custom_id` from the input appears in exactly one of them.

## How it works

- **Postgres holds everything**: uploaded files (one row per line), batches, requests, results, and the job queue ([River](https://riverqueue.com)).
- **A batch's lines wait in a `requests` table**, not all in the queue at once. A scheduler keeps a fixed number of them (the window, default 32) in River at a time and refills slots as requests finish.
- **Fair scheduling:** each free slot goes to the tenant with the fewest requests in flight (ties: whoever's oldest request has waited longest). A tenant alone gets the whole window; when a second tenant arrives, freed slots go to it until both have an equal share. So a small batch gets going within a fraction of a second even while another tenant's huge batch is running, instead of waiting for all of it.
- **Workers** call the model, read the *whole* reply before trusting it (a reply cut off midway is a failed attempt, never a result), and retry temporary failures quickly (1, 2, 4, 8 seconds; up to 5 attempts).
- **Statuses only change by compare-and-set** (`UPDATE ... WHERE status = <expected>`), so when two things race (cancel against finishing, expiry against cancel, two servers at once) exactly one wins.

## Guarantees and the tests that check them

| Guarantee | How | Tests |
|-----------|-----|-------|
| **No lost work.** A request whose worker crashes is retried. | Each request's job stays in Postgres; River rescues jobs left running by a dead worker. A request interrupted by a shutdown runs again without using up an attempt. A job that dies on every attempt is failed with a reason, never left hanging. | `TestStuckJobIsRescuedByRiver`, `TestRetryAfterCrashedAttemptFinishes`, `TestShutdownMidRequestKeepsItsAttempt`, `TestRequestOfDeadJobIsGivenUp`, `TestFinalizingSurvivesCrash`, `scripts/crash-test.sh` |
| **No duplicate results.** Each result is saved once, keyed by `batch_id + custom_id`. | Saving moves the request out of `running` by compare-and-set, so only one attempt can save; the results table's primary key backs it up. | `TestSameRequestRunTwiceSavesOneResult`, `TestConcurrentFinishSavesOnlyOnce`, `TestTwoSchedulersNeverOverfillWindow` |
| **Cancel works.** After cancel, no new requests from that batch start. | Cancel and the worker's start check lock the batch row against each other; requests not yet running are cancelled at once and their slots freed. | `TestCancelMidBatch`, `TestCancelInProgressBatch`, `TestStartCancelsRequestOfCancellingBatch`, `TestCancelAndFinalizeRace`, `TestCancelAndFanOutRace` |
| **Fairness.** A tenant with a huge batch cannot block a tenant with a small one. | Free slots go to the tenant with the fewest requests in flight, so a newly arriving tenant gets slots as soon as any free up. | `TestSmallTenantNotBlockedByBigBatch`, `TestArrivingTenantGetsFreedSlots`, `TestLoneTenantGetsWholeWindow`, `TestNewTenantGetsFreedSlots`, `scripts/fairness-experiment.sh` |
| **Expiry works.** Unfinished requests are marked expired when the window ends. | Deadlines are checked against the database's clock; at the deadline, unfinished requests expire and late results are dropped. | `TestExpiryMidBatch`, `TestOverdueBatchExpires`, `TestExpiryAndLastResultRace`, `TestUndoneExpiryIsNeverVisible`, `TestExpiryAndCancelRace`, `scripts/crash-test.sh --expiry` |

**Invariants checked after every end-to-end test.** Whatever a test did (crashes, cancels, expiry, races), once its workers stop, `testfixtures.CheckIntegrity` checks that: every queued or running request has a River job for it; no two requests share a job; results exist exactly for succeeded and failed requests; a finished batch has no unfinished requests and lists every `custom_id` exactly once across its two files; file line counts are right; and no batch has more than one ending. A test breaks each invariant on purpose to make sure the checks notice.

## Crash tests

`scripts/crash-test.sh` shows the service surviving a hard crash. It starts a batch of 2,000 requests, kills the server with `SIGKILL` partway through (no chance to clean up), starts it again, and checks the database. It needs Docker with Compose and `curl`, and exits non-zero if any check fails.

```sh
scripts/crash-test.sh            # kill mid-batch, restart, the batch must complete
scripts/crash-test.sh --expiry   # kill mid-batch and stay down past the batch's deadline
```

A real run of the first:

```
==> Killing the server with SIGKILL (32 requests in flight, 18 mid-call to the model)

==> Checking the database
  ok    batch completed
  ok    every custom_id is in exactly one output file (2000 lines, 2000 distinct)
  ok    no request has more than one result (2000 results, 2000 distinct)
  ok    the crash really interrupted work (18 requests were mid-call)
  ok    every request has exactly one result (2000 of 2000)

==================== crash test summary ====================
 Mode:                      normal
 Result:                    PASS
 Requests in batch:         2000
 Killed at:                 205 done, 32 in flight (18 mid-call)
 Jobs rescued by River:     15
 Jobs retried (any reason): 65
 Results saved:             2000 (2000 distinct custom_ids)
 Succeeded / failed:        2000 / 0
 Finished after restart:    40s
 Results / errors file:     2000 / 0 lines
=============================================================
```

The `--expiry` variant gives the batch a 30-second completion window and keeps the server down until 10 seconds after the deadline. After the restart, the overdue batch must expire promptly, keep the results saved before the deadline, and keep nothing saved after it:

```
  ok    batch expired
  ok    every custom_id is in exactly one output file (2000 lines, 2000 distinct)
  ok    every request finished before the deadline or expired (219 + 0 + 1781)
  ok    no result saved after the deadline was kept (0)
  ok    expired requests say batch_expired in the errors file (1781)
  ok    expired within 5s of the restart (0s)
```

For these tests the server rescues jobs of a dead worker after 20 seconds instead of the usual 5 minutes.

## Measurements

All on one machine. Apple M5 (10 cores), 16 GB RAM, macOS 26.6.2. Docker 29.7.2 with 10 CPUs and 7.7 GB for its VM (everything below runs inside it). Postgres 17.11 (Debian 17.11-1.pgdg13+2), server built with go1.27.2. Every number below comes from a script in this repo (or, in the throughput-gap investigation, from the SQL query shown with it); each section names the command, the commit, and how many runs or requests it is based on, and links to the raw output in [`docs/results/`](docs/results/).

### Fairness experiment (noisy neighbor)

Tenant A submits one big batch; 5 seconds later tenants B, C and D each submit a small one. The same scenario runs with fair scheduling and with first come, first served, 5 times each, under identical conditions: same input files, same arrival timing, and the same fake model seed (so every request gets the same delay and the same errors in every run; the fake model is restarted before each run). Settings as in `compose.yaml`: 16 workers, a window of 32, fake model at 50–500 ms per request.

```sh
scripts/fairness-experiment.sh                 # defaults: 3,000-request big batch, 3 x 100-request small ones
```

Commit `9274f49` (with `RIVER_FETCH_COOLDOWN` at its new 10 ms default), run on 2026-10-19. A real run, `RUNS=5 BIG=3000 SMALL=100 SMALL_DELAY=5 SEED=42 ERROR_RATE=0.02 CRASH_RATE=0.01 scripts/fairness-experiment.sh` (raw data: [per-run CSV, busy-worker samples, and chart](docs/results/fairness-2026-10-19-normal-rates/)):

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

What it shows: with fair scheduling the small tenants get their first slot within 0.1 s of submitting and finish about 8 s after submitting; first come, first served makes them wait about 53 s for the big batch to get out of the way, about 7× longer. The cost is that the big batch finishes about 6 s (9%) later, since it shares the window, and total throughput is about 6% lower; the next section explains why. (With River's previous 100 ms fetch cooldown the same experiment gave 9.2 s against 66 s and a 3% throughput gap; everything is faster now because workers no longer wait for River's next fetch. That run is in the git history of `docs/results/`.)

#### Investigating the throughput gap

The hypothesis: with fair scheduling the big tenant finishes last, so its final retries hold slots during their backoff while the other workers sit idle. Two checks.

**(a) Turn errors and cut-off replies off.** If retries cause the gap, it should disappear. It does: throughput is the same in both modes ([raw data](docs/results/fairness-2026-10-19-no-errors/)). From `RUNS=5 BIG=3000 SMALL=100 SMALL_DELAY=5 SEED=42 ERROR_RATE=0 CRASH_RATE=0 scripts/fairness-experiment.sh`:

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

**(b) Look for an idle tail.** With the normal error rates (the first table above), the last 10 seconds of the fair runs average 7.7 busy workers out of 16, against 14.2 first come, first served, and the fair runs have about 65 more idle worker-seconds in total (87 against 22). With errors off, the tails are the same (15.3 against 15.5).

**What causes it.** The last job to finish in every fair run is the same request, `a-2341`, on its fifth attempt: with seed 42 the fake model fails it four times in a row, so its retries wait 1 + 2 + 4 + 8 seconds. Under fair scheduling the big tenant's requests are reached later (they share the window), so this request starts about 18 s before the end and its retry chain *is* the end of the run, with the other workers idle during its last backoff. Under first come, first served it starts 18.8–19.8 s before the end and finishes 1.1–2.2 s early, while the small tenants' requests keep the workers busy. Query used, on the database after the run:

```sql
-- For each run, request a-2341's attempts and when it finished/was queued relative to the run's end.
WITH runs AS (
  SELECT metadata->>'experiment' AS tag, max(completed_at) AS ended FROM batches
  WHERE metadata->>'experiment' ~ '^(fair|fifo)-' GROUP BY 1 HAVING sum(request_total) = 3300)
SELECT r.tag, j.attempt,
       extract(epoch FROM r.ended - j.finalized_at) AS done_s_before_end,
       extract(epoch FROM r.ended - j.created_at)  AS queued_s_before_end
FROM runs r JOIN batches b ON b.metadata->>'experiment' = r.tag
JOIN river_job j ON j.args->>'batch_id' = b.id AND j.args->>'custom_id' = 'a-2341';
```

which gave 5 attempts in all 10 runs, done 0.1–0.2 s before the end in the fair runs and 1.1–2.2 s before the end in the first-come runs.

So the gap is a tail effect of one unlucky request's retry chain, not a slowdown while the batch is running. Because that chain takes about the same 18 seconds however fast the rest goes, it is a bigger share of a faster run: the gap grew from 3% to 6% when the fetch cooldown change made runs shorter. It would shrink as the big batch grows.

### Throughput

How many requests per second the service itself can push through, with a near-instant fake model so the model is never the limit. From `REQUESTS=10000 RUNS=3 scripts/throughput.sh`, commit `d41e264`, run on 2026-10-19 ([raw data](docs/results/throughput-2026-10-19/)):

| Setting | RIVER_FETCH_COOLDOWN | WORKERS | SCHEDULER_WINDOW | Requests/s | Busy workers (average) |
|---|---|---|---|---|---|
| defaults (River's 100 ms fetch cooldown) | 100ms | 16 | 32 | 157 [157–157] | 1.3 of 16 |
| fetch cooldown 50 ms | 50ms | 16 | 32 | 311 [311–312] | 1.2 of 16 |
| fetch cooldown 10 ms | 10ms | 16 | 32 | 1416 [1388–1453] | 8.6 of 16 |
| fetch cooldown 1 ms | 1ms | 16 | 32 | 2203 [2113–2236] | 11.7 of 16 |
| fetch cooldown 1 ms, window 128 | 1ms | 16 | 128 | 2105 [2079–2154] | 11.9 of 16 |
| fetch cooldown 1 ms, 64 workers, window 128 | 1ms | 64 | 128 | 3278 [3244–3340] | 36.6 of 64 |

**What limits it.** With River's default settings throughput is about 160 requests/s, and it is River's fetch cooldown: River fetches new jobs at most once per `RIVER_FETCH_COOLDOWN` (100 ms by default, and River's own docs say "throughput is limited by this value"), taking at most as many jobs as there are free workers, so 16 workers can start at most 16 × 10 = 160 jobs a second. The table bears this out: halving the cooldown doubles throughput (157 → 311 requests/s), and at 100 ms only about 1.3 of 16 workers are busy, because they spend most of their time waiting for the next fetch. At 10 ms throughput reaches about 1,400 requests/s (the cooldown alone would allow 1,600), and from 1 ms other limits take over: a bigger window changes nothing, more workers help (64 workers reach about 3,300 requests/s with 37 busy), so what is left is the per-request work (the start check, the save, the scheduler) and CPU in the Docker VM.

**Does it matter with a real model?** Less: 16 workers calling a model that takes 0.5 s can only do about 32 requests/s however fast River fetches. But a 100 ms cooldown also adds up to 100 ms of idle time before each worker's next job (about 50 ms on average), against a model call of 0.275 s on average in the fairness experiment. Rerunning that experiment with a 10 ms cooldown measured the difference: busy workers rose from 13.1 to 14.7 of 16 (fair) and 13.5 to 15.7 (first come), and throughput from 44.9 to 50.3 and 46.4 to 53.7 requests/s, close to the 16 / 0.275 ≈ 58 that 16 workers could do. Lowering it costs only more frequent, cheap fetch queries, so **the server's default is now 10 ms** (`RIVER_FETCH_COOLDOWN`); the fairness experiment above was rerun with it.

### API load test

Many clients polling batch status and listing batches while three tenants' batches of 2,000 requests run (compose.yaml's settings, so workers, the scheduler and finishing batches all share the database). k6 (`grafana/k6:2.2.0`) runs inside Docker's network; each simulated client polls a random tenant's batch status and lists its batches, pausing 0.1–0.5 s between rounds. Commit `15a795f`, run on 2026-10-19. The three tenants already had 67 to 106 earlier batches each from the experiments, which is what the list endpoint pages through.

50 clients for 60 s, `VUS=50 DURATION=60s REQUESTS=2000 scripts/load-test.sh` ([raw data](docs/results/load-test-2026-10-19-50-clients/)):

| Endpoint | Requests | Requests/s | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) | Errors |
|---|---|---|---|---|---|---|---|
| `GET /v1/batches/{id}` (status) | 9920 | 164 | 0.4 | 0.6 | 1.3 | 87.3 | 0.00% |
| `GET /v1/batches?limit=20` (list) | 9920 | 164 | 0.7 | 6.5 | 8.7 | 86.6 | 0.00% |

Meanwhile the batches kept running: 3343 requests finished during the load; 3 of 3 batches were still in progress at the end.

200 clients for 60 s, `VUS=200 DURATION=60s REQUESTS=2000 scripts/load-test.sh` ([raw data](docs/results/load-test-2026-10-19-200-clients/)):

| Endpoint | Requests | Requests/s | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) | Errors |
|---|---|---|---|---|---|---|---|
| `GET /v1/batches/{id}` (status) | 39593 | 655 | 0.5 | 1.2 | 8.7 | 48.6 | 0.00% |
| `GET /v1/batches?limit=20` (list) | 39593 | 655 | 1.2 | 6.5 | 14.1 | 76.9 | 0.00% |

Meanwhile the batches kept running: 3390 requests finished during the load; 3 of 3 batches were still in progress at the end.

No errors at either load, and the batches kept the same pace. These latencies are lower than the status rows in the table below because k6 talks to the server inside Docker's network, while `scripts/measure.sh` uses curl from macOS through Docker's port forwarding, and those batches have 50,000 requests rather than 2,000.

### Other measurements

Commit `d41e264`, run on 2026-10-19, when `RIVER_FETCH_COOLDOWN` still defaulted to River's 100 ms (it is now 10 ms). Settings are compose.yaml's (16 workers, window of 32, fake model at 50–500 ms with 2% errors and 1% cut-off replies) unless a row says otherwise. The `status` rows use a near-instant fake model (1–5 ms, no errors) and `RIVER_FETCH_COOLDOWN=10ms` so 50,000-request batches finish in minutes; status latency and file writing depend on batch size, not model speed. Raw output: [`docs/results/measurements-2026-10-19/`](docs/results/measurements-2026-10-19/).

| Measurement | Result | Based on | Command |
|---|---|---|---|
| Upload a 90.4 MB, 440,000-line file (checked line by line as it streams in) | 3.35 s median [3.26–3.36], 27 MB/s; server memory peaked at 64 MiB | 3 uploads | `scripts/measure.sh upload` |
| Batch status (`GET /v1/batches/{id}`) on a 50,000-request batch, while it runs | p50 5.7 ms, p90 6.8 ms, p99 12.1 ms, max 35.6 ms | 600 requests (200 per batch, 3 batches) | `scripts/measure.sh status` |
| Batch status on a 50,000-request batch, after it completes | p50 5.2 ms, p90 6.1 ms, p99 9.1 ms, max 13.1 ms | 600 requests (200 per batch, 3 batches) | `scripts/measure.sh status` |
| Write the results file of a completed 50,000-request batch (inside Postgres) | 0.851 s median [0.769–0.885] | 3 batches | `scripts/measure.sh status` |
| Cancel → `cancelled`, window of 32 full, fake model 50–500 ms | 0.455 s median [0.34–0.57]; the cancel call itself returns in 24 ms; 11.5 [9–15] requests were mid-call | 10 cancels | `scripts/measure.sh cancel` |
| Deadline → `expired` (server running), 2,000-request batch with a 10 s window | 21.5 ms median [2.7–34] | 5 batches | `scripts/measure.sh expiry` |
| Overdue batch after a restart (server killed mid-batch, down until 10 s past the deadline): server start → `expired` | 0.71 s median [0.71–0.72] | 3 crash tests, 500 requests each | `scripts/measure.sh overdue` |

## Metrics

The server serves Prometheus metrics at `http://localhost:8080/metrics`, and `docker compose up` starts a Prometheus at `http://localhost:9090` that scrapes it every second. Labels are only ever `tenant` and `outcome`, never a batch or request ID (a test checks this), so the number of time series stays small however many batches run.

| Metric | Type | Labels | What it measures |
|--------|------|--------|------------------|
| `batch_workers_busy` | gauge | | Workers on this server running a request right now |
| `batch_workers_total` | gauge | | Workers on this server |
| `batch_requests_in_flight` | gauge | `tenant` | Requests holding a slot in the window (in River or running, including waiting for a retry) |
| `batch_requests_waiting_for_retry` | gauge | `tenant` | In-flight requests waiting out a retry delay |
| `batch_queue_depth` | gauge | `tenant` | Requests waiting in the scheduler's table for a slot |
| `batch_request_wait_seconds` | histogram | `tenant` | From a batch's submission until a request's model call first started |
| `batch_request_outcomes_total` | counter | `tenant`, `outcome` | Requests by outcome: `completed`, `failed`, `retried` (one per retried attempt), `expired`, `cancelled` |
| `batch_model_call_seconds` | histogram | `outcome` | Model call duration, `success` or `error` |
| `batch_scheduler_pass_seconds` | histogram | `outcome` | Scheduler pass duration, `ran` or `skipped` (another server held the lock) |

The three per-tenant gauges are read from Postgres at each scrape and describe the whole service, so with several servers take `max()` of them across servers, not `sum()`. Outcomes are counted only after the database change that caused them commits, so rolled-back work is never counted.

### Dashboard

`docker compose --profile monitoring up` also starts Grafana at `http://localhost:3000` with a "Batch inference" dashboard (provisioned from `deploy/grafana/`, no login needed to view): each tenant's share of the window, busy workers, p99 wait per tenant, and outcomes per second.

This is the fair-scheduling run of a real experiment. Tenant A's 3,000-request batch holds all 32 slots until tenants B, C and D submit 100 requests each; A then drops to 8 slots while each of them gets 8, and takes the whole window back once they finish. The small tenants' p99 wait stays around 10 s while tenant A's own queue keeps growing:

![Grafana dashboard during a fair-scheduling run](docs/images/grafana-fair-scheduling.png)

The first come, first served run right after it, for contrast (just the share-of-the-window panel): tenant A keeps all 32 slots until it is done, and only then do B, C and D get in.

![Requests in flight per tenant under first come, first served](docs/images/grafana-first-come-in-flight.png)

Produced with:

```sh
docker compose --profile monitoring up -d
RUNS=1 scripts/fairness-experiment.sh                      # one fair run, then one first-come run
scripts/grafana-screenshot.sh fair-1-1792442151 docs/images/grafana-fair-scheduling.png
scripts/grafana-screenshot.sh fifo-1-1792442227 fifo.png   # then cropped to the top-left panel
```

The run tags come from the batches' metadata (`fair-1-…`, `fifo-1-…`); the run's raw results are in [`docs/results/fairness-2026-10-19-screenshot-run/`](docs/results/fairness-2026-10-19-screenshot-run/). The first-come image shows only the share-of-the-window panel because the other panels average over 30 seconds, so their first 30 seconds still include the end of the fair run before it.

**Repeating the throughput investigation with metrics** (instead of the experiment script's own sampler), in Prometheus at `http://localhost:9090`:

```promql
sum(batch_workers_busy)                                     # busy workers over time: look for an idle tail
sum by (tenant) (batch_requests_waiting_for_retry)          # who holds slots while waiting for a retry
sum by (tenant) (batch_requests_in_flight)                  # each tenant's share of the window
histogram_quantile(0.99, sum by (tenant, le) (rate(batch_request_wait_seconds_bucket[1m])))  # p99 wait per tenant
sum by (outcome) (rate(batch_request_outcomes_total[1m]))   # completed, retried, failed per second
```

## Settings

Server settings, read from environment variables (`compose.yaml` sets the development values):

| Variable | Default | Meaning |
|----------|---------|---------|
| `DATABASE_URL` | (required) | Postgres connection string |
| `HTTP_ADDR` | `:8080` | Address the API listens on |
| `MODEL_BASE_URL` | `http://localhost:8000/v1` | Any OpenAI-compatible server (the fake model, vLLM, ...) |
| `MODEL_API_KEY` | (none) | Only needed for hosted providers |
| `WORKERS` | 16 | Requests sent to the model at once |
| `SCHEDULER_WINDOW` | 2 × `WORKERS` | Most requests queued or running at once |
| `FAIR_SCHEDULING` | `true` | `true`: each free slot goes to the tenant with the fewest requests in flight. `false`: first come, first served (the experiment's baseline) |
| `MAX_ATTEMPTS` | 5 | Tries per request |
| `JOB_TIMEOUT` | 2m | Longest one model call may take |
| `RESCUE_STUCK_JOBS_AFTER` | 5m | How long a job may stay running before River assumes its worker died and runs it again; must be longer than `JOB_TIMEOUT` |
| `SCHEDULER_INTERVAL` | 200ms | How often the scheduler runs when no finished request nudges it sooner |
| `RIVER_FETCH_COOLDOWN` | 10ms | Shortest time between River's fetches of new jobs; caps throughput at about `WORKERS` / cooldown (see [Throughput](#throughput)). River's own default is 100 ms |
| `RIVER_FETCH_POLL_INTERVAL` | 1s | How often River looks for jobs when it hears of none |
| `COMPLETION_WINDOWS` | `24h` | Allowed completion windows (tests add short ones like `10s`) |
| `MAX_UPLOAD_BYTES` | 104857600 | Upload size limit (100 MB) |
| `MAX_BATCH_REQUESTS` | 50000 | Most requests in one batch, as with OpenAI; a bigger file is refused when the batch is created |
| `SEED_TENANTS` | (none) | **Development only.** Tenants and API keys to create at startup: `tenant_a=key,tenant_b=key`. There is no sign-up; real deployments should create tenants another way and never put keys in a file |

Fake model settings (`cmd/fakemodel`), the pretend AI model used in development and tests:

| Variable | Default | Meaning |
|----------|---------|---------|
| `FAKE_MODEL_ADDR` | `:8000` | Address it listens on (vLLM's default port) |
| `FAKE_MODEL_MIN_DELAY` | 50ms | Shortest time to answer a request |
| `FAKE_MODEL_MAX_DELAY` | 500ms | Longest time to answer a request (each request waits a random time in between) |
| `FAKE_MODEL_OUTPUT_WORDS` | 20 | Words in each reply |
| `FAKE_MODEL_ERROR_RATE` | 0.02 | Share of requests answered with HTTP 500 |
| `FAKE_MODEL_CRASH_RATE` | 0.01 | Share of replies cut off halfway (a 200 status, half the body, then the connection drops) |
| `FAKE_MODEL_SEED` | (none) | Makes delays, errors and cut-offs repeatable per request; unset means truly random |
