# batch-inference

A multi-tenant batch inference service in Go. Upload a file of AI requests, and the service runs them in the background on a shared model and gives back a results file. It follows the [OpenAI Batch API](https://platform.openai.com/docs/guides/batch), so the official OpenAI SDKs work against it unchanged, and it can send work to any OpenAI-compatible model server (vLLM, Ollama, or a hosted provider). It is built to never lose work, never save a result twice, and survive crashes, and (next on the list) to be fair across tenants.

**Status:** work in progress. Done: files, batches, workers, results files, cancel, and expiry. Still to come: fair scheduling across tenants, Prometheus metrics, CI, the load test and noisy-neighbor experiment, and a run on a real GPU.

## Run it

```sh
docker compose up          # Postgres, the server on :8080, and a fake model on :8000
go test -race ./...        # needs the Postgres from docker compose running
```

The fake model pretends to be an AI model, so no GPU is needed: it waits 50 to 500 ms per request, fails 2% of requests, and cuts off 1% of replies halfway, so retries are exercised all the time. Development tenants and API keys (`sk-dev-tenant-a` to `sk-dev-tenant-d`) are set in `compose.yaml`.

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
- **A batch's lines wait in a `requests` table**, not all in the queue at once. A scheduler keeps a fixed number of them (the window, default 32) in River at a time and refills slots as requests finish. This is what will let a small batch from one tenant get a turn while another tenant's huge batch is running (fair scheduling is the next step).
- **Workers** call the model, read the *whole* reply before trusting it (a reply cut off midway is a failed attempt, never a result), and retry temporary failures quickly (1, 2, 4, 8 seconds; up to 5 attempts).
- **Statuses only change by compare-and-set** (`UPDATE ... WHERE status = <expected>`), so when two things race (cancel against finishing, expiry against cancel, two servers at once) exactly one wins.

## Guarantees and the tests that check them

| Guarantee | How | Tests |
|-----------|-----|-------|
| **No lost work.** A request whose worker crashes is retried. | Each request's job stays in Postgres; River rescues jobs left running by a dead worker. A job that dies on every attempt is failed with a reason, never left hanging. | `TestStuckJobIsRescuedByRiver`, `TestRetryAfterCrashedAttemptFinishes`, `TestRequestOfDeadJobIsGivenUp`, `TestFinalizingSurvivesCrash`, `scripts/crash-test.sh` |
| **No duplicate results.** Each result is saved once, keyed by `batch_id + custom_id`. | Saving moves the request out of `running` by compare-and-set, so only one attempt can save; the results table's primary key backs it up. | `TestSameRequestRunTwiceSavesOneResult`, `TestConcurrentFinishSavesOnlyOnce`, `TestTwoSchedulersNeverOverfillWindow` |
| **Cancel works.** After cancel, no new requests from that batch start. | Cancel and the worker's start check lock the batch row against each other; requests not yet running are cancelled at once and their slots freed. | `TestCancelMidBatch`, `TestCancelInProgressBatch`, `TestStartCancelsRequestOfCancellingBatch`, `TestCancelAndFinalizeRace` |
| **Fairness.** A tenant with a huge batch cannot block a tenant with a small one. | *Next: Progress item 9.* | *To come.* |
| **Expiry works.** Unfinished requests are marked expired when the window ends. | Deadlines are checked against the database's clock; at the deadline, unfinished requests expire and late results are dropped. | `TestExpiryMidBatch`, `TestOverdueBatchExpires`, `TestExpiryAndLastResultRace`, `TestExpiryAndCancelRace`, `scripts/crash-test.sh --expiry` |

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

## Measurements so far

All with the fake model on a laptop (Docker Desktop, 10 CPUs), 16 workers, window of 32.

| What | Result |
|------|--------|
| Upload a 92 MB, 440,000-line file | 2.1 s; server memory peaked at 54 MiB (the file is streamed, never held whole) |
| Batch status (`GET /v1/batches/{id}`) on a 50,000-line batch | median 4–5 ms |
| Write the 50,000-line results file | 0.55 s, inside Postgres |
| Cancel → `cancelled` (32 requests in flight, model 50–500 ms) | 0.29–0.54 s, median 0.39 s (10 runs) |
| Deadline → `expired` (server running) | 4–58 ms (5 batches) |
| Overdue batch after a restart | expired in under 1 s |

## Settings

| Variable | Default | Meaning |
|----------|---------|---------|
| `DATABASE_URL` | (required) | Postgres connection string |
| `MODEL_BASE_URL` | `http://localhost:8000/v1` | Any OpenAI-compatible server (the fake model, vLLM, ...) |
| `MODEL_API_KEY` | (none) | Only needed for hosted providers |
| `WORKERS` | 16 | Requests sent to the model at once |
| `SCHEDULER_WINDOW` | 2 × `WORKERS` | Most requests in the queue at once |
| `MAX_ATTEMPTS` | 5 | Tries per request |
| `JOB_TIMEOUT` | 2m | Longest one model call may take |
| `COMPLETION_WINDOWS` | `24h` | Allowed completion windows (tests add short ones like `10s`) |
| `MAX_UPLOAD_BYTES` | 104857600 | Upload size limit (100 MB) |
| `SEED_TENANTS` | (none) | Tenants to create at startup: `tenant_a=key,tenant_b=key` |
