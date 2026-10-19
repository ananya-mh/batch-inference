# batch-inference

A multi-tenant batch inference service in Go. Upload a file of AI requests, and the service runs them in the background on a shared model and gives back a results file. It follows the OpenAI Batch API, so the official OpenAI SDKs work against it unchanged. It is built to be fair across tenants, to never lose work, and to survive crashes.

> The full README (design, guarantees and their tests, experiment results) is still to come.

## Run it

```sh
docker compose up          # Postgres, the server on :8080, and a fake model on :8000
go test -race ./...        # needs the Postgres from docker compose running
```

## Crash test

`scripts/crash-test.sh` shows the service surviving a hard crash. It starts a batch of 2,000 requests, kills the server with `SIGKILL` partway through (no chance to clean up), starts it again, and then checks the database:

- every request has exactly one result (nothing lost, nothing saved twice), and
- every `custom_id` appears in exactly one of the results and errors files.

```sh
scripts/crash-test.sh          # 2,000 requests; pass a number for a different size
```

It needs Docker with Compose and `curl`, and exits non-zero if any check fails. The fake model stays flaky (2% errors, 1% cut-off replies), so retries happen as well. A real run:

```
==> Killing the server with SIGKILL (32 requests in flight, 14 mid-call to the model)

==> Checking the database
  ok    batch completed
  ok    every request has exactly one result (2000 results, 2000 distinct)
  ok    every custom_id is in exactly one output file (2000 lines, 2000 distinct)
  ok    the crash really interrupted work (14 requests were mid-call)

==================== crash test summary ====================
 Result:                    PASS
 Requests in batch:         2000
 Killed at:                 202 done, 32 in flight (14 mid-call)
 Jobs rescued by River:     16
 Jobs retried (any reason): 80
 Results saved:             2000 (2000 distinct custom_ids)
 Succeeded / failed:        2000 / 0
 Results / errors file:     2000 / 0 lines
 Finished after restart:    40s
=============================================================
```

How it recovers: each request's River job stays in Postgres. Jobs that were mid-call when the server died are left marked as running; River's rescuer notices they are stuck and runs them again. Saving a result and marking the job done happen in one transaction, and the results table allows only one result per `(batch_id, custom_id)`, so a request that runs twice is still saved once. For this test the server rescues stuck jobs after 20 seconds instead of the usual 5 minutes.
