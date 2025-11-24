# pgwf

pgwf (Postgres Workflow) is a pure-SQL workflow engine. It's built specifically to solve for reliable coordination of complex interconnected, long running jobs without dealing with arbitrarily large or complex internal job states. The pgwf workflow engine provides for durable job metadata, leasing, and traceability entirely inside PostgreSQL. This state stays lightweight (and reasonably sized for postgres) by avoiding any integration of internal job payloads or state. When paired with journal system, we can achieve complex distributed durable patterns with minimal infrastructure complexity or need for distributed transaction coordination.

## Core Objects

> Note: Everything lives inside the `pgwf` schema. 

> Note: The pgwf implementation is pure PostgresSQL and will work with any client language. This repository includes a golang testing harness but it is entirely independent from the implementation. Consumers of the framework should not need to write or use any golang code.

### Functions

| Function          | Description                                                                                                            | Signature                                                                                                                                   |
|-------------------|------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| `submit_job` | Inserts a new job, validates dependencies, and (optionally) emits notifications for `next_need`.                       | `submit_job(job_id TEXT, worker_id TEXT, next_need TEXT, wait_for TEXT[], singleton_key TEXT, available_at TIMESTAMPTZ, expires_at TIMESTAMPTZ)` |
| `get_work`        | Leases up to `limit_jobs` that match the supplied capabilities, assigning a fresh `lease_id` and visibility timeout.   | `get_work(worker_id TEXT, worker_caps TEXT[], lease_seconds INT, limit_jobs INT)`                                                           |
| `extend_lease`    | Heartbeats an active lease by pushing `lease_expires_at` into the future.                                              | `extend_lease(job_id TEXT, lease_id TEXT, worker_id TEXT, additional_seconds INT)`                                                          |
| `reschedule_job`  | Returns a leased job to the queue with updated capability/dependency metadata and clears the lease.                    | `reschedule_job(job_id TEXT, lease_id TEXT, worker_id TEXT, next_need TEXT, wait_for TEXT[], singleton_key TEXT, available_at TIMESTAMPTZ)` |
| `reschedule_unheld_job` | Mutates any `READY` job’s metadata/availability without first needing a lease.                                         | `reschedule_unheld_job(job_id TEXT, worker_id TEXT, next_need TEXT, wait_for TEXT[], singleton_key TEXT, available_at TIMESTAMPTZ)`         |
| `complete_job`    | Archives the job, deletes it from `pgwf.jobs`, removes the job_id from dependents, and wakes listeners.                | `complete_job(job_id TEXT, lease_id TEXT, worker_id TEXT)`                                                                                  |
| `complete_unheld_job` | Archives a `READY` job that locking and completing in a single op (and unblocking dependent work/notifying as needed). | `complete_unheld_job(job_id TEXT, worker_id TEXT)`                                                                                          |
| `cancel_job` | Marks a job for cancellation, preventing additional leases, extensions, or reschedules while capturing who requested it. | `cancel_job(job_id TEXT, worker_id TEXT, reason TEXT)` |
| `archive_cancelled_jobs` | Bulk-archives cancelled jobs whose leases have expired, removes dependencies, and emits aggregate traces. | `archive_cancelled_jobs(worker_id TEXT, limit INTEGER)` |

### Backing Tables

| Table | Columns (summary) | Purpose |
|-------|-------------------|---------|
| `jobs` | `job_id`, `next_need`, `wait_for[]`, `singleton_key`, `available_at`, `expires_at`, `lease_id`, `lease_expires_at`, `lease_expiration_count`, `consecutive_expirations`, timestamps, cancellation metadata | Live job metadata for runnable/leased/delayed jobs plus crash-concern counters. |
| `jobs_archive` | `job_id`, `next_need`, `wait_for[]`, `singleton_key`, `created_at`, `expires_at`, `lease_id`, `lease_expiration_count`, `consecutive_expirations`, `archived_at`, cancellation metadata | Immutable snapshot for completed or cancelled jobs; prevents `job_id` reuse while preserving historical counters. |
| `jobs_trace` | `trace_id`, `job_id`, `event_type`, `worker_id`, `event_at`, `input_data`, `output_data` | Append-only audit log of every workflow call. |

### Views

| View | Columns (summary) | Purpose |
|------|-------------------|---------|
| `jobs_with_status` | `jobs.*` plus computed `status` (`READY`, `PENDING_JOBS`, `AWAITING_FUTURE`, `ACTIVE`, `CRASH_CONCERN`, `EXPIRED`, `CANCELLED`) | Primary locking surface for functions that care about availability + lease state. |
| `jobs_friendly_status` | `job_id`, `status`, human-oriented columns (`creation_dt`, `pending_jobs`, `sleep_until`, `worker_id`, `cancelled_at`, `cancelled_by`, `expires_at`) | Convenience view for monitoring dashboards or ad-hoc inspection. |

#### Job Status Definitions

| Status | Description                                        |
|--------|----------------------------------------------------|
| `READY` | Job is available, unleased and ready to be leased. |
| `EXPIRED` | Job’s `expires_at` is in the past; it stays mutable (reschedule, cancel, extend) but will not be leased again. |
| `PENDING_JOBS` | Job is waiting for dependent jobs to complete.     |
| `AWAITING_FUTURE` | Job is waiting for a future time to run.           |
| `ACTIVE` | Job is currently being processed.                  |
| `CRASH_CONCERN` | Job repeatedly let leases expire; pgwf sidelines it until an operator clears the concern or reschedules/completes it. |
| `CANCELLED` | Job was marked for cancellation and is pending archival once any active lease expires. |


## Inspiration

The workflow engine patterns here are inspired in part by durable execution systems including [dbos](https://github.com/dbos-inc), [restate](https://github.com/restatedev/restate) and [temporal](https://github.com/temporalio/temporal). The implementation is inspired by [pgmq](https://github.com/tembo-io/pgmq)—particularly its disciplined use of SQL functions, visibility timeouts, and lightweight queue semantics.


## Garden Variety Use

1. Have multiple workers waiting for work. Each worker polls get_work(). Workers may have different capabilities (e.g. one worker might run python, another might run llm on gpu, another might be a human-based review process)
2. An external actor submits a job, including it's immediate need as well as any dependencies it might have (wait for other jobs to complete, wait for a certain time, etc).
3. A worker that has the needed capability will receive a lease for the new job and begin processing it.
4. The worker processes the job as long as it has sufficient capabilities. As the worker processes that job, it will keep calling extend_lease() to keep the lease alive.
5. At some point, the worker encounters a requirement that it cannot complete (this step requires a massive GPU). At this point, teh worker will rescheudle the job, informing the workflow engine of the new need.
6. Another worker, that can satisfy that new need, will get a new lease for the job and work through the additional rquirements.
7. When the job is complete, the GPU worker calls complete_job() to mark the job as complete.

## Why pgwf?

- **Separate execution metadata from payload state** – pgwf stores orchestration facts (leases, dependencies, trace) while an arbitrary journal or blob store can hold immutable payloads and outputs. This mirrors double-entry accounting: pgwf ensures work only moves forward, and the journal preserves every state mutation for replay. Keep coordination data small to minimize infrastructure requirements.
- **Single dependency** – Everything lives inside a schema; migrations are regular SQL, so operators reuse existing Postgres tooling. Store state in whatever way you want.
- **Deterministic leasing** – Visibility timeouts plus explicit `lease_id`s guarantee only one worker owns a job at a time and that resumes are idempotent.
- **Dependency-aware flow** – `wait_for` lets you fan out multiple child jobs, then automatically unblock the parent when all children finish.
- **Capability routing** – Workers advertise capabilities (e.g., “python”, “human-review”), and jobs hop between phases by updating `next_need`.
- **Singleton enforcement** – `singleton_key` ensures only one job for a logical entity (customer, invoice, run) can be leased simultaneously.
- **Observable & traceable** – Every mutation writes to `pgwf.jobs_trace` so you can rebuild timelines or audit operator actions.
- **Composable** – Functions can be called from stored procedures, application code, or CLI sessions.

## Key Capabilities

### Cancellation Lifecycle

Operators can call `pgwf.cancel_job` to mark in-flight or queued work for cancellation. Once a cancelled job’s lease expires (or if it was `READY`/`PENDING_JOBS`), it transitions to the `CANCELLED` status so it no longer leases, emits notifications, or blocks dependent work from progressing. The `pgwf.archive_cancelled_jobs` function performs bulk archival of these rows, drops the cancelled job_ids from any `wait_for` arrays, and appends both per-job (`job_cancel_archived`) and aggregate (`job_cancel_archived_run`) trace events. When the `pg_cron` extension is available, the schema automatically schedules `SELECT pgwf.archive_cancelled_jobs('pgwf-cron', 500);` to run every minute so cancelled rows are reclaimed without external automation. Environments without `pg_cron` can invoke the same function manually or via their own scheduler.

### Lease IDs

Every successful `get_work` returns a `lease_id`. Follow-on calls (`extend_lease`, `reschedule_job`, `complete_job`) require the same `(job_id, lease_id)` pair and verify that the lease has not expired. Benefits:

- Workers can safely retry idempotent operations; if the lease expired or another worker took over, pgwf raises a clear error.
- Observability improves because `jobs_trace` records which lease performed each action.
- Long-running work can emit heartbeats (`extend_lease`) without risk of a duplicate completion.
- Bugs in workers where they feed the wrong job_id into pgwf can be caught early as the lease_id cannot be easily known and is required to mutate a job.

### Worker capabilities (`next_need`)

Workers advertise capabilities via `worker_caps` when calling `get_work`. Jobs similarly declare what they need next via `next_need`. Example lifecycle:

1. `next_need = 'python.ingest'` – containerized Python workers pull data from the journal and normalize it.
2. Those workers reschedule the job with `next_need = 'python.transform'` or `'python.fanout'` depending on branching logic.
3. After automation, the job moves to `next_need = 'human.review'` so only compliance reviewers lease it.
4. Finally the job transitions to `next_need = 'python.finalize'` to publish outputs.

Capabilities make it easy to run heterogeneous fleets (containers, serverless, humans) against the same queue without separate tables per team.

### Singleton keys

`singleton_key` is an optional mutex scope. If all “billing for customer-42” jobs share `singleton_key = 'customer-42'`, pgwf ensures only one job with that key holds a lease at any time. This prevents concurrent workflows from trampling shared resources without involving advisory locks or external coordination.

### Wait-for semantics

`wait_for` stores the `job_id`s that must finish before a job becomes runnable:

1. Parent job first submits or leases each child (`child-1` … `child-N`).
2. Parent then reschedules itself with `wait_for => ARRAY['child-1', ..., 'child-N']`.
3. As each child calls `pgwf.complete_job`, pgwf removes that `job_id` from all dependent rows. Once the array becomes empty (and `available_at <= now()`), the parent automatically becomes eligible again—no extra bookkeeping in application code.

This is ideal for fork/join flows like “fan out a machine learning batch, then consolidate results” or “kick off multiple third-party checks, then proceed once all pass”.

### NOTIFY pattern

Whenever notification fan-out is enabled and new runnable work appears—job submission, reschedule, dependency release—pgwf emits `NOTIFY pgwf.need.<capability>`. Workers call `LISTEN pgwf.need.<capability>` for the caps they support, then `WAIT FOR NOTIFY` between polling loops. This reduces noisy polling and speeds up reaction time without an external message bus.


### Execution Trace

To make debugging and auditing easier, pgwf records every mutation in `pgwf.jobs_trace`. This includes the `job_id`, `lease_id`, `worker_id`, and the `event_type` (e.g. `submit_job`, `extend_lease`, `reschedule_job`, `complete_job`). `worker_id` could be structured to include things like team, environment, and worker identify, etc (e.g. `PROD;HUMAN-REVIEW;jim@oheir.org`). 
### Runtime toggles

- `pgwf.set_trace(enabled BOOLEAN)` / `pgwf.is_trace_enabled()` – Trace logging is **enabled by default** because it is invaluable when load is moderate and you need to reconstruct timelines. Each operation inserts a JSONB payload, so high-throughput systems with their own observability pipelines can disable it via `SELECT pgwf.set_trace(FALSE)` and re-enable later.
- `pgwf.set_notify(enabled BOOLEAN)` / `pgwf.is_notify_enabled()` – LISTEN/NOTIFY fan-out is **disabled by default** so pgwf plays nicely with connection pools (LISTEN keeps a session pinned). When disabled, pgwf neither emits `NOTIFY` events nor registers `LISTEN` channels, so workers rely on polling. Enable it on dedicated, long-lived connections with `SELECT pgwf.set_notify(TRUE)` to get near-instant wake-ups.
- `pgwf.set_crash_concern_threshold(p_threshold INTEGER)` / `pgwf.crash_concern_threshold()` – Controls how many consecutive lease expirations a job is allowed before pgwf marks it `CRASH_CONCERN` and removes it from future `get_work` results. Defaults to 5; lower it in stricter environments or raise it if flapping jobs are expected.

### Crash concern handling

Every time `pgwf.get_work` picks up a job whose previous lease already expired, pgwf increments two counters on the row: the lifetime `lease_expiration_count` and the `consecutive_expirations` streak. When the streak reaches the configured crash-concern threshold, the job finishes the in-flight lease but immediately transitions to the `CRASH_CONCERN` status, which causes subsequent `get_work` calls to ignore it. Operators can inspect the counters directly via `pgwf.jobs_with_status` and `pgwf.jobs_friendly_status`, then call `pgwf.clear_crash_concern(job_id, worker_id, reason TEXT DEFAULT NULL)` once they have remediated the underlying problem. Clearing the concern resets `consecutive_expirations` to zero (historical totals remain) and emits a `crash_concern_cleared` trace so the job becomes `READY` again.

## Execution Metadata vs Payload State

`pgwf` is often used in combination with a journal system. The journal system provides an immutable, forward-only log of business state. The journal allows arbitrary state information (create a journal, append entries to describe progress, etc). pgwf slots alongside that journal:

- The journal owns payloads: inputs, intermediate artifacts, and outputs. Entries are never mutated and the journal only guarantees in-order durable consistency.
- pgwf owns execution metadata: which capability needs are next, who currently holds the lease, and which dependencies remain.

Because the two concerns are separate, you can replay or rehydrate workflows by reading the journal while pgwf keeps scheduling honest. pgwf never stores payload blobs. pgwf just stores pointer (`job_id`) back to the journal plus scheduling metadata.

## Inter-transaction Jobs

Because pgwf is implemented entirely inside Postgres, job creation can live inside the same transaction as your domain writes:

```sql
BEGIN;
INSERT INTO invoices (invoice_id, total_cents, status) VALUES ('inv-42', 12345, 'pending');
SELECT pgwf.submit_job('inv-42', 'billing-service', 'invoice.collect', ARRAY[]::TEXT[], 'customer-123', clock_timestamp());
COMMIT;
```

```sql
BEGIN;
INSERT INTO invoices (invoice_id, total_cents, status) VALUES ('inv-42', 12345, 'completed');
SELECT pgwf.complete_job('inv-42', 'lease1234', 'PROD;HUMAN-REVIEW;jim@oheir.org');
COMMIT;
```

If the transaction commits, both the invoice row and the workflow job become durable; if the transaction rolls back, neither persists. You get atomic guarantees without distributed locks, message buses, or two-phase commit. That tight coupling dramatically reduces the “write payload, enqueue later” gap that often causes lost work when applications crash between database and queue writes.

## Example: Durable Workflows with a Journal

```text
┌──────────────┐        ┌────────────┐        ┌──────────────┐
│  producer    │        │  journal   │        │    pgwf      │
└──────┬───────┘        └────┬───────┘        └─────┬────────┘
       │ write payload       │                      │
       └─────────────────────┘                      │
                job_id                              │
                                                    ▼
                                           lease / dependency mgmt
```

1. **Producer writes payload** – The producer persists the workflow input in journal, receiving a deterministic `job_id`.
2. **Submit metadata** – The producer calls `pgwf.submit_job(job_id, worker_id, next_need, wait_for, singleton_key, available_at)` to register the work. Example:

    ```sql
    SELECT job_id
    FROM pgwf.submit_job(
        p_job_id        => 'job-123',
        p_worker_id     => 'ingest-service',
        p_next_need     => 'transcode.video',
        p_wait_for      => ARRAY['preflight-99'],
        p_singleton_key => 'video-777',
        p_available_at  => clock_timestamp()
    );
    ```

3. **Workers poll** – Workers call `pgwf.get_work(worker_id, worker_caps, lease_seconds, limit_jobs)` to lease jobs. When notifications are enabled they also `LISTEN pgwf.need.<capability>` to wake up instantly; otherwise they simply poll. Each lease returns full metadata plus a fresh `lease_id`. A “python” worker might perform ETL, while a “human-review” worker handles compliance later.
4. **Process + heartbeat** – While running, workers use `pgwf.extend_lease(job_id, lease_id, worker_id, additional_seconds)` to keep ownership. If they expect a long pause, they can reschedule themselves with a future `available_at`.
5. **Reschedule when blocked** – Workers mutate capability + dependencies via `pgwf.reschedule_job(...)`. Example: after splitting a video into 10 child jobs, the parent reschedules itself with `wait_for = ARRAY['child-1', ..., 'child-10']`. Each child completion removes its `job_id`; when the final one finishes, the parent becomes runnable automatically. Another example: a worker might reschedule itself with `next_need = 'human-review'` if it detects a compliance issue.
6. **Complete** – After writing the final payload back to the journal, the worker calls `pgwf.complete_job(job_id, lease_id, worker_id)`. pgwf archives the row, deletes the live copy, removes the job_id from any dependent `wait_for` arrays, and emits NOTIFY signals to wake listeners (if enabled).

Because the payload lives in the journal, pgwf focuses purely on orchestration metadata and leasing. If a worker crashes, the visibility timeout makes the job eligible again while the payload remains durable in the journal.

## Repository Layout

| Path             | Purpose |
|------------------|---------|
| `pgwf.sql`       | Schema, tables, and SQL functions that implement the workflow runtime.
| `test/`          | Integration tests that spin up embedded Postgres, apply `pgwf.sql`, and exercise leasing + wait-for semantics.

## Development

1. Apply `pgwf.sql` to a Postgres instance (the script is idempotent).
2. Run the embedded integration suite with `go test ./test`.
3. Iterate on SQL definitions, keeping an eye on the trace tables to ensure observability guarantees remain intact.
