# pgwf

pgwf (Postgres Workflow) is a pure-SQL workflow engine. It's built specifically to solve for reliable coordination of complex interconnected, long running jobs without dealing with arbitrarily large or complex internal job states. The pgwf workflow engine provides for durable job metadata, leasing, and traceability entirely inside PostgreSQL. Jobs may include a small JSON payload (object, ≤512 bytes) captured at creation and optionally updated when rescheduling to a new `next_need`, plus optional immutable JSON metadata (object, ≤8192 bytes) set at submission time. Larger state should live in external systems. When paired with journal system, we can achieve complex distributed durable patterns with minimal infrastructure complexity or need for distributed transaction coordination.

## Core Objects

> Note: Everything lives inside the `pgwf` schema. 

> Note: The pgwf implementation is pure PostgresSQL and will work with any client language. This repository includes a golang testing harness but it is entirely independent from the implementation. Consumers of the framework should not need to write or use any golang code.

### Functions

| Function          | Description                                                                                                            | Signature                                                                                                                                   |
|-------------------|------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| `submit_job` | Inserts a new job, validates dependencies, attaches an optional payload and metadata, and (optionally) emits notifications for `next_need`. | `submit_job(tenant_id TEXT, job_id TEXT, worker_id TEXT, next_need TEXT, wait_for TEXT[], payload JSONB, metadata JSONB, singleton_key TEXT, available_at TIMESTAMPTZ, expires_at TIMESTAMPTZ)` |
| `get_work`        | Leases up to `limit_jobs` that match the supplied capabilities and optional tenant filter, assigning a fresh `lease_id` and visibility timeout.   | `get_work(worker_id TEXT, worker_caps TEXT[], tenant_ids TEXT[], lease_seconds INT, limit_jobs INT)`                                                           |
| `extend_lease`    | Heartbeats an active lease by pushing `lease_expires_at` into the future.                                              | `extend_lease(tenant_id TEXT, job_id TEXT, lease_id TEXT, worker_id TEXT, additional_seconds INT)`                                                          |
| `reschedule_job`  | Returns a leased job to the queue with updated capability/dependency metadata, optional payload override, and clears the lease. | `reschedule_job(tenant_id TEXT, job_id TEXT, lease_id TEXT, worker_id TEXT, next_need TEXT, wait_for TEXT[], available_at TIMESTAMPTZ, payload JSONB)` |
| `reschedule_unheld_job` | Mutates any `READY` job's metadata/availability (including optional payload override) without first needing a lease.      | `reschedule_unheld_job(tenant_id TEXT, job_id TEXT, worker_id TEXT, next_need TEXT, wait_for TEXT[], available_at TIMESTAMPTZ, payload JSONB)`         |
| `complete_job`    | Archives the job, deletes it from `pgwf.jobs`, removes the job_id from dependents, and wakes listeners.                | `complete_job(tenant_id TEXT, job_id TEXT, lease_id TEXT, worker_id TEXT)`                                                                                  |
| `complete_unheld_job` | Archives a `READY` job that locking and completing in a single op (and unblocking dependent work/notifying as needed). | `complete_unheld_job(tenant_id TEXT, job_id TEXT, worker_id TEXT)`                                                                                          |
| `cancel_job` | Marks a job for cancellation, preventing additional leases, extensions, or reschedules while capturing who requested it. | `cancel_job(tenant_id TEXT, job_id TEXT, worker_id TEXT, reason TEXT)` |
| `archive_cancelled_jobs` | Bulk-archives cancelled jobs whose leases have expired, removes dependencies, and emits aggregate traces. | `archive_cancelled_jobs(worker_id TEXT, tenant_ids TEXT[], limit INTEGER)` |
| `clear_crash_concern` | Resets consecutive expiration counter for a job stuck in CRASH_CONCERN status. | `clear_crash_concern(tenant_id TEXT, job_id TEXT, worker_id TEXT, reason TEXT)` |

### Backing Tables

| Table | Columns (summary) | Purpose |
|-------|-------------------|---------|
| `jobs` | `tenant_id`, `job_id`, `next_need`, `wait_for[]`, `payload`, `metadata`, `singleton_key`, `available_at`, `expires_at`, `lease_id`, `lease_expires_at`, `lease_expiration_count`, `consecutive_expirations`, timestamps, cancellation metadata | Live job metadata for runnable/leased/delayed jobs plus crash-concern counters. Primary key: `(tenant_id, job_id)`. |
| `jobs_archive` | `tenant_id`, `job_id`, `next_need`, `wait_for[]`, `payload`, `metadata`, `singleton_key`, `created_at`, `expires_at`, `lease_id`, `lease_expiration_count`, `consecutive_expirations`, `archived_at`, cancellation metadata | Immutable snapshot for completed or cancelled jobs; prevents `job_id` reuse within same tenant. Primary key: `(tenant_id, job_id)`. |
| `jobs_trace` | `trace_id`, `tenant_id`, `job_id`, `event_type`, `worker_id`, `event_at`, `input_data`, `output_data` | Append-only audit log of every workflow call, scoped per tenant. |

### Views

| View | Columns (summary) | Purpose |
|------|-------------------|---------|
| `jobs_with_status` | `tenant_id`, `jobs.*` plus computed `status` (`READY`, `PENDING_JOBS`, `AWAITING_FUTURE`, `ACTIVE`, `CRASH_CONCERN`, `EXPIRED`, `CANCELLED`) | Primary locking surface for functions that care about availability + lease state. Status computation respects tenant boundaries for dependencies. |
| `jobs_friendly_status` | `tenant_id`, `job_id`, `status`, human-oriented columns (`creation_dt`, `pending_jobs`, `sleep_until`, `worker_id`, `cancelled_at`, `cancelled_by`, `expires_at`, `payload`) | Convenience view for monitoring dashboards or ad-hoc inspection. |

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

### Payloads

- Optional JSONB object supplied at submission time; defaults to `{}`.
- Must be an object and ≤512 bytes stored size (`pg_column_size`) in both `jobs` and `jobs_archive`.
- Returned from `submit_job`, `get_work`, and surfaced in status views; intentionally excluded from trace rows.

### Metadata

- Optional JSONB object supplied at submission time; defaults to `{}`.
- Must be an object and ≤8192 bytes stored size (`pg_column_size`) in both `jobs` and `jobs_archive`.
- Immutable after submission; returned from `submit_job` and surfaced in `jobs_with_status`.
- Intentionally excluded from trace rows.


## Inspiration

The workflow engine patterns here are inspired in part by durable execution systems including [dbos](https://github.com/dbos-inc), [restate](https://github.com/restatedev/restate) and [temporal](https://github.com/temporalio/temporal). The implementation is inspired by [pgmq](https://github.com/tembo-io/pgmq)—particularly its disciplined use of SQL functions, visibility timeouts, and lightweight queue semantics.


## Multi-Tenancy

pgwf supports multi-tenancy through a composite primary key `(tenant_id, job_id)` on all core tables. This provides complete data isolation between tenants at the database level while allowing efficient resource sharing.

### Key Multi-Tenant Characteristics

- **Tenant Isolation**: Jobs are scoped per tenant. Dependencies (`wait_for`) can only reference jobs within the same tenant, preventing cross-tenant leakage.
- **Singleton Keys**: `singleton_key` constraints are scoped per tenant, allowing different tenants to have active jobs with the same key simultaneously.
- **Worker Flexibility**: Workers can serve multiple tenants or be dedicated to specific tenants via the `tenant_ids` parameter in `get_work()`.
- **Tenant Filtering**: Operations like `get_work()` and `archive_cancelled_jobs()` accept optional `tenant_ids TEXT[]` parameter:
  - `NULL` or `'{}'`: Operate across all tenants
  - `'{tenant1}'`: Operate only on tenant1
  - `'{tenant1, tenant2}'`: Operate only on these tenants
- **Performance**: Tenant-aware indexes ensure efficient query performance even with millions of jobs across thousands of tenants.

### Multi-Tenant Example

```sql
-- Tenant A submits a job
SELECT * FROM pgwf.submit_job(
    p_tenant_id => 'acme_corp',
    p_job_id => 'job-123',
    p_worker_id => 'ingest-service',
    p_next_need => 'python.process'
);

-- Tenant B submits a job with the same job_id (allowed - different tenant)
SELECT * FROM pgwf.submit_job(
    p_tenant_id => 'globex_inc',
    p_job_id => 'job-123',  -- Same job_id, different tenant
    p_worker_id => 'ingest-service',
    p_next_need => 'python.process'
);

-- Multi-tenant worker gets work from all tenants
SELECT tenant_id, job_id, lease_id
FROM pgwf.get_work(
    p_worker_id => 'worker-1',
    p_worker_caps => ARRAY['python.process'],
    p_tenant_ids => NULL  -- All tenants
);

-- Single-tenant worker gets work from specific tenant
SELECT tenant_id, job_id, lease_id
FROM pgwf.get_work(
    p_worker_id => 'worker-2',
    p_worker_caps => ARRAY['python.process'],
    p_tenant_ids => ARRAY['acme_corp']  -- Only acme_corp
);
```

## Garden Variety Use

1. Have multiple workers waiting for work. Each worker polls get_work(). Workers may have different capabilities (e.g. one worker might run python, another might run llm on gpu, another might be a human-based review process)
2. An external actor submits a job, including it's immediate need as well as any dependencies it might have (wait for other jobs to complete, wait for a certain time, etc).
3. A worker that has the needed capability will receive a lease for the new job and begin processing it.
4. The worker processes the job as long as it has sufficient capabilities. As the worker processes that job, it will keep calling extend_lease() to keep the lease alive.
5. At some point, the worker encounters a requirement that it cannot complete (this step requires a massive GPU). At this point, teh worker will rescheudle the job, informing the workflow engine of the new need.
6. Another worker, that can satisfy that new need, will get a new lease for the job and work through the additional rquirements.
7. When the job is complete, the GPU worker calls complete_job() to mark the job as complete.

## Why pgwf?

- **Separate execution metadata from payload state** – pgwf stores orchestration facts (leases, dependencies, trace) plus a tiny JSON payload that can be tweaked via reschedules, while an external journal or blob store can hold immutable payload history and outputs. This mirrors double-entry accounting: pgwf keeps work moving forward, and the journal preserves every state mutation for replay. Keep coordination data small to minimize infrastructure requirements.
- **Single dependency** – Everything lives inside a schema; migrations are regular SQL, so operators reuse existing Postgres tooling. Store state in whatever way you want.
- **Deterministic leasing** – Visibility timeouts plus explicit `lease_id`s guarantee only one worker owns a job at a time and that resumes are idempotent.
- **Dependency-aware flow** – `wait_for` lets you fan out multiple child jobs, then automatically unblock the parent when all children finish.
- **Capability routing** – Workers advertise capabilities (e.g., “python”, “human-review”), and jobs hop between phases by updating `next_need`.
- **Singleton enforcement** – `singleton_key` ensures only one job for a logical entity (customer, invoice, run) can be leased simultaneously.
- **Observable & traceable** – Every mutation writes to `pgwf.jobs_trace` so you can rebuild timelines or audit operator actions.
- **Composable** – Functions can be called from stored procedures, application code, or CLI sessions.

## Key Capabilities

### Cancellation Lifecycle

Operators can call `pgwf.cancel_job` to mark in-flight or queued work for cancellation. Once a cancelled job's lease expires (or if it was `READY`/`PENDING_JOBS`), it transitions to the `CANCELLED` status so it no longer leases, emits notifications, or blocks dependent work from progressing. The `pgwf.archive_cancelled_jobs` function performs bulk archival of these rows, drops the cancelled job_ids from any `wait_for` arrays, and appends both per-job (`job_cancel_archived`) and aggregate (`job_cancel_archived_run`) trace events. Applications should invoke this function periodically (manually or via their own scheduler) to reclaim cancelled rows.

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

#### Alternate capability fallback

Optionally set `alternate_next_need` with `alternate_after_seconds`. While a job is READY and unleased, once `ready_since + alternate_after_seconds <= now()` the job’s **effective** capability pivots to `alternate_next_need`. The pivot is derived in `jobs_with_status` (column `effective_next_need`) so `get_work` routes using the active capability without mutating the stored `next_need`. No `NOTIFY` is emitted when the pivot occurs; workers relying on LISTEN should continue polling. Example submission:

```sql
SELECT pgwf.submit_job(
    p_tenant_id => 'tenant-a',
    p_job_id => 'doc-review',
    p_worker_id => 'router',
    p_next_need => 'human.review',
    p_alternate_next_need => 'llm.review',
    p_alternate_after_seconds => 300  -- fall back after 5 minutes
);
```

### Singleton keys

`singleton_key` is an optional mutex scope. If all “billing for customer-42” jobs share `singleton_key = 'customer-42'`, pgwf ensures only one job with that key holds a lease at any time. This prevents concurrent workflows from trampling shared resources without involving advisory locks or external coordination.
The key is set (or left NULL) when the job is first submitted and remains immutable; reschedule helpers do not accept a singleton parameter.

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

Because the two concerns are separate, you can replay or rehydrate workflows by reading the journal while pgwf keeps scheduling honest. pgwf never stores payload blobs. pgwf just stores pointer (`job_id`) back to the journal plus scheduling metadata, along with a small immutable `metadata` JSONB for job context.

## Inter-transaction Jobs

Because pgwf is implemented entirely inside Postgres, job creation can live inside the same transaction as your domain writes:

```sql
BEGIN;
INSERT INTO invoices (invoice_id, total_cents, status) VALUES ('inv-42', 12345, 'pending');
SELECT pgwf.submit_job('customer-123', 'inv-42', 'billing-service', 'invoice.collect', ARRAY[]::TEXT[]);
COMMIT;
```

```sql
BEGIN;
INSERT INTO invoices (invoice_id, total_cents, status) VALUES ('inv-42', 12345, 'completed');
SELECT pgwf.complete_job('customer-123', 'inv-42', 'lease1234', 'PROD;HUMAN-REVIEW;jim@oheir.org');
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
2. **Submit job** – The producer calls `pgwf.submit_job(job_id, worker_id, next_need, wait_for, metadata, singleton_key, available_at)` to register the work. Example:

    ```sql
    SELECT tenant_id, job_id
    FROM pgwf.submit_job(
        p_tenant_id     => 'customer-42',
        p_job_id        => 'job-123',
        p_worker_id     => 'ingest-service',
        p_next_need     => 'transcode.video',
        p_wait_for      => ARRAY['preflight-99'],
        p_metadata      => '{"source":"journal"}'::JSONB,
        p_singleton_key => 'video-777',
        p_available_at  => clock_timestamp()
    );
    ```
   The singleton key (if any) is fixed at submission time; subsequent reschedules do not accept or alter it.

3. **Workers poll** – Workers call `pgwf.get_work(worker_id, worker_caps, tenant_ids, lease_seconds, limit_jobs)` to lease jobs. When notifications are enabled they also `LISTEN pgwf.need.<capability>` to wake up instantly; otherwise they simply poll. Each lease returns full metadata plus a fresh `lease_id`. A "python" worker might perform ETL, while a "human-review" worker handles compliance later.
4. **Process + heartbeat** – While running, workers use `pgwf.extend_lease(tenant_id, job_id, lease_id, worker_id, additional_seconds)` to keep ownership. If they expect a long pause, they can reschedule themselves with a future `available_at`.
5. **Reschedule when blocked** – Workers mutate capability + dependencies via `pgwf.reschedule_job(...)`. Example: after splitting a video into 10 child jobs, the parent reschedules itself with `wait_for = ARRAY['child-1', ..., 'child-10']`. Each child completion removes its `job_id`; when the final one finishes, the parent becomes runnable automatically. Another example: a worker might reschedule itself with `next_need = 'human-review'` if it detects a compliance issue.
6. **Complete** – After writing the final payload back to the journal, the worker calls `pgwf.complete_job(tenant_id, job_id, lease_id, worker_id)`. pgwf archives the row, deletes the live copy, removes the job_id from any dependent `wait_for` arrays, and emits NOTIFY signals to wake listeners (if enabled).

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
