# Job Cancellation Specification

## Overview
- Introduce a first-class cancellation lifecycle so jobs can be explicitly retired before running, extending the status model defined in `pgwf.jobs_with_status` (`pgwf.sql:42-61`) without disturbing the rest of the leasing API surface.
- Preserve existing leasing semantics for in-flight work—workers may finish or release a job they already hold—but ensure cancelled jobs never re-enter the READY queue or emit capability notifications afterward.
- Treat cancellation as an alternative terminal path that still archives metadata, unblocks dependents, and participates in trace logging alongside the current completion flow (`pgwf.sql:216-274`).
- Keep the change scoped to SQL schema/functions plus regression coverage in the Go harness (`test/pgwf_test.go`); client libraries remain pure SQL consumers.

## Schema Changes
- Extend `pgwf.jobs` with `cancel_requested BOOLEAN NOT NULL DEFAULT FALSE`, `cancel_requested_by TEXT`, and `cancel_requested_at TIMESTAMPTZ` so we can durably mark a job for cancellation and capture who/when the request was made (`pgwf.sql:11-20`).
- Mirror the three columns in `pgwf.jobs_archive` so the archive records whether a job finished normally or via cancellation and when that decision was recorded (`pgwf.sql:22-30`).
- Enable the `pg_cron` extension alongside `pgcrypto` so pgwf installations can schedule recurring maintenance work without extra infrastructure.
- Update `pgwf.jobs_with_status` so cancelled-but-not-active rows report `status = 'CANCELLED'` ahead of the READY/PENDING/AWAITING cases, while actively leased rows continue to report `ACTIVE` until their lease expires; expose `cancel_requested_at` in the select list for downstream consumers.
- Update `pgwf.jobs_friendly_status` to include the new `CANCELLED` state (with `cancelled_at`/`cancelled_by` columns) so dashboards can display who issued the cancel request (`pgwf.sql:53-61`).

## Cancel Flow
- Add `pgwf.cancel_job(p_job_id TEXT, p_worker_id TEXT, p_reason TEXT DEFAULT NULL)` that locks the job row `FOR UPDATE`, validates it still exists in `pgwf.jobs`, sets the cancel columns, and returns the row so callers can see whether it was already marked.
- If the job is already archived, raise the same “job already completed” error that `submit_job` uses (`pgwf.sql:374-424`); if it is already cancelled, treat the call as idempotent and simply emit a trace event.
- For ACTIVE jobs, do not revoke the lease; we only toggle the cancel columns and let the existing lease expire or the worker finish voluntarily.
- Teach `pgwf.extend_lease` (`pgwf.sql:520-568`), `pgwf.reschedule_job`, and `pgwf.reschedule_unheld_job` to raise `job %s is cancelled and cannot be extended/rescheduled` when `cancel_requested = TRUE`, preventing cancelled work from re-entering the queue or extending visibility time.
- Ensure `cancel_job` never clears `wait_for` or singleton metadata; the job simply transitions to CANCELLED once it loses its lease, and any downstream notifications are suppressed until archival.

## Archival Flow
- Implement `pgwf.archive_cancelled_jobs(p_worker_id TEXT, p_limit INTEGER DEFAULT 100)` that selects cancelled rows whose `lease_expires_at <= clock_timestamp()` (`status = 'CANCELLED'`) with `FOR UPDATE SKIP LOCKED`, capped by `p_limit`.
- Archive in bulk: insert the selected rows into `pgwf.jobs_archive` with a single `INSERT ... SELECT`, delete them from `pgwf.jobs` with one `DELETE ... WHERE job_id = ANY(...)`, and remove their job_ids from dependent `wait_for` arrays with a set-based update to keep latency low. Emit the per-job trace entries via one `INSERT INTO pgwf.jobs_trace ... SELECT ... FROM archived_rows` so tracing stays consistent without row-by-row loops.
- Return only the number of archived jobs (an integer), raising if no worker_id is supplied (mirrors existing functions). Include that count in the `'job_cancel_archived'` trace event so operators can audit sweep volume without per-row entries, and collect aggregate metrics (duration, limit, count) for observability.
- When archiving, populate the new cancel columns in `pgwf.jobs_archive`, and store an output JSON blob in the trace event with the aggregate metadata for auditing.
- Document that operators should invoke `archive_cancelled_jobs` periodically (or after issuing `cancel_job`) to reclaim rows; the function is idempotent because it skips jobs once they leave the live table.
- Use `pg_cron` to create a scheduled job (e.g., `SELECT cron.schedule('pgwf_archive_cancelled', '* * * * *', $$SELECT pgwf.archive_cancelled_jobs('pgwf-cron', 500);$$)`) that runs every 60 seconds, ensuring cancelled jobs are archived automatically even if no external sweeper is deployed.

## Trace & Notifications
- Emit a `'job_cancel_requested'` event from `cancel_job` via `_emit_trace_event` (`pgwf.sql:120-175`), recording `job_id`, `worker_id`, optional `p_reason`, and whether the job was ACTIVE at request time.
- During `archive_cancelled_jobs`, insert the per-job `'job_cancel_archived'` trace records with a single bulk insert so every archived job still produces its own audit row without iterative function calls. Emit an aggregate `'job_cancel_archived_run'` event (also via `_emit_trace_event`) only when the sweep actually archived one or more jobs; include the total count, limit, and worker id in that payload.
- Update `_update_waiters_for_completion` to skip `_notify_need` for rows that now have `cancel_requested = TRUE`, satisfying the requirement that cancelled jobs never trigger “new work” notifications even if their dependencies finish (`pgwf.sql:216-236`).
- Leave `_notify_need` unchanged, but add a guard in `submit_job`/`_reschedule_locked_job` to only notify when `cancel_requested = FALSE`.
- Extend the README to describe the CANCELLED status, both new functions, and the fact that tracing must be enabled to see cancel records.

## Testing & Docs
- Add table-driven tests in `test/pgwf_test.go` that: (1) submit a job, cancel it, ensure `jobs_with_status` reports `CANCELLED`, and verify `pgwf.get_work` never returns it; (2) assert `extend_lease` fails for a cancelled lease; (3) ensure cancelling a `PENDING_JOBS` job suppresses `pg_notify` when its dependency completes; (4) run `archive_cancelled_jobs` and confirm the row lands in `jobs_archive` with cancel metadata while dependents unblock.
- Add a tracing test that enables tracing, calls `cancel_job`, and verifies two trace rows (`job_cancel_requested`, `job_cancel_archived`) appear with the expected worker id.
- Update README tables/descriptions for the new schema fields, the CANCELLED status, the new SQL entry points, and the operational guidance for running `archive_cancelled_jobs`.
- Regenerate `pgwf_embed.go` so the embedded SQL matches the new schema.
