# complete_unheld_job specification

## Overview
- Add a helper function that lets a worker finish a job that is not currently leased.
- The function mirrors `pgwf.complete_job`’s behavior (archive row, remove job, update waiters, notifications, trace) but keys off `job_id` only.
- Used for jobs that never acquired a lease or whose lease expired and the worker still wants to mark them complete.

## Function signature
```sql
CREATE OR REPLACE FUNCTION pgwf.complete_unheld_job(
    p_job_id   TEXT,
    p_worker_id TEXT
) RETURNS BOOLEAN
```

## Behavior
1. Lock the job row from `pgwf.jobs_with_status` with `FOR UPDATE` ensuring the row exists, `lease_expires_at <= clock_timestamp()`, and `available_at <= clock_timestamp()` (effectively `status IN ('READY','PENDING_JOBS')`). If not found raise `job % is not available to complete without a lease`.
2. If the job **is** leased but the lease has expired (even if `lease_id` remains populated), treat it like available.
3. After successful lock, pass the locked row to `pgwf._complete_locked_job` along with the worker ID and `jsonb_build_object('completed_without_lease', TRUE)` context so the shared helper performs the archive/delete/notify/trace steps.
4. Return the helper’s boolean result back to the caller.

## Notes / open questions
- Do we want to allow completing jobs whose lease has expired but `lease_id` is still set? (Currently spec chooses **yes** so workers can recover stranded jobs.)
- Update `pgwf.complete_job` to include `worker_id` inside the `job_finished` trace input payload so both functions report the worker that finished the job.
- Introduce an internal helper, e.g., `pgwf._complete_locked_job(v_job pgwf.jobs%ROWTYPE, p_worker_id TEXT, p_context JSONB DEFAULT NULL) RETURNS BOOLEAN`, that assumes the caller already `FOR UPDATE` locked the target row (either ACTIVE or unheld). This helper encapsulates archiving, dependency cleanup, notify, and trace emission (with `p_context` merged into the trace input). Existing `pgwf.complete_job` and the new `pgwf.complete_unheld_job` should both call this helper after they acquire their respective lock predicates.

# reschedule_unheld_job specification

## Overview
- Add a sibling helper to `pgwf.reschedule_job` that lets a worker reschedule a job that is currently available (no active lease).
- Enables repairing or tweaking jobs that timed out or were never leased by updating `next_need`, `wait_for`, `singleton_key`, and `available_at`.

## Function signature
```sql
CREATE OR REPLACE FUNCTION pgwf.reschedule_unheld_job(
    p_job_id TEXT,
    p_worker_id TEXT,
    p_next_need TEXT,
    p_wait_for TEXT[] DEFAULT '{}'::TEXT[],
    p_singleton_key TEXT DEFAULT NULL,
    p_available_at TIMESTAMPTZ DEFAULT clock_timestamp()
)
RETURNS TABLE(job_id TEXT, next_need TEXT, wait_for TEXT[], available_at TIMESTAMPTZ)
```

## Behavior
1. Lock the job from `pgwf.jobs_with_status` with `FOR UPDATE` verifying it exists, `lease_expires_at <= clock_timestamp()`, and `available_at <= clock_timestamp()` (i.e., not ACTIVE and ready time already reached). Raise `job % is not available to reschedule without a lease` if the row fails those checks.
2. Normalize `p_wait_for` with `pgwf.normalize_wait_for`, and set `v_available_at := COALESCE(p_available_at, clock_timestamp())`.
3. After successful lock, call `pgwf._reschedule_locked_job` with the row plus the supplied parameters so the helper handles normalization, updates, notifications, and tracing.
4. Return the helper’s result set.

## Notes
- Gives operators a symmetric tool to reschedule jobs that have gone idle without needing to obtain a lease first.
- Consider whether to include the existing `lease_id` (if any) inside the trace `input_data` for debugging even though the lease has expired; current plan omits it for clarity.
- Add a mirrored helper, `pgwf._reschedule_locked_job(v_job pgwf.jobs%ROWTYPE, p_worker_id TEXT, p_next_need TEXT, p_wait_for TEXT[], p_singleton_key TEXT, p_available_at TIMESTAMPTZ) RETURNS TABLE(...)`, that assumes the row is `FOR UPDATE` locked and handles the normalization + update + notifications + trace logic. Both `pgwf.reschedule_job` (leases) and `pgwf.reschedule_unheld_job` (available jobs) should delegate to it after validating their distinct status conditions.
