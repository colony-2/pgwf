# Internal helper extraction

Before introducing any new public functions, extract the shared logic that both the leased and unheld variants will use. All internal-only helpers must follow the `_snake_case` naming convention to signal they are not part of the public API.

## `_complete_locked_job` helper
- **Purpose**: encapsulate the completion pipeline (archive, delete, dependency unblocking, notifications, trace) so `pgwf.complete_job` and future `pgwf.complete_unheld_job` call the same routine.
- **Signature** (conceptual):
  ```sql
  CREATE OR REPLACE FUNCTION pgwf._complete_locked_job(
      p_locked_job pgwf.jobs%ROWTYPE,
      p_worker_id TEXT,
      p_trace_context JSONB DEFAULT '{}'::JSONB
  ) RETURNS BOOLEAN
  ```
- **Assumptions**:
  - Caller already ran `SELECT ... FOR UPDATE` against `pgwf.jobs_with_status` with the appropriate predicate (ACTIVE for leased, READY/PENDING for unheld) and supplied the exact `pgwf.jobs` row via `p_locked_job`.
  - Job has either an active lease (leased completion) or has expired/never leased (unheld).
- **Responsibilities**:
  - Insert the row into `pgwf.jobs_archive`.
  - Delete from `pgwf.jobs`.
  - Remove completed job ID from other jobs’ `wait_for` arrays and notify newly unblocked jobs whose `available_at <= now()`.
  - Emit the `job_finished` trace event when tracing is enabled; include `p_trace_context` merged into the existing payload so outer functions can document whether the completion was leased/unheld.
  - Return `TRUE` once completion succeeds.
- **Callers**:
  - Existing `pgwf.complete_job` should obtain/lock the row via `jobs_with_status` (`status = 'ACTIVE'`), then pass the locked row and `jsonb_build_object('lease_id', p_lease_id)` context to `_complete_locked_job`.
  - New `pgwf.complete_unheld_job` will lock via `status IN ('READY','PENDING_JOBS')` (+ availability checks) and pass context like `jsonb_build_object('completed_without_lease', TRUE)`.

## `_reschedule_locked_job` helper
- **Purpose**: unify the mutation path that rewrites a job’s `next_need`, `wait_for`, `singleton_key`, and `available_at`, clears leases, and emits notifications/traces.
- **Signature** (conceptual):
  ```sql
  CREATE OR REPLACE FUNCTION pgwf._reschedule_locked_job(
      p_locked_job pgwf.jobs%ROWTYPE,
      p_worker_id TEXT,
      p_next_need TEXT,
      p_wait_for TEXT[],
      p_singleton_key TEXT,
      p_available_at TIMESTAMPTZ
  ) RETURNS TABLE(job_id TEXT, next_need TEXT, wait_for TEXT[], available_at TIMESTAMPTZ)
  ```
- **Assumptions**:
  - Caller already validated and locked the job row via `pgwf.jobs_with_status` (ACTIVE for leased, READY-or-equivalent for unheld) and passed the original `pgwf.jobs` row as `p_locked_job`.
  - Wait-for normalization occurs inside the helper to keep both entry points consistent.
- **Responsibilities**:
  - Normalize `p_wait_for` via `pgwf.normalize_wait_for`.
  - Update the base row with the new values, clear `lease_id`, set `lease_expires_at = '-infinity'`, and set `available_at = COALESCE(p_available_at, clock_timestamp())`.
  - Send NOTIFY events when appropriate.
  - Emit the `'reschedule_job'` trace event with the full input/output payload, including whichever context (e.g., `lease_id`) the caller wants recorded.
  - Return the updated row via `RETURN NEXT`.
- **Callers**:
  - Existing `pgwf.reschedule_job` (leased) should lock via `status = 'ACTIVE'` and include `jsonb_build_object('lease_id', p_lease_id)` in the trace input.
- Future `pgwf.reschedule_unheld_job` will lock on available jobs (status not ACTIVE, `available_at <= now()`) and omit the lease context.

## `_lock_job_for_status` helper
- **Purpose**: centralize the `SELECT ... FOR UPDATE` logic that loads a job row with its current status so callers don’t repeat status filters and join clauses.
- **Signature** (conceptual):
  ```sql
  CREATE OR REPLACE FUNCTION pgwf._lock_job_for_status(
      p_job_id TEXT,
      p_allowed_status TEXT[],
      p_require_available BOOLEAN DEFAULT FALSE
  ) RETURNS pgwf.jobs%ROWTYPE
  ```
- **Behavior**:
  - Reads from `pgwf.jobs_with_status` filtering by `job_id = p_job_id` and `status = ANY(p_allowed_status)`.
  - When `p_require_available = TRUE`, also ensures `available_at <= clock_timestamp()`.
  - Returns the locked base-table row (via `FOR UPDATE` on `pgwf.jobs`), raising a descriptive error when no matching row is found.
- **Consumers**:
  - `pgwf.complete_job`, `pgwf.extend_lease`, `pgwf.reschedule_job` (status = ACTIVE).
  - Upcoming unheld helpers (status IN READY/PENDING, `p_require_available = TRUE`).

## `_emit_trace_event` helper
- **Purpose**: avoid duplicating the conditional `IF pgwf.is_trace_enabled() THEN INSERT ...` clause scattered across functions.
- **Signature** (conceptual):
  ```sql
  CREATE OR REPLACE FUNCTION pgwf._emit_trace_event(
      p_job_id TEXT,
      p_event_type TEXT,
      p_worker_id TEXT,
      p_input JSONB,
      p_output JSONB DEFAULT NULL
  ) RETURNS VOID
  ```
- **Behavior**:
  - Checks `pgwf.is_trace_enabled()` internally and performs the insert when enabled; otherwise it returns immediately.
  - Handles defaulting JSON arguments to `'{}'::JSONB` / `NULL`.
- **Consumers**: every function that currently emits trace rows (`submit_job`, `get_work`, `extend_lease`, `reschedule_job`, `complete_job`, plus future helpers).

## `_validate_active_lease` helper
- **Motivation**: `pgwf.extend_lease`, `pgwf.reschedule_job`, and `pgwf.complete_job` all repeat the pattern of ensuring a job is actively leased by a given lease ID.
- **Concept**:
  - A helper that accepts `(p_job_id, p_lease_id, p_expected_status TEXT DEFAULT 'ACTIVE')` and uses `_lock_job_for_status` internally to verify the lease is still valid. It would return the locked row (or even just the lease expiration) so callers can proceed without repeating the same conditionals.
  - Extend/reschedule could also share logic for updating `lease_expires_at` (computing `clock_timestamp() + make_interval(...)`).

These additions should reduce the duplication between `extend_lease`, `reschedule_job`, `complete_job`, and their unheld counterparts while keeping the public API unchanged.
