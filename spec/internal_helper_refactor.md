# internal helper refactor

`pgwf.sql` currently repeats the same locking, validation, archival, notification, and tracing blocks across `get_work`, `extend_lease`, `reschedule_job`, and `complete_job`. This spec introduces a set of internal-only helpers (all prefixed with `_`) that consolidate those behaviors while keeping the existing public API unchanged.

## Goals
- Remove copy/pasted `SELECT ... FOR UPDATE` clauses that differ only by status predicates.
- Ensure archival, dependency cleanup, and notification logic always run in the same order regardless of entry point.
- Centralize tracing and notification toggles so adding new events does not require editing every caller.
- Provide common lease validation and wait-for normalization so logic errors remain in one place.

## `_lock_job_for_status`
- **Purpose**: Single point for locking a job row by querying `pgwf.jobs_with_status` with a single `status` filter.
- **Signature**:
  ```sql
  CREATE OR REPLACE FUNCTION pgwf._lock_job_for_status(
      p_job_id TEXT,
      p_status TEXT,
      p_expected_lease_id TEXT DEFAULT NULL,
      p_missing_message TEXT DEFAULT NULL
  ) RETURNS pgwf.jobs_with_status
  ```
- **Behavior**:
  - SELECT and `FOR UPDATE` the matching `jobs_with_status` row, applying the optional `lease_id` predicate.
  - Return the full view row (including `status`) so callers can pass it directly to helpers without extra reconstruction.
- **Consumers**: `pgwf.extend_lease`, `pgwf.reschedule_job`, `pgwf.complete_job` (pass `'ACTIVE'` plus the caller’s lease ID/message), and future unheld helpers (pass `'READY'`/`'PENDING_JOBS'`).

## `_emit_trace_event`
- **Observation**: Every function repeats the `IF pgwf.is_trace_enabled()` block with similar `INSERT` syntax.
- **Signature**:
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
  - Return immediately when tracing is disabled.
  - Insert into `pgwf.jobs_trace` when enabled, defaulting omitted JSON arguments to `'{}'::JSONB`.
- **Consumers**: `submit_job`, `get_work`, `extend_lease`, `reschedule_job`, `complete_job`, and future helpers that emit events.

## `_notify_need`
- **Observation**: `submit_job`, `reschedule_job`, `complete_job`, and the dependency cleanup loop all run the same `IF pgwf.is_notify_enabled()` / `pg_notify` code.
- **Signature**:
  ```sql
  CREATE OR REPLACE FUNCTION pgwf._notify_need(
      p_next_need TEXT,
      p_job_id TEXT
  ) RETURNS VOID
  ```
- **Behavior**:
  - Exit when notify is disabled.
  - Execute `pg_notify(format('pgwf.need.%s', p_next_need), p_job_id)` when enabled.
- **Consumers**: anywhere that currently wants to wake sleepers after a job becomes runnable (submit/reschedule completion loops) plus future scheduling helpers.

## `_archive_and_delete_job`
- **Observation**: `complete_job` (and upcoming unheld completion) perform the same archive insert and delete.
- **Signature**:
  ```sql
  CREATE OR REPLACE FUNCTION pgwf._archive_and_delete_job(
      p_locked_job pgwf.jobs_with_status
  ) RETURNS pgwf.jobs_archive
  ```
- **Behavior**:
  - Insert the locked job into `jobs_archive` (mirroring `complete_job`’s columns).
  - Delete the original `pgwf.jobs` row via PK + lease safety.
  - Return the archived row for trace payloads.
- **Consumers**: `_complete_locked_job` described below and any administrative cleanup routines.

## `_update_waiters_for_completion`
- **Observation**: The `FOR ... LOOP` inside `complete_job` that removes a finished job from `wait_for` arrays and notifies unblocked jobs will also be required by `complete_unheld_job`.
- **Signature**:
  ```sql
  CREATE OR REPLACE FUNCTION pgwf._update_waiters_for_completion(
      p_completed_job_id TEXT
  ) RETURNS SETOF RECORD -- columns: job_id TEXT, next_need TEXT, became_unblocked BOOLEAN
  ```
- **Behavior**:
  - Run the existing `UPDATE ... RETURNING` statement and emit each mutated row once with a `became_unblocked` flag (available + wait_for empty).
  - Call `_notify_need` internally for rows that become runnable now.
- **Consumers**: `_complete_locked_job`, future administrative bulk completion helpers.

## `_complete_locked_job`
- **Purpose**: unify the completion path once a row is already locked.
- **Signature**:
  ```sql
  CREATE OR REPLACE FUNCTION pgwf._complete_locked_job(
      p_locked_job pgwf.jobs_with_status,
      p_worker_id TEXT,
      p_trace_context JSONB DEFAULT '{}'::JSONB
  ) RETURNS BOOLEAN
  ```
- **Behavior**:
  1. Call `_archive_and_delete_job`.
  2. Invoke `_update_waiters_for_completion`.
  3. Fire `_emit_trace_event` with `'job_finished'` plus `p_trace_context` merged into the `input_data`.
  4. Return TRUE when done.
- **Consumers**: `pgwf.complete_job` after `_lock_job_for_status(... 'ACTIVE_ONLY', lease_id ...)`; future `pgwf.complete_unheld_job`.

## `_reschedule_locked_job`
- **Purpose**: share the mutation pipeline across leased and unheld reschedules.
- **Signature**:
  ```sql
  CREATE OR REPLACE FUNCTION pgwf._reschedule_locked_job(
      p_locked_job pgwf.jobs_with_status,
      p_worker_id TEXT,
      p_next_need TEXT,
      p_wait_for TEXT[],
      p_singleton_key TEXT,
      p_available_at TIMESTAMPTZ,
      p_trace_context JSONB DEFAULT '{}'::JSONB
  ) RETURNS TABLE(job_id TEXT, next_need TEXT, wait_for TEXT[], available_at TIMESTAMPTZ)
  ```
- **Behavior**:
  - Normalize `p_wait_for` via `pgwf.normalize_wait_for`.
  - Update `pgwf.jobs` with new values, clear any lease, and ensure `available_at := COALESCE(p_available_at, clock_timestamp())`.
  - Call `_notify_need` after every rewrite so the appropriate listeners wake up regardless of current availability.
  - Emit the `reschedule_job` trace event via `_emit_trace_event`, merging `p_trace_context` so callers can include lease IDs or unheld metadata.
- **Consumers**: current `pgwf.reschedule_job` (leased path) and the future `pgwf.reschedule_unheld_job`.

## Rollout plan
1. Implement `_lock_job_for_status`; update the public functions that currently inline the lock predicates to depend on it (pass the lease id when needed).
2. Introduce `_emit_trace_event` and `_notify_need`, refactoring every existing caller (safe because both helpers are pure additions).
3. Build `_archive_and_delete_job`, `_update_waiters_for_completion`, and `_complete_locked_job`; switch `pgwf.complete_job` to call them while maintaining its observable behavior.
4. Build `_reschedule_locked_job`; migrate `pgwf.reschedule_job` to it.
5. After the shared helpers land, layer in `complete_unheld_job` and `reschedule_unheld_job` per their separate specs with minimal duplicated logic.

## Risks / mitigations
- **Risk**: Hidden behavioral drift while moving large SQL blocks. *Mitigation*: Add targeted regression tests in `test/pgwf_test.go` for lease errors, trace toggles, and dependency notifications before and after refactor.
- **Risk**: Helper signatures may expose `pgwf.jobs%ROWTYPE`, which can change. *Mitigation*: Keep helpers internal only and document their usage so renames happen atomically.
- **Risk**: Additional function call overhead. *Mitigation*: PL/pgSQL inline calls are cheap relative to the IO work already performed; we gain maintainability and reduce bug risk across API expansions.
