# jobs_with_status & jobs_friendly_status specifications

## Overview
- Introduce a raw status view (`pgwf.jobs_with_status`) that exposes every column from `pgwf.jobs` plus a derived `status` column.
- Layer a friendlier view (`pgwf.jobs_friendly_status`) on top that reduces the columns to the monitoring-centric subset requested earlier (job id, status, creation timestamp, pending jobs, sleep-until, worker id).

## `pgwf.jobs_with_status` definition
- Columns: `pgwf.jobs.*`, `status TEXT`.
- Status evaluation:
  1. `ACTIVE` when `lease_expires_at > clock_timestamp()`.
  2. `AWAITING_FUTURE` when `available_at > clock_timestamp()`.
  3. `PENDING_JOBS` when `array_length(wait_for, 1) > 0`.
  4. `READY` otherwise.
- Implementation outline:
  ```sql
  CREATE OR REPLACE VIEW pgwf.jobs_with_status AS
  SELECT
      j.*,
      CASE
          WHEN j.lease_expires_at > clock_timestamp() THEN 'ACTIVE'
          WHEN j.available_at > clock_timestamp() THEN 'AWAITING_FUTURE'
          WHEN COALESCE(array_length(j.wait_for, 1), 0) > 0 THEN 'PENDING_JOBS'
          ELSE 'READY'
      END AS status
  FROM pgwf.jobs j;
  ```

## `pgwf.jobs_friendly_status` definition
- Built directly on top of `pgwf.jobs_with_status`.
- Columns:
  - `job_id`
  - `status`
  - `creation_dt` (alias of `created_at`)
  - `pending_jobs` (non-empty `wait_for`, otherwise `NULL`)
  - `sleep_until` (`available_at` if status `AWAITING_FUTURE`, else `NULL`)
  - `worker_id` (`lease_id` if status `ACTIVE`, else `NULL`)
- Sample definition:
  ```sql
  CREATE OR REPLACE VIEW pgwf.jobs_friendly_status AS
  SELECT
      jws.job_id,
      jws.status,
      jws.created_at AS creation_dt,
      CASE WHEN jws.status = 'PENDING_JOBS' THEN jws.wait_for ELSE NULL END AS pending_jobs,
      CASE WHEN jws.status = 'AWAITING_FUTURE' THEN jws.available_at ELSE NULL END AS sleep_until,
      CASE WHEN jws.status = 'ACTIVE' THEN jws.lease_id ELSE NULL END AS worker_id
  FROM pgwf.jobs_with_status jws;
  ```

## Notes
- The status logic in both views must stay consistent and retain the priority ordering listed above.
- Exposing the raw view makes it easy to join additional data or debug without losing context.
- As part of adding these views, update existing functions to read from `jobs_with_status` instead of `pgwf.jobs` directly so they can rely on the derived `status` column:
  - `get_work` should only consider rows with `status = 'READY'`.
  - `complete_job`, `reschedule_job` and `extend_lease` should restrict to `status = 'ACTIVE'`.
  - Any future logic that needs to understand job readiness or leasing state should prefer the status column to duplicating comparisons.
