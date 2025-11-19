BEGIN;

CREATE SCHEMA IF NOT EXISTS pgwf;

SET search_path = pgwf, public;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SEQUENCE IF NOT EXISTS pgwf.jobs_trace_id_seq;

CREATE TABLE IF NOT EXISTS pgwf.jobs (
    job_id TEXT PRIMARY KEY,
    next_need TEXT NOT NULL,
    wait_for TEXT[] NOT NULL DEFAULT '{}'::TEXT[],
    singleton_key TEXT,
    available_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    lease_id TEXT,
    lease_expires_at TIMESTAMPTZ NOT NULL DEFAULT '-infinity',
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS pgwf.jobs_archive (
    archived_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    job_id TEXT PRIMARY KEY,
    next_need TEXT NOT NULL,
    wait_for TEXT[] NOT NULL DEFAULT '{}'::TEXT[],
    singleton_key TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    lease_id TEXT
);

CREATE TABLE IF NOT EXISTS pgwf.jobs_trace (
    trace_id BIGINT PRIMARY KEY DEFAULT nextval('pgwf.jobs_trace_id_seq'),
    job_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    event_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    input_data JSONB NOT NULL,
    output_data JSONB
);

CREATE OR REPLACE FUNCTION pgwf.is_trace_enabled()
RETURNS BOOLEAN
LANGUAGE sql
AS $$
    SELECT TRUE;
$$;

CREATE OR REPLACE FUNCTION pgwf.set_trace(enabled BOOLEAN)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
BEGIN
    IF enabled IS NULL THEN
        RAISE EXCEPTION 'enabled flag cannot be NULL';
    END IF;

    EXECUTE format(
        'CREATE OR REPLACE FUNCTION pgwf.is_trace_enabled() RETURNS BOOLEAN LANGUAGE plpgsql AS %L',
        CASE WHEN enabled THEN 'BEGIN RETURN TRUE; END;' ELSE 'BEGIN RETURN FALSE; END;' END
    );

    RETURN enabled;
END;
$$;

CREATE OR REPLACE FUNCTION pgwf.is_notify_enabled()
RETURNS BOOLEAN
LANGUAGE sql
AS $$
    SELECT FALSE;
$$;

CREATE OR REPLACE FUNCTION pgwf.set_notify(enabled BOOLEAN)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
BEGIN
    IF enabled IS NULL THEN
        RAISE EXCEPTION 'enabled flag cannot be NULL';
    END IF;

    EXECUTE format(
        'CREATE OR REPLACE FUNCTION pgwf.is_notify_enabled() RETURNS BOOLEAN LANGUAGE plpgsql AS %L',
        CASE WHEN enabled THEN 'BEGIN RETURN TRUE; END;' ELSE 'BEGIN RETURN FALSE; END;' END
    );

    RETURN enabled;
END;
$$;

CREATE OR REPLACE FUNCTION pgwf.normalize_wait_for(p_wait_for TEXT[])
RETURNS TEXT[]
LANGUAGE plpgsql
AS $$
DECLARE
    v_input TEXT[] := COALESCE(p_wait_for, ARRAY[]::TEXT[]);
    v_clean TEXT[];
    v_missing TEXT[];
BEGIN
    IF array_length(v_input, 1) IS NULL THEN
        RETURN ARRAY[]::TEXT[];
    END IF;

    WITH ordered AS (
        SELECT value AS job_id, ord
        FROM unnest(v_input) WITH ORDINALITY AS w(value, ord)
        WHERE value IS NOT NULL
    )
    SELECT
        COALESCE(array_agg(o.job_id ORDER BY ord)
                 FILTER (WHERE j.job_id IS NOT NULL), ARRAY[]::TEXT[]),
        array_agg(o.job_id ORDER BY ord)
            FILTER (WHERE j.job_id IS NULL AND ja.job_id IS NULL)
    INTO v_clean, v_missing
    FROM ordered o
    LEFT JOIN pgwf.jobs j ON j.job_id = o.job_id
    LEFT JOIN pgwf.jobs_archive ja ON ja.job_id = o.job_id;

    IF v_missing IS NOT NULL THEN
        RAISE EXCEPTION 'wait_for references unknown jobs: %', v_missing;
    END IF;

    RETURN v_clean;
END;
$$;

CREATE OR REPLACE FUNCTION pgwf.submit_job(
    p_job_id TEXT,
    p_worker_id TEXT,
    p_next_need TEXT,
    p_wait_for TEXT[] DEFAULT '{}'::TEXT[],
    p_singleton_key TEXT DEFAULT NULL,
    p_available_at TIMESTAMPTZ DEFAULT clock_timestamp()
)
RETURNS TABLE(job_id TEXT, next_need TEXT, wait_for TEXT[], available_at TIMESTAMPTZ)
LANGUAGE plpgsql
AS $$
DECLARE
    v_wait_for TEXT[];
    v_effective_available TIMESTAMPTZ := COALESCE(p_available_at, clock_timestamp());
BEGIN
    IF EXISTS (SELECT 1 FROM pgwf.jobs_archive ja WHERE ja.job_id = p_job_id) THEN
        RAISE EXCEPTION 'job_id % has already completed and cannot be resubmitted', p_job_id;
    END IF;

    v_wait_for := pgwf.normalize_wait_for(p_wait_for);

    INSERT INTO pgwf.jobs (job_id, next_need, wait_for, singleton_key, available_at)
    VALUES (p_job_id, p_next_need, v_wait_for, p_singleton_key, v_effective_available)
    RETURNING pgwf.jobs.job_id, pgwf.jobs.next_need, pgwf.jobs.wait_for, pgwf.jobs.available_at
    INTO job_id, next_need, wait_for, available_at;

    IF pgwf.is_notify_enabled() THEN
        PERFORM pg_notify(format('pgwf.need.%s', next_need), job_id);
    END IF;

    IF pgwf.is_trace_enabled() THEN
        INSERT INTO pgwf.jobs_trace (job_id, event_type, worker_id, input_data, output_data)
        VALUES (
            p_job_id,
            'job_submitted',
            p_worker_id,
            jsonb_build_object(
                'job_id', p_job_id,
                'worker_id', p_worker_id,
                'next_need', p_next_need,
                'wait_for', v_wait_for,
                'singleton_key', p_singleton_key,
                'available_at', v_effective_available
            ),
            jsonb_build_object(
                'job_id', job_id,
                'next_need', next_need,
                'wait_for', wait_for,
                'available_at', available_at
            )
        );
    END IF;

    RETURN NEXT;
END;
$$;

CREATE OR REPLACE FUNCTION pgwf.get_work(
    p_worker_id TEXT,
    p_worker_caps TEXT[],
    p_lease_seconds INTEGER DEFAULT 60,
    p_limit_jobs INTEGER DEFAULT 1
)
RETURNS TABLE(
    job_id TEXT,
    lease_id TEXT,
    next_need TEXT,
    singleton_key TEXT,
    wait_for TEXT[],
    available_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_caps TEXT[] := p_worker_caps;
    v_now TIMESTAMPTZ := clock_timestamp();
    v_expires TIMESTAMPTZ;
    v_count INTEGER := 0;
    v_cap TEXT;
BEGIN
    IF v_caps IS NULL OR array_length(v_caps, 1) = 0 THEN
        RAISE EXCEPTION 'worker_caps cannot be empty';
    END IF;

    IF p_lease_seconds IS NULL OR p_lease_seconds <= 0 THEN
        RAISE EXCEPTION 'lease_seconds must be positive';
    END IF;

    IF p_limit_jobs IS NULL OR p_limit_jobs <= 0 THEN
        RAISE EXCEPTION 'limit_jobs must be positive';
    END IF;

    v_expires := v_now + make_interval(secs => p_lease_seconds);

    FOR job_id, lease_id, next_need, singleton_key, wait_for, available_at, lease_expires_at IN
        WITH candidates AS (
            SELECT j.*
            FROM pgwf.jobs j
            WHERE j.available_at <= v_now
              AND (j.lease_id IS NULL OR j.lease_expires_at <= v_now)
              AND COALESCE(array_length(j.wait_for, 1), 0) = 0
              AND j.next_need = ANY(v_caps)
              AND (
                  j.singleton_key IS NULL OR NOT EXISTS (
                      SELECT 1
                      FROM pgwf.jobs other
                      WHERE other.singleton_key = j.singleton_key
                        AND other.lease_id IS NOT NULL
                        AND other.lease_expires_at > v_now
                  )
              )
            ORDER BY j.created_at ASC
            LIMIT p_limit_jobs
            FOR UPDATE SKIP LOCKED
        )
        UPDATE pgwf.jobs j
        SET
            lease_id = gen_random_uuid()::TEXT,
            lease_expires_at = v_expires
        FROM candidates c
        WHERE j.job_id = c.job_id
        RETURNING j.job_id, j.lease_id, j.next_need, j.singleton_key, j.wait_for, j.available_at, j.lease_expires_at
    LOOP
        v_count := v_count + 1;

        IF pgwf.is_trace_enabled() THEN
            INSERT INTO pgwf.jobs_trace (job_id, event_type, worker_id, input_data, output_data)
            VALUES (
                job_id,
                'job_retrieved',
                p_worker_id,
                jsonb_build_object(
                    'worker_id', p_worker_id,
                    'worker_caps', v_caps,
                    'lease_seconds', p_lease_seconds,
                    'limit_jobs', p_limit_jobs
                ),
                jsonb_build_object(
                    'lease_id', lease_id,
                    'lease_expires_at', lease_expires_at
                )
            );
        END IF;

        RETURN NEXT;
    END LOOP;

    IF v_count = 0 AND pgwf.is_notify_enabled() THEN
        FOREACH v_cap IN ARRAY v_caps LOOP
            EXIT WHEN v_cap IS NULL;
            EXECUTE 'LISTEN ' || quote_ident('pgwf.need.' || v_cap);
        END LOOP;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION pgwf.extend_lease(
    p_job_id TEXT,
    p_lease_id TEXT,
    p_worker_id TEXT,
    p_additional_seconds INTEGER DEFAULT 60
)
RETURNS TIMESTAMPTZ
LANGUAGE plpgsql
AS $$
DECLARE
    v_current TIMESTAMPTZ;
    v_new TIMESTAMPTZ;
BEGIN
    IF p_additional_seconds IS NULL OR p_additional_seconds <= 0 THEN
        RAISE EXCEPTION 'additional_seconds must be positive';
    END IF;

    SELECT lease_expires_at INTO v_current
    FROM pgwf.jobs
    WHERE job_id = p_job_id AND lease_id = p_lease_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'active lease not found for job %', p_job_id;
    END IF;

    IF v_current <= clock_timestamp() THEN
        RAISE EXCEPTION 'lease for job % has expired', p_job_id;
    END IF;

    v_new := clock_timestamp() + make_interval(secs => p_additional_seconds);

    UPDATE pgwf.jobs
    SET lease_expires_at = v_new
    WHERE job_id = p_job_id AND lease_id = p_lease_id;

    IF pgwf.is_trace_enabled() THEN
        INSERT INTO pgwf.jobs_trace (job_id, event_type, worker_id, input_data, output_data)
        VALUES (
            p_job_id,
            'lease_extended',
            p_worker_id,
            jsonb_build_object(
                'job_id', p_job_id,
                'lease_id', p_lease_id,
                'additional_seconds', p_additional_seconds
            ),
            jsonb_build_object(
                'previous_expires_at', v_current,
                'new_expires_at', v_new
            )
        );
    END IF;

    RETURN v_new;
END;
$$;

CREATE OR REPLACE FUNCTION pgwf.reschedule_job(
    p_job_id TEXT,
    p_lease_id TEXT,
    p_worker_id TEXT,
    p_next_need TEXT,
    p_wait_for TEXT[] DEFAULT '{}'::TEXT[],
    p_singleton_key TEXT DEFAULT NULL,
    p_available_at TIMESTAMPTZ DEFAULT clock_timestamp()
)
RETURNS TABLE(job_id TEXT, next_need TEXT, wait_for TEXT[], available_at TIMESTAMPTZ)
LANGUAGE plpgsql
AS $$
DECLARE
    v_wait_for TEXT[];
    v_available_at TIMESTAMPTZ := COALESCE(p_available_at, clock_timestamp());
BEGIN
    v_wait_for := pgwf.normalize_wait_for(p_wait_for);

    PERFORM 1
    FROM pgwf.jobs j
    WHERE j.job_id = p_job_id AND j.lease_id = p_lease_id AND j.lease_expires_at > clock_timestamp()
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'job % is not currently leased with lease %', p_job_id, p_lease_id;
    END IF;

    UPDATE pgwf.jobs
    SET next_need = p_next_need,
        wait_for = v_wait_for,
        singleton_key = p_singleton_key,
        available_at = v_available_at,
        lease_id = NULL,
        lease_expires_at = '-infinity'
    WHERE pgwf.jobs.job_id = p_job_id
    RETURNING pgwf.jobs.job_id, pgwf.jobs.next_need, pgwf.jobs.wait_for, pgwf.jobs.available_at
    INTO job_id, next_need, wait_for, available_at;

    IF pgwf.is_notify_enabled() THEN
        PERFORM pg_notify(format('pgwf.need.%s', next_need), job_id);
    END IF;

    IF pgwf.is_trace_enabled() THEN
        INSERT INTO pgwf.jobs_trace (job_id, event_type, worker_id, input_data, output_data)
        VALUES (
            p_job_id,
            'reschedule_job',
            p_worker_id,
            jsonb_build_object(
                'job_id', p_job_id,
                'lease_id', p_lease_id,
                'next_need', p_next_need,
                'wait_for', v_wait_for,
                'singleton_key', p_singleton_key,
                'available_at', v_available_at
            ),
            jsonb_build_object(
                'job_id', job_id,
                'next_need', next_need,
                'wait_for', wait_for,
                'available_at', available_at
            )
        );
    END IF;

    RETURN NEXT;
END;
$$;

CREATE OR REPLACE FUNCTION pgwf.complete_job(
    p_job_id TEXT,
    p_lease_id TEXT,
    p_worker_id TEXT
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    v_job pgwf.jobs%ROWTYPE;
    v_archive pgwf.jobs_archive%ROWTYPE;
    v_row RECORD;
BEGIN
    SELECT * INTO v_job
    FROM pgwf.jobs
    WHERE job_id = p_job_id AND lease_id = p_lease_id AND lease_expires_at > clock_timestamp()
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'job % is not actively leased by %', p_job_id, p_lease_id;
    END IF;

    INSERT INTO pgwf.jobs_archive (job_id, next_need, wait_for, singleton_key, created_at, lease_id)
    VALUES (v_job.job_id, v_job.next_need, v_job.wait_for, v_job.singleton_key, v_job.created_at, v_job.lease_id)
    RETURNING * INTO v_archive;

    DELETE FROM pgwf.jobs WHERE job_id = v_job.job_id AND lease_id = v_job.lease_id;

    FOR v_row IN
        UPDATE pgwf.jobs
        SET wait_for = array_remove(wait_for, v_job.job_id)
        WHERE v_job.job_id = ANY(wait_for)
        RETURNING job_id, next_need, available_at, (COALESCE(array_length(wait_for, 1), 0) = 0) AS now_unblocked
    LOOP
        IF v_row.now_unblocked AND v_row.available_at <= clock_timestamp() THEN
            IF pgwf.is_notify_enabled() THEN
                PERFORM pg_notify(format('pgwf.need.%s', v_row.next_need), v_row.job_id);
            END IF;
        END IF;
    END LOOP;

    IF pgwf.is_trace_enabled() THEN
        INSERT INTO pgwf.jobs_trace (job_id, event_type, worker_id, input_data, output_data)
        VALUES (
            p_job_id,
            'job_finished',
            p_worker_id,
            jsonb_build_object(
                'job_id', p_job_id,
                'lease_id', p_lease_id
            ),
            jsonb_build_object('archived_row', to_jsonb(v_archive))
        );
    END IF;

    RETURN TRUE;
END;
$$;

COMMIT;
