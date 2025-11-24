package pgwftest

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	pq "github.com/lib/pq"
)

var (
	testDB      *sql.DB
	embedded    *embeddedpostgres.EmbeddedPostgres
	testConnStr string
)

func TestMain(m *testing.M) {
	baseDir, err := os.MkdirTemp("", "pgwf-embed-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(baseDir)

	port := 15433
	if override := os.Getenv("PGWF_TEST_PORT"); override != "" {
		if parsed, err := strconv.Atoi(override); err == nil {
			port = parsed
		}
	}

	cfg := embeddedpostgres.DefaultConfig().
		Port(uint32(port)).
		Database("postgres").
		Username("postgres").
		Password("postgres").
		RuntimePath(filepath.Join(baseDir, "runtime")).
		DataPath(filepath.Join(baseDir, "data")).
		BinariesPath(filepath.Join(baseDir, "binaries"))

	embedded = embeddedpostgres.NewDatabase(cfg)
	if err := embedded.Start(); err != nil {
		panic(fmt.Sprintf("failed to start embedded postgres: %v", err))
	}

	connStr := fmt.Sprintf("postgres://postgres:postgres@localhost:%d/postgres?sslmode=disable", port)
	testConnStr = connStr
	testDB, err = sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}

	if err := waitForDB(testDB); err != nil {
		panic(err)
	}

	scriptPath := filepath.Join("..", "pgwf.sql")
	script, err := os.ReadFile(scriptPath)
	if err != nil {
		panic(err)
	}

	if _, err := testDB.Exec(string(script)); err != nil {
		if pgErr, ok := err.(*pq.Error); ok {
			panic(fmt.Sprintf("failed to apply pgwf.sql: %s (position %s)", pgErr.Message, pgErr.Position))
		}
		panic(fmt.Sprintf("failed to apply pgwf.sql: %v", err))
	}

	code := m.Run()

	if err := testDB.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "warning: closing db: %v\n", err)
	}
	if err := embedded.Stop(); err != nil {
		fmt.Fprintf(os.Stderr, "warning: stopping embedded postgres: %v\n", err)
	}

	os.Exit(code)
}

func waitForDB(db *sql.DB) error {
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := db.Ping(); err == nil {
			return nil
		} else {
			lastErr = err
		}
		time.Sleep(100 * time.Millisecond)
	}
	if lastErr != nil {
		return fmt.Errorf("database did not become ready in time: %w", lastErr)
	}
	return fmt.Errorf("database did not become ready in time")
}

func resetTables(t *testing.T) {
	t.Helper()
	if _, err := testDB.Exec(`TRUNCATE pgwf.jobs, pgwf.jobs_archive, pgwf.jobs_trace RESTART IDENTITY`); err != nil {
		t.Fatalf("failed to reset tables: %v", err)
	}
	if _, err := testDB.Exec(`SELECT pgwf.set_crash_concern_threshold($1)`, 5); err != nil {
		t.Fatalf("failed to reset crash concern threshold: %v", err)
	}
}

func TestSubmitAndLeaseFlow(t *testing.T) {
	resetTables(t)

	var (
		jobID     string
		nextNeed  string
		available time.Time
	)

	err := testDB.QueryRow(
		`SELECT job_id, next_need, available_at FROM pgwf.submit_job($1, $2, $3, $4, $5, $6)`,
		"job-basic", "submitter", "cap.alpha", nil, nil, nil,
	).Scan(&jobID, &nextNeed, &available)
	if err != nil {
		t.Fatalf("submit_job failed: %v", err)
	}
	if nextNeed != "cap.alpha" {
		t.Fatalf("expected next_need cap.alpha, got %s", nextNeed)
	}
	if available.IsZero() {
		t.Fatalf("available timestamp should be set")
	}

	rows, err := testDB.Query(
		`SELECT job_id, lease_id, lease_expires_at FROM pgwf.get_work($1, $2, $3, $4)`,
		"worker-1", pqStringArray([]string{"cap.alpha"}), 30, 1,
	)
	if err != nil {
		t.Fatalf("get_work failed: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatalf("expected a leased job")
	}

	var leasedID, leaseID string
	var leaseExpires time.Time
	if err := rows.Scan(&leasedID, &leaseID, &leaseExpires); err != nil {
		t.Fatalf("scan leased row: %v", err)
	}
	if leasedID != jobID {
		t.Fatalf("expected %s got %s", jobID, leasedID)
	}
	if leaseID == "" {
		t.Fatalf("lease id should not be empty")
	}
	if !leaseExpires.After(time.Now().Add(-time.Second)) {
		t.Fatalf("lease expiration should be in the future, got %v", leaseExpires)
	}

	if rows.Next() {
		t.Fatalf("expected only one leased job")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iteration error: %v", err)
	}

	var traceCount int
	err = testDB.QueryRow(`SELECT count(*) FROM pgwf.jobs_trace WHERE job_id = $1`, jobID).Scan(&traceCount)
	if err != nil {
		t.Fatalf("trace count failed: %v", err)
	}
	if traceCount != 2 { // submit + lease
		t.Fatalf("expected 2 trace rows, got %d", traceCount)
	}
}

func TestSingletonPreventsConcurrentLease(t *testing.T) {
	resetTables(t)

	_, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3, $4, $5)`, "job-a", "submitter", "cap.beta", nil, "single-key")
	if err != nil {
		t.Fatalf("submit job-a: %v", err)
	}
	_, err = testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3, $4, $5)`, "job-b", "submitter", "cap.beta", nil, "single-key")
	if err != nil {
		t.Fatalf("submit job-b: %v", err)
	}

	jobID, leaseID := leaseSingleJob(t, []string{"cap.beta"})
	if jobID != "job-a" {
		t.Fatalf("expected job-a first, got %s", jobID)
	}

	if hasRow(t, `SELECT 1 FROM pgwf.get_work($1, $2, $3, $4)`, "worker", pqStringArray([]string{"cap.beta"}), 30, 1) {
		t.Fatalf("second job should not lease while singleton active")
	}

	if _, err := testDB.Exec(`SELECT pgwf.complete_job($1, $2, $3)`, jobID, leaseID, "worker"); err != nil {
		t.Fatalf("complete_job failed: %v", err)
	}

	jobID2, _ := leaseSingleJob(t, []string{"cap.beta"})
	if jobID2 != "job-b" {
		t.Fatalf("expected job-b after completion, got %s", jobID2)
	}
}

func TestDependencyRelease(t *testing.T) {
	resetTables(t)

	_, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "dep-parent", "submitter", "cap.parent")
	if err != nil {
		t.Fatalf("submit parent: %v", err)
	}

	waitDeps := pqStringArray([]string{"dep-parent"})
	_, err = testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3, $4)`, "dep-child", "submitter", "cap.child", waitDeps)
	if err != nil {
		t.Fatalf("submit child: %v", err)
	}

	jobID, leaseID := leaseSingleJob(t, []string{"cap.parent", "cap.child"})
	if jobID != "dep-parent" {
		t.Fatalf("expected parent job first, got %s", jobID)
	}

	if _, err := testDB.Exec(`SELECT pgwf.complete_job($1, $2, $3)`, jobID, leaseID, "worker-parent"); err != nil {
		t.Fatalf("complete parent: %v", err)
	}

	jobID2, _ := leaseSingleJob(t, []string{"cap.child"})
	if jobID2 != "dep-child" {
		t.Fatalf("expected child after parent completion, got %s", jobID2)
	}
}

func TestCancelJobPreventsFutureWork(t *testing.T) {
	resetTables(t)

	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "job-cancel", "submitter", "cap.cancel"); err != nil {
		t.Fatalf("submit job-cancel: %v", err)
	}
	if _, err := testDB.Exec(`SELECT pgwf.cancel_job($1, $2, $3)`, "job-cancel", "operator", "test"); err != nil {
		t.Fatalf("cancel job-cancel: %v", err)
	}

	var status string
	if err := testDB.QueryRow(`SELECT status FROM pgwf.jobs_with_status WHERE job_id = $1`, "job-cancel").Scan(&status); err != nil {
		t.Fatalf("query status: %v", err)
	}
	if status != "CANCELLED" {
		t.Fatalf("expected CANCELLED status, got %s", status)
	}

	expectNoWork(t, []string{"cap.cancel"})

	_, err := testDB.Exec(`SELECT pgwf.reschedule_unheld_job($1, $2, $3)`, "job-cancel", "operator", "cap.other")
	if err == nil || (!strings.Contains(err.Error(), "cancelled") && !strings.Contains(err.Error(), "not available")) {
		t.Fatalf("expected cancellation/availability error from reschedule_unheld_job, got %v", err)
	}
}

func TestExtendLeaseAndReschedule(t *testing.T) {
	resetTables(t)

	_, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "job-resched", "submitter", "cap.gamma")
	if err != nil {
		t.Fatalf("submit job: %v", err)
	}

	jobID, leaseID := leaseSingleJob(t, []string{"cap.gamma"})

	var currentExpires time.Time
	if err := testDB.QueryRow(`SELECT lease_expires_at FROM pgwf.jobs WHERE job_id = $1`, jobID).Scan(&currentExpires); err != nil {
		t.Fatalf("query current expire: %v", err)
	}

	var newExpire time.Time
	if err := testDB.QueryRow(`SELECT pgwf.extend_lease($1, $2, $3, $4)`, jobID, leaseID, "worker", 120).Scan(&newExpire); err != nil {
		t.Fatalf("extend lease: %v", err)
	}
	if !newExpire.After(currentExpires) {
		t.Fatalf("expected expiration to increase")
	}

	err = testDB.QueryRow(
		`SELECT job_id FROM pgwf.reschedule_job($1, $2, $3, $4, $5, $6, $7)`,
		jobID, leaseID, "worker", "cap.delta", pqStringArray([]string{}), nil, time.Now(),
	).Scan(&jobID)
	if err != nil {
		t.Fatalf("reschedule failed: %v", err)
	}

	jobID2, _ := leaseSingleJob(t, []string{"cap.delta"})
	if jobID2 != "job-resched" {
		t.Fatalf("expected rescheduled job to lease again, got %s", jobID2)
	}
}

func TestCancelActiveJobBlocksExtensions(t *testing.T) {
	resetTables(t)

	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "job-active-cancel", "submitter", "cap.cancel.active"); err != nil {
		t.Fatalf("submit job: %v", err)
	}

	rows, err := testDB.Query(
		`SELECT job_id, lease_id FROM pgwf.get_work($1, $2, $3, $4)`,
		"worker-active", pqStringArray([]string{"cap.cancel.active"}), 30, 1,
	)
	if err != nil {
		t.Fatalf("get_work job-active-cancel: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("expected lease row")
	}
	var jobID, leaseID string
	if err := rows.Scan(&jobID, &leaseID); err != nil {
		t.Fatalf("scan lease: %v", err)
	}

	if _, err := testDB.Exec(`SELECT pgwf.cancel_job($1, $2, $3)`, jobID, "operator-active", "active cancellation"); err != nil {
		t.Fatalf("cancel active job: %v", err)
	}

	_, err = testDB.Exec(`SELECT pgwf.extend_lease($1, $2, $3, $4)`, jobID, leaseID, "worker-active", 30)
	if err == nil || !strings.Contains(err.Error(), "cancelled") {
		t.Fatalf("expected extend_lease cancellation error, got %v", err)
	}

	var dummy string
	err = testDB.QueryRow(
		`SELECT job_id FROM pgwf.reschedule_job($1, $2, $3, $4)`,
		jobID, leaseID, "worker-active", "cap.cancel.active",
	).Scan(&dummy)
	if err == nil || !strings.Contains(err.Error(), "cancelled") {
		t.Fatalf("expected reschedule cancellation error, got %v", err)
	}
}

func TestTraceToggle(t *testing.T) {
	resetTables(t)
	defer setTrace(t, true)

	setTrace(t, false)
	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "trace-off", "submitter", "cap.trace"); err != nil {
		t.Fatalf("submit with trace disabled: %v", err)
	}
	if count := traceCount(t); count != 0 {
		t.Fatalf("expected 0 trace rows while disabled, got %d", count)
	}

	resetTables(t)
	setTrace(t, true)
	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "trace-on", "submitter", "cap.trace"); err != nil {
		t.Fatalf("submit with trace enabled: %v", err)
	}
	jobID, _ := leaseSingleJob(t, []string{"cap.trace"})

	if count := traceCount(t); count != 2 {
		t.Fatalf("expected 2 trace rows (submit + lease) for %s, got %d", jobID, count)
	}
}

func TestGetWorkErrors(t *testing.T) {
	resetTables(t)

	cases := []struct {
		name          string
		caps          []string
		leaseSeconds  int
		limitJobs     int
		wantSubstring string
	}{
		{"empty caps", nil, 30, 1, "worker_caps cannot be empty"},
		{"invalid lease seconds", []string{"cap"}, 0, 1, "lease_seconds must be positive"},
		{"invalid limit", []string{"cap"}, 30, 0, "limit_jobs must be positive"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := testDB.Query(
				`SELECT * FROM pgwf.get_work($1, $2, $3, $4)`,
				"worker", pqStringArray(tc.caps), tc.leaseSeconds, tc.limitJobs,
			)
			if rows != nil {
				rows.Close()
			}
			expectErrorContains(t, err, tc.wantSubstring)
		})
	}
}

func TestLeaseValidationErrors(t *testing.T) {
	resetTables(t)

	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "job-err", "submitter", "cap.err"); err != nil {
		t.Fatalf("submit job: %v", err)
	}

	t.Run("extend without lease", func(t *testing.T) {
		var expires time.Time
		err := testDB.QueryRow(
			`SELECT pgwf.extend_lease($1, $2, $3, $4)`,
			"job-err", "missing", "worker", 60,
		).Scan(&expires)
		expectErrorContains(t, err, "active lease not found")
	})

	t.Run("reschedule without lease", func(t *testing.T) {
		var jobID string
		err := testDB.QueryRow(
			`SELECT job_id FROM pgwf.reschedule_job($1, $2, $3, $4, $5, $6, $7)`,
			"job-err", "missing", "worker", "cap.err", pqStringArray(nil), nil, time.Now(),
		).Scan(&jobID)
		expectErrorContains(t, err, "is not currently leased")
	})

	t.Run("complete without lease", func(t *testing.T) {
		var ok bool
		err := testDB.QueryRow(
			`SELECT pgwf.complete_job($1, $2, $3)`,
			"job-err", "missing", "worker",
		).Scan(&ok)
		expectErrorContains(t, err, "is not actively leased")
	})
}

func TestDuplicateJobSubmission(t *testing.T) {
	resetTables(t)

	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "job-dup", "submitter", "cap.dup"); err != nil {
		t.Fatalf("first submit: %v", err)
	}
	_, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "job-dup", "submitter", "cap.dup")
	if err == nil {
		t.Fatal("expected duplicate submission to fail")
	}
	var pqErr *pq.Error
	if ok := errors.As(err, &pqErr); !ok || string(pqErr.Code) != "23505" {
		t.Fatalf("expected unique_violation, got %v", err)
	}
}

func TestCompletionArchivesJob(t *testing.T) {
	resetTables(t)

	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "job-archive", "submitter", "cap.archive"); err != nil {
		t.Fatalf("submit: %v", err)
	}
	jobID, leaseID := leaseSingleJob(t, []string{"cap.archive"})

	var ok bool
	if err := testDB.QueryRow(`SELECT pgwf.complete_job($1, $2, $3)`, jobID, leaseID, "worker").Scan(&ok); err != nil {
		t.Fatalf("complete_job: %v", err)
	}
	if !ok {
		t.Fatalf("complete_job returned false")
	}

	var archivedCount int
	if err := testDB.QueryRow(`SELECT count(*) FROM pgwf.jobs_archive WHERE job_id = $1 AND lease_id = $2`, jobID, leaseID).Scan(&archivedCount); err != nil {
		t.Fatalf("query archive: %v", err)
	}
	if archivedCount != 1 {
		t.Fatalf("expected archived row, got %d", archivedCount)
	}

	var remaining int
	if err := testDB.QueryRow(`SELECT count(*) FROM pgwf.jobs`).Scan(&remaining); err != nil {
		t.Fatalf("count jobs: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("expected jobs table empty, found %d rows", remaining)
	}

	var finishEvents int
	if err := testDB.QueryRow(`SELECT count(*) FROM pgwf.jobs_trace WHERE event_type = 'job_finished' AND job_id = $1`, jobID).Scan(&finishEvents); err != nil {
		t.Fatalf("count finish traces: %v", err)
	}
	if finishEvents != 1 {
		t.Fatalf("expected job_finished trace, got %d", finishEvents)
	}

	_, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "job-archive", "submitter", "cap.archive")
	if err == nil {
		t.Fatalf("expected resubmission of archived job to fail")
	}
	expectErrorContains(t, err, "already completed")
}

func TestArchiveCancelledJobs(t *testing.T) {
	resetTables(t)

	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "cancel-1", "submitter", "cap.cancel.archive"); err != nil {
		t.Fatalf("submit cancel-1: %v", err)
	}
	waitDeps := pqStringArray([]string{"cancel-1"})
	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3, $4)`, "dependent", "submitter", "cap.dependent", waitDeps); err != nil {
		t.Fatalf("submit dependent: %v", err)
	}
	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "cancel-2", "submitter", "cap.cancel.other"); err != nil {
		t.Fatalf("submit cancel-2: %v", err)
	}

	if _, err := testDB.Exec(`SELECT pgwf.cancel_job($1, $2, $3)`, "cancel-1", "operator", "archive test"); err != nil {
		t.Fatalf("cancel cancel-1: %v", err)
	}
	if _, err := testDB.Exec(`SELECT pgwf.cancel_job($1, $2, $3)`, "cancel-2", "operator", "archive test"); err != nil {
		t.Fatalf("cancel cancel-2: %v", err)
	}

	var archived int
	if err := testDB.QueryRow(`SELECT pgwf.archive_cancelled_jobs($1, $2)`, "sweeper", 10).Scan(&archived); err != nil {
		t.Fatalf("archive_cancelled_jobs: %v", err)
	}
	if archived != 2 {
		t.Fatalf("expected to archive 2 jobs, got %d", archived)
	}

	if hasRow(t, `SELECT 1 FROM pgwf.jobs WHERE job_id = ANY($1)`, pqStringArray([]string{"cancel-1", "cancel-2"})) {
		t.Fatalf("cancelled jobs should be removed from live table")
	}

	var cancelledArchiveRows int
	if err := testDB.QueryRow(
		`SELECT count(*) FROM pgwf.jobs_archive WHERE job_id = ANY($1) AND cancel_requested`,
		pqStringArray([]string{"cancel-1", "cancel-2"}),
	).Scan(&cancelledArchiveRows); err != nil {
		t.Fatalf("count archived rows: %v", err)
	}
	if cancelledArchiveRows != 2 {
		t.Fatalf("expected archived rows to record cancellation, got %d", cancelledArchiveRows)
	}

	var depWait pqStringArray
	if err := testDB.QueryRow(`SELECT wait_for FROM pgwf.jobs WHERE job_id = 'dependent'`).Scan(&depWait); err != nil {
		t.Fatalf("query dependent wait_for: %v", err)
	}
	if len(depWait) != 0 {
		t.Fatalf("dependent wait_for should be cleared, got %v", depWait)
	}

	var perJobTraceCount int
	if err := testDB.QueryRow(`SELECT count(*) FROM pgwf.jobs_trace WHERE event_type = 'job_cancel_archived' AND worker_id = $1`, "sweeper").Scan(&perJobTraceCount); err != nil {
		t.Fatalf("count per-job traces: %v", err)
	}
	if perJobTraceCount != 2 {
		t.Fatalf("expected 2 job_cancel_archived traces, got %d", perJobTraceCount)
	}

	var runTraceCountBefore int
	if err := testDB.QueryRow(`SELECT count(*) FROM pgwf.jobs_trace WHERE event_type = 'job_cancel_archived_run'`).Scan(&runTraceCountBefore); err != nil {
		t.Fatalf("count run traces: %v", err)
	}
	if runTraceCountBefore != 1 {
		t.Fatalf("expected single run trace after archiving, got %d", runTraceCountBefore)
	}

	if err := testDB.QueryRow(`SELECT pgwf.archive_cancelled_jobs($1, $2)`, "sweeper", 10).Scan(&archived); err != nil {
		t.Fatalf("second archive_cancelled_jobs: %v", err)
	}
	if archived != 0 {
		t.Fatalf("expected 0 archived rows on second run, got %d", archived)
	}

	var runTraceCountAfter int
	if err := testDB.QueryRow(`SELECT count(*) FROM pgwf.jobs_trace WHERE event_type = 'job_cancel_archived_run'`).Scan(&runTraceCountAfter); err != nil {
		t.Fatalf("count run traces after: %v", err)
	}
	if runTraceCountAfter != runTraceCountBefore {
		t.Fatalf("expected no new run trace when nothing archived, got %d before %d after", runTraceCountBefore, runTraceCountAfter)
	}
}

func TestNotifyToggle(t *testing.T) {
	resetTables(t)
	setNotify(t, false)
	t.Cleanup(func() { setNotify(t, false) })

	channel := "pgwf.need.cap.notify"
	listener := newNotifyListener(t, channel)
	defer listener.Close()

	setNotify(t, true)
	drainNotifications(listener)

	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "job-notify-on", "submitter", "cap.notify"); err != nil {
		t.Fatalf("submit job-notify-on: %v", err)
	}
	if !waitForNotification(listener, time.Second) {
		t.Fatalf("expected notification when notify enabled")
	}

	resetTables(t)
	setNotify(t, false)
	drainNotifications(listener)

	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "job-notify-off", "submitter", "cap.notify"); err != nil {
		t.Fatalf("submit job-notify-off: %v", err)
	}
	if waitForNotification(listener, 300*time.Millisecond) {
		t.Fatalf("expected no notification when notify disabled")
	}
}

func TestCancelPendingJobSuppressesNotify(t *testing.T) {
	resetTables(t)
	setNotify(t, true)
	t.Cleanup(func() { setNotify(t, false) })

	listener := newNotifyListener(t, "pgwf.need.cap.pending.cancel")
	defer listener.Close()
	drainNotifications(listener)

	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "blocker-cancel", "submitter", "cap.blocker.cancel"); err != nil {
		t.Fatalf("submit blocker: %v", err)
	}
	waitDeps := pqStringArray([]string{"blocker-cancel"})
	if _, err := testDB.Exec(
		`SELECT pgwf.submit_job($1, $2, $3, $4)`,
		"pending-cancel", "submitter", "cap.pending.cancel", waitDeps,
	); err != nil {
		t.Fatalf("submit pending job: %v", err)
	}
	if _, err := testDB.Exec(`SELECT pgwf.cancel_job($1, $2, $3)`, "pending-cancel", "operator", "cancel pending"); err != nil {
		t.Fatalf("cancel pending job: %v", err)
	}
	drainNotifications(listener)

	blockerJob, blockerLease := leaseSingleJob(t, []string{"cap.blocker.cancel"})
	if blockerJob != "blocker-cancel" {
		t.Fatalf("expected blocker-cancel, got %s", blockerJob)
	}
	if _, err := testDB.Exec(`SELECT pgwf.complete_job($1, $2, $3)`, blockerJob, blockerLease, "worker-blocker"); err != nil {
		t.Fatalf("complete blocker: %v", err)
	}

	if waitForNotification(listener, 300*time.Millisecond) {
		t.Fatalf("expected no notification for cancelled pending job")
	}
}

func TestGetWorkRegistersListen(t *testing.T) {
	resetTables(t)
	setNotify(t, true)
	t.Cleanup(func() { setNotify(t, false) })

	ctx := context.Background()
	conn, err := testDB.Conn(ctx)
	if err != nil {
		t.Fatalf("open conn: %v", err)
	}
	t.Cleanup(func() {
		if _, err := conn.ExecContext(ctx, "UNLISTEN *"); err != nil {
			t.Fatalf("unlisten: %v", err)
		}
		conn.Close()
	})

	caps := []string{"cap.listen.a", "cap.listen.b"}

	rows, err := conn.QueryContext(ctx,
		`SELECT * FROM pgwf.get_work($1, $2, $3, $4)`,
		"worker-listen", pqStringArray(caps), 30, 1,
	)
	if err != nil {
		t.Fatalf("get_work: %v", err)
	}
	if rows.Next() {
		t.Fatalf("expected no work to lease")
	}
	rows.Close()

	chanRows, err := conn.QueryContext(ctx, `SELECT pg_listening_channels() AS channel`)
	if err != nil {
		t.Fatalf("pg_listening_channels: %v", err)
	}
	defer chanRows.Close()

	listening := map[string]bool{}
	for chanRows.Next() {
		var channel string
		if err := chanRows.Scan(&channel); err != nil {
			t.Fatalf("scan channel: %v", err)
		}
		listening[channel] = true
	}
	if err := chanRows.Err(); err != nil {
		t.Fatalf("iterate channels: %v", err)
	}

	for _, capName := range caps {
		channel := fmt.Sprintf("pgwf.need.%s", capName)
		if !listening[channel] {
			t.Fatalf("expected listener on %s, found %+v", channel, listening)
		}
	}
}

func TestJobsStatusViews(t *testing.T) {
	resetTables(t)

	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "job-ready", "submitter", "cap.ready"); err != nil {
		t.Fatalf("submit job-ready: %v", err)
	}

	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "job-blocker", "submitter", "cap.blocker"); err != nil {
		t.Fatalf("submit job-blocker: %v", err)
	}
	waitDeps := pqStringArray([]string{"job-blocker"})
	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3, $4)`, "job-pending", "submitter", "cap.pending", waitDeps); err != nil {
		t.Fatalf("submit job-pending: %v", err)
	}

	futureTime := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Microsecond)
	if _, err := testDB.Exec(
		`SELECT pgwf.submit_job($1, $2, $3, $4, $5, $6)`,
		"job-future", "submitter", "cap.future", pqStringArray([]string{}), nil, futureTime,
	); err != nil {
		t.Fatalf("submit job-future: %v", err)
	}

	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "job-active", "submitter", "cap.active"); err != nil {
		t.Fatalf("submit job-active: %v", err)
	}
	activeJob, activeLease := leaseSingleJob(t, []string{"cap.active"})
	if activeJob != "job-active" {
		t.Fatalf("expected job-active lease, got %s", activeJob)
	}

	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "job-cancelled", "submitter", "cap.cancelled"); err != nil {
		t.Fatalf("submit job-cancelled: %v", err)
	}
	if _, err := testDB.Exec(`SELECT pgwf.cancel_job($1, $2, $3)`, "job-cancelled", "operator", "view test"); err != nil {
		t.Fatalf("cancel job-cancelled: %v", err)
	}

	expiredAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	if _, err := testDB.Exec(
		`SELECT pgwf.submit_job($1, $2, $3, $4, $5, $6, $7)`,
		"job-expired", "submitter", "cap.expired", pqStringArray([]string{}), nil, nil, expiredAt,
	); err != nil {
		t.Fatalf("submit job-expired: %v", err)
	}

	targetIDs := []string{"job-active", "job-future", "job-pending", "job-ready", "job-cancelled", "job-expired"}
	rows, err := testDB.Query(`SELECT job_id, status FROM pgwf.jobs_with_status WHERE job_id = ANY($1)`, pqStringArray(targetIDs))
	if err != nil {
		t.Fatalf("query jobs_with_status: %v", err)
	}
	defer rows.Close()

	seen := map[string]string{}
	for rows.Next() {
		var jobID, status string
		if err := rows.Scan(&jobID, &status); err != nil {
			t.Fatalf("scan jobs_with_status: %v", err)
		}
		seen[jobID] = status
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate jobs_with_status: %v", err)
	}

	wantStatus := map[string]string{
		"job-active":    "ACTIVE",
		"job-future":    "AWAITING_FUTURE",
		"job-pending":   "PENDING_JOBS",
		"job-ready":     "READY",
		"job-cancelled": "CANCELLED",
		"job-expired":   "EXPIRED",
	}
	for jobID, want := range wantStatus {
		got, ok := seen[jobID]
		if !ok {
			t.Fatalf("missing %s in jobs_with_status", jobID)
		}
		if got != want {
			t.Fatalf("job %s status = %s, want %s", jobID, got, want)
		}
	}

	type friendlyExpect struct {
		status      string
		worker      string
		sleep       *time.Time
		pending     []string
		cancelled   bool
		cancelledBy string
		expires     *time.Time
	}

	futurePtr := futureTime
	expectFriendly := map[string]friendlyExpect{
		"job-active":    {status: "ACTIVE", worker: activeLease},
		"job-future":    {status: "AWAITING_FUTURE", sleep: &futurePtr},
		"job-pending":   {status: "PENDING_JOBS", pending: []string{"job-blocker"}},
		"job-ready":     {status: "READY"},
		"job-cancelled": {status: "CANCELLED", cancelled: true, cancelledBy: "operator"},
		"job-expired":   {status: "EXPIRED", expires: &expiredAt},
	}

	for jobID, exp := range expectFriendly {
		var (
			status   string
			creation time.Time
			pending  pqStringArray
			sleep    sql.NullTime
			worker   sql.NullString
			cancelAt sql.NullTime
			cancelBy sql.NullString
			expires  sql.NullTime
		)

		err := testDB.QueryRow(
			`SELECT status, creation_dt, pending_jobs, sleep_until, worker_id, cancelled_at, cancelled_by, CASE WHEN expires_at = 'infinity' THEN NULL ELSE expires_at END FROM pgwf.jobs_friendly_status WHERE job_id = $1`,
			jobID,
		).Scan(&status, &creation, &pending, &sleep, &worker, &cancelAt, &cancelBy, &expires)
		if err != nil {
			t.Fatalf("query friendly row for %s: %v", jobID, err)
		}

		if status != exp.status {
			t.Fatalf("friendly status mismatch for %s: got %s want %s", jobID, status, exp.status)
		}
		if exp.worker != "" {
			if !worker.Valid || worker.String != exp.worker {
				t.Fatalf("worker mismatch for %s: got %+v want %s", jobID, worker, exp.worker)
			}
		} else if worker.Valid {
			t.Fatalf("expected worker_id NULL for %s, got %s", jobID, worker.String)
		}

		if exp.sleep != nil {
			if !sleep.Valid {
				t.Fatalf("expected sleep_until for %s", jobID)
			}
			if !sleep.Time.Equal(*exp.sleep) {
				t.Fatalf("sleep_until mismatch for %s: got %v want %v", jobID, sleep.Time, *exp.sleep)
			}
		} else if sleep.Valid {
			t.Fatalf("expected sleep_until NULL for %s, got %v", jobID, sleep.Time)
		}

		if exp.pending != nil {
			if len(pending) != len(exp.pending) {
				t.Fatalf("pending length mismatch for %s: got %v want %v", jobID, pending, exp.pending)
			}
			for i, want := range exp.pending {
				if pending[i] != want {
					t.Fatalf("pending mismatch for %s: got %v want %v", jobID, pending, exp.pending)
				}
			}
		} else if pending != nil {
			t.Fatalf("expected pending_jobs NULL for %s, got %v", jobID, pending)
		}
		if exp.cancelled {
			if !cancelAt.Valid {
				t.Fatalf("expected cancelled_at for %s", jobID)
			}
			if !cancelBy.Valid || cancelBy.String != exp.cancelledBy {
				t.Fatalf("expected cancelled_by %s for %s, got %+v", exp.cancelledBy, jobID, cancelBy)
			}
		} else {
			if cancelAt.Valid {
				t.Fatalf("expected cancelled_at NULL for %s, got %v", jobID, cancelAt.Time)
			}
			if cancelBy.Valid {
				t.Fatalf("expected cancelled_by NULL for %s, got %s", jobID, cancelBy.String)
			}
		}
		if exp.expires != nil {
			if !expires.Valid || !expires.Time.Equal(exp.expires.Truncate(time.Microsecond)) {
				t.Fatalf("expires mismatch for %s: got %v want %v", jobID, expires, exp.expires)
			}
		} else if expires.Valid {
			t.Fatalf("expected expires_at NULL for %s, got %v", jobID, expires.Time)
		}
	}
}

func TestExpiredJobsSkipLeasing(t *testing.T) {
	resetTables(t)

	expiredAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	if _, err := testDB.Exec(
		`SELECT pgwf.submit_job($1, $2, $3, $4, $5, $6, $7)`,
		"job-expired", "submitter", "cap.expired", pqStringArray([]string{}), nil, nil, expiredAt,
	); err != nil {
		t.Fatalf("submit expired job: %v", err)
	}

	expectNoWork(t, []string{"cap.expired"})

	var status string
	if err := testDB.QueryRow(`SELECT status FROM pgwf.jobs_with_status WHERE job_id = $1`, "job-expired").Scan(&status); err != nil {
		t.Fatalf("query status: %v", err)
	}
	if status != "EXPIRED" {
		t.Fatalf("expected EXPIRED status, got %s", status)
	}

	var rescheduled string
	err := testDB.QueryRow(
		`SELECT job_id FROM pgwf.reschedule_unheld_job($1, $2, $3, $4, $5, $6)`,
		"job-expired", "operator", "cap.expired.new", pqStringArray([]string{}), nil, time.Now().UTC(),
	).Scan(&rescheduled)
	if err != nil {
		t.Fatalf("reschedule expired job: %v", err)
	}
	if rescheduled != "job-expired" {
		t.Fatalf("expected job-expired reschedule, got %s", rescheduled)
	}

	if err := testDB.QueryRow(`SELECT status FROM pgwf.jobs_with_status WHERE job_id = $1`, "job-expired").Scan(&status); err != nil {
		t.Fatalf("query status after reschedule: %v", err)
	}
	if status != "EXPIRED" {
		t.Fatalf("expected EXPIRED status after reschedule, got %s", status)
	}

	expectNoWork(t, []string{"cap.expired.new"})
}

func TestExtendLeaseOnExpiredJob(t *testing.T) {
	resetTables(t)

	expiresSoon := time.Now().UTC().Add(time.Minute).Truncate(time.Microsecond)
	if _, err := testDB.Exec(
		`SELECT pgwf.submit_job($1, $2, $3, $4, $5, $6, $7)`,
		"job-extend", "submitter", "cap.extend", pqStringArray([]string{}), nil, nil, expiresSoon,
	); err != nil {
		t.Fatalf("submit job: %v", err)
	}

	jobID, leaseID := leaseSingleJob(t, []string{"cap.extend"})

	if _, err := testDB.Exec(`UPDATE pgwf.jobs SET expires_at = clock_timestamp() - interval '1 minute' WHERE job_id = $1`, jobID); err != nil {
		t.Fatalf("force expiry: %v", err)
	}

	var newLeaseExpiry time.Time
	if err := testDB.QueryRow(`SELECT pgwf.extend_lease($1, $2, $3, $4)`, jobID, leaseID, "worker-extend", 120).Scan(&newLeaseExpiry); err != nil {
		t.Fatalf("extend lease on expired job: %v", err)
	}
	if !newLeaseExpiry.After(time.Now().Add(time.Minute)) {
		t.Fatalf("expected extended lease in future, got %v", newLeaseExpiry)
	}

	var status string
	if err := testDB.QueryRow(`SELECT status FROM pgwf.jobs_with_status WHERE job_id = $1`, jobID).Scan(&status); err != nil {
		t.Fatalf("query status after extend: %v", err)
	}
	if status != "ACTIVE" {
		t.Fatalf("expected ACTIVE status after extend, got %s", status)
	}
}

func TestPartialDependencyRelease(t *testing.T) {
	resetTables(t)

	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "dep-a", "submitter", "cap.parent"); err != nil {
		t.Fatalf("submit dep-a: %v", err)
	}
	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "dep-b", "submitter", "cap.parent"); err != nil {
		t.Fatalf("submit dep-b: %v", err)
	}
	waitDeps := pqStringArray([]string{"dep-a", "dep-b"})
	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3, $4)`, "child", "submitter", "cap.child", waitDeps); err != nil {
		t.Fatalf("submit child: %v", err)
	}

	aJob, aLease := leaseSingleJob(t, []string{"cap.parent"})
	if aJob != "dep-a" {
		t.Fatalf("expected dep-a first, got %s", aJob)
	}
	bJob, bLease := leaseSingleJob(t, []string{"cap.parent"})
	if bJob != "dep-b" {
		t.Fatalf("expected dep-b second, got %s", bJob)
	}

	if _, err := testDB.Exec(`SELECT pgwf.complete_job($1, $2, $3)`, aJob, aLease, "worker-a"); err != nil {
		t.Fatalf("complete dep-a: %v", err)
	}

	var waitFor pqStringArray
	if err := testDB.QueryRow(`SELECT wait_for FROM pgwf.jobs WHERE job_id = 'child'`).Scan(&waitFor); err != nil {
		t.Fatalf("query child wait_for: %v", err)
	}
	if len(waitFor) != 1 || waitFor[0] != "dep-b" {
		t.Fatalf("expected child still waiting on dep-b, got %v", waitFor)
	}

	expectNoWork(t, []string{"cap.child"})

	if _, err := testDB.Exec(`SELECT pgwf.complete_job($1, $2, $3)`, bJob, bLease, "worker-b"); err != nil {
		t.Fatalf("complete dep-b: %v", err)
	}

	var childWaitCount int
	if err := testDB.QueryRow(`SELECT COALESCE(array_length(wait_for, 1), 0) FROM pgwf.jobs WHERE job_id = 'child'`).Scan(&childWaitCount); err != nil {
		t.Fatalf("query child wait len: %v", err)
	}
	if childWaitCount != 0 {
		t.Fatalf("expected child wait list empty, got %d", childWaitCount)
	}

	childJob, _ := leaseSingleJob(t, []string{"cap.child"})
	if childJob != "child" {
		t.Fatalf("expected child to lease, got %s", childJob)
	}
}

func TestSubmitJobFiltersArchivedDependencies(t *testing.T) {
	resetTables(t)

	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "dep-live", "submitter", "cap.live"); err != nil {
		t.Fatalf("submit dep-live: %v", err)
	}
	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "dep-arch", "submitter", "cap.arch"); err != nil {
		t.Fatalf("submit dep-arch: %v", err)
	}

	jobID, leaseID := leaseSingleJob(t, []string{"cap.arch"})
	if jobID != "dep-arch" {
		t.Fatalf("expected dep-arch, got %s", jobID)
	}
	if _, err := testDB.Exec(`SELECT pgwf.complete_job($1, $2, $3)`, jobID, leaseID, "worker-arch"); err != nil {
		t.Fatalf("complete dep-arch: %v", err)
	}

	var childID string
	err := testDB.QueryRow(
		`SELECT job_id FROM pgwf.submit_job($1, $2, $3, $4)`,
		"child", "submitter", "cap.child", pqStringArray([]string{"dep-arch", "dep-live"}),
	).Scan(&childID)
	if err != nil {
		t.Fatalf("submit child: %v", err)
	}

	var waitFor pqStringArray
	if err := testDB.QueryRow(`SELECT wait_for FROM pgwf.jobs WHERE job_id = 'child'`).Scan(&waitFor); err != nil {
		t.Fatalf("query child wait_for: %v", err)
	}
	if len(waitFor) != 1 || waitFor[0] != "dep-live" {
		t.Fatalf("expected wait_for to retain only dep-live, got %v", waitFor)
	}
}

func TestSubmitJobWaitsOnActiveJob(t *testing.T) {
	resetTables(t)

	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "dep-active", "submitter", "cap.dep.active"); err != nil {
		t.Fatalf("submit dep-active: %v", err)
	}
	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3, $4)`, "child", "submitter", "cap.child", pqStringArray([]string{"dep-active"})); err != nil {
		t.Fatalf("submit child: %v", err)
	}

	var waitFor pqStringArray
	if err := testDB.QueryRow(`SELECT wait_for FROM pgwf.jobs WHERE job_id = 'child'`).Scan(&waitFor); err != nil {
		t.Fatalf("query child wait_for: %v", err)
	}
	if len(waitFor) != 1 || waitFor[0] != "dep-active" {
		t.Fatalf("expected child to wait on dep-active, got %v", waitFor)
	}

	expectNoWork(t, []string{"cap.child"})

	depJob, depLease := leaseSingleJob(t, []string{"cap.dep.active"})
	if depJob != "dep-active" {
		t.Fatalf("expected dep-active lease, got %s", depJob)
	}
	if _, err := testDB.Exec(`SELECT pgwf.complete_job($1, $2, $3)`, depJob, depLease, "worker-dep"); err != nil {
		t.Fatalf("complete dep-active: %v", err)
	}

	childJob, _ := leaseSingleJob(t, []string{"cap.child"})
	if childJob != "child" {
		t.Fatalf("expected child to lease after dependency completion, got %s", childJob)
	}
}

func TestSubmitJobWaitForUnknownError(t *testing.T) {
	resetTables(t)

	_, err := testDB.Exec(
		`SELECT pgwf.submit_job($1, $2, $3, $4)`,
		"child", "submitter", "cap.child", pqStringArray([]string{"unknown"}),
	)
	expectErrorContains(t, err, "wait_for references unknown jobs")
}

func TestRescheduleWaitForValidation(t *testing.T) {
	resetTables(t)

	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "dep-resched", "submitter", "cap.dep.active"); err != nil {
		t.Fatalf("submit dep-resched: %v", err)
	}
	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "dep-old", "submitter", "cap.dep.old"); err != nil {
		t.Fatalf("submit dep-old: %v", err)
	}
	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "job-resched-wait", "submitter", "cap.target"); err != nil {
		t.Fatalf("submit job-resched-wait: %v", err)
	}

	oldJob, oldLease := leaseSingleJob(t, []string{"cap.dep.old"})
	if oldJob != "dep-old" {
		t.Fatalf("expected dep-old lease, got %s", oldJob)
	}
	if _, err := testDB.Exec(`SELECT pgwf.complete_job($1, $2, $3)`, oldJob, oldLease, "worker-old"); err != nil {
		t.Fatalf("complete dep-old: %v", err)
	}

	jobID, leaseID := leaseSingleJob(t, []string{"cap.target"})
	if jobID != "job-resched-wait" {
		t.Fatalf("expected job-resched-wait, got %s", jobID)
	}

	var dummy string
	err := testDB.QueryRow(
		`SELECT job_id FROM pgwf.reschedule_job($1, $2, $3, $4, $5, $6, $7)`,
		jobID, leaseID, "worker", "cap.target", pqStringArray([]string{"unknown"}), nil, time.Now(),
	).Scan(&dummy)
	expectErrorContains(t, err, "wait_for references unknown jobs")

	err = testDB.QueryRow(
		`SELECT job_id FROM pgwf.reschedule_job($1, $2, $3, $4, $5, $6, $7)`,
		jobID, leaseID, "worker", "cap.target", pqStringArray([]string{"dep-resched", "dep-old"}), nil, time.Now(),
	).Scan(&dummy)
	if err != nil {
		t.Fatalf("reschedule with filtered wait_for: %v", err)
	}

	var waitFor pqStringArray
	if err := testDB.QueryRow(`SELECT wait_for FROM pgwf.jobs WHERE job_id = $1`, jobID).Scan(&waitFor); err != nil {
		t.Fatalf("query rescheduled wait_for: %v", err)
	}
	if len(waitFor) != 1 || waitFor[0] != "dep-resched" {
		t.Fatalf("expected wait_for to include only dep-resched, got %v", waitFor)
	}
}

func TestRescheduleAllowsActiveDependency(t *testing.T) {
	resetTables(t)

	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "dep-active-resched", "submitter", "cap.dep.active"); err != nil {
		t.Fatalf("submit dependency: %v", err)
	}
	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "job-target", "submitter", "cap.target"); err != nil {
		t.Fatalf("submit job-target: %v", err)
	}

	jobID, leaseID := leaseSingleJob(t, []string{"cap.target"})

	var rescheduled string
	if err := testDB.QueryRow(
		`SELECT job_id FROM pgwf.reschedule_job($1, $2, $3, $4, $5, $6, $7)`,
		jobID, leaseID, "worker-target", "cap.target", pqStringArray([]string{"dep-active-resched"}), nil, time.Now(),
	).Scan(&rescheduled); err != nil {
		t.Fatalf("reschedule target: %v", err)
	}
	if rescheduled != "job-target" {
		t.Fatalf("expected job-target from reschedule, got %s", rescheduled)
	}

	var waitFor pqStringArray
	if err := testDB.QueryRow(`SELECT wait_for FROM pgwf.jobs WHERE job_id = $1`, jobID).Scan(&waitFor); err != nil {
		t.Fatalf("query wait_for: %v", err)
	}
	if len(waitFor) != 1 || waitFor[0] != "dep-active-resched" {
		t.Fatalf("expected wait_for to include dependency, got %v", waitFor)
	}

	expectNoWork(t, []string{"cap.target"})

	depJob, depLease := leaseSingleJob(t, []string{"cap.dep.active"})
	if depJob != "dep-active-resched" {
		t.Fatalf("expected dependency lease, got %s", depJob)
	}
	if _, err := testDB.Exec(`SELECT pgwf.complete_job($1, $2, $3)`, depJob, depLease, "worker-dep"); err != nil {
		t.Fatalf("complete dependency: %v", err)
	}

	targetJob, _ := leaseSingleJob(t, []string{"cap.target"})
	if targetJob != "job-target" {
		t.Fatalf("expected job-target to become runnable, got %s", targetJob)
	}
}

func TestCompleteUnheldJob(t *testing.T) {
	resetTables(t)

	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "job-free", "worker", "cap.gamma"); err != nil {
		t.Fatalf("submit job-free: %v", err)
	}

	if _, err := testDB.Exec(`SELECT pgwf.complete_unheld_job($1, $2)`, "job-free", "worker"); err != nil {
		t.Fatalf("complete_unheld_job: %v", err)
	}

	if hasRow(t, `SELECT 1 FROM pgwf.jobs WHERE job_id = $1`, "job-free") {
		t.Fatalf("job should have been removed from jobs")
	}
	if !hasRow(t, `SELECT 1 FROM pgwf.jobs_archive WHERE job_id = $1`, "job-free") {
		t.Fatalf("job should exist in archive")
	}

	if _, err := testDB.Exec(`SELECT pgwf.complete_unheld_job($1, $2)`, "job-missing", "worker"); err == nil || !strings.Contains(err.Error(), "is not available") {
		t.Fatalf("expected error for missing job, got %v", err)
	}

	// Future job should not be completable without a lease.
	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3, $4, $5, $6)`, "job-future", "worker", "cap.zeta", nil, nil, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("submit future job: %v", err)
	}
	if _, err := testDB.Exec(`SELECT pgwf.complete_unheld_job($1, $2)`, "job-future", "worker"); err == nil || !strings.Contains(err.Error(), "is not available") {
		t.Fatalf("expected future job error, got %v", err)
	}
}

func TestRescheduleUnheldJob(t *testing.T) {
	resetTables(t)

	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "job-free", "worker", "cap.delta"); err != nil {
		t.Fatalf("submit job-free: %v", err)
	}

	rows, err := testDB.Query(`
        SELECT job_id, next_need
        FROM pgwf.reschedule_unheld_job($1, $2, $3, $4, $5, $6)
    `, "job-free", "worker", "cap.theta", nil, "single", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("reschedule_unheld_job: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("expected rescheduled row")
	}
	var jobID, nextNeed string
	if err := rows.Scan(&jobID, &nextNeed); err != nil {
		t.Fatalf("scan rescheduled: %v", err)
	}
	if jobID != "job-free" || nextNeed != "cap.theta" {
		t.Fatalf("unexpected reschedule result: %s %s", jobID, nextNeed)
	}

	if rows.Next() {
		t.Fatalf("expected single reschedule result")
	}

	if _, err := testDB.Exec(`SELECT pgwf.reschedule_unheld_job($1, $2, $3)`, "job-missing", "worker", "cap"); err == nil || !strings.Contains(err.Error(), "is not available") {
		t.Fatalf("expected error for missing job, got %v", err)
	}

	// Leased jobs should not be rescheduled via the unheld helper.
	resetTables(t)
	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "job-leased", "worker", "cap.leased"); err != nil {
		t.Fatalf("submit job-leased: %v", err)
	}
	leasedJob, _ := leaseSingleJob(t, []string{"cap.leased"})
	if _, err := testDB.Exec(`SELECT pgwf.reschedule_unheld_job($1, $2, $3)`, leasedJob, "worker", "cap.other"); err == nil || !strings.Contains(err.Error(), "is not available") {
		t.Fatalf("expected leased job error, got %v", err)
	}
}

func TestCrashConcernCountingAndClearing(t *testing.T) {
	resetTables(t)

	if _, err := testDB.Exec(`SELECT pgwf.set_crash_concern_threshold($1)`, 1); err != nil {
		t.Fatalf("set crash threshold: %v", err)
	}

	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "job-crash", "submitter", "cap.crash"); err != nil {
		t.Fatalf("submit job-crash: %v", err)
	}

	jobID, _ := leaseSingleJob(t, []string{"cap.crash"})

	if _, err := testDB.Exec(`UPDATE pgwf.jobs SET lease_expires_at = clock_timestamp() - interval '1 minute' WHERE job_id = $1`, jobID); err != nil {
		t.Fatalf("force lease expiry: %v", err)
	}

	reacquiredJob, _ := leaseSingleJob(t, []string{"cap.crash"})
	if reacquiredJob != jobID {
		t.Fatalf("expected to reacquire %s got %s", jobID, reacquiredJob)
	}

	var total, consecutive int64
	if err := testDB.QueryRow(`SELECT lease_expiration_count, consecutive_expirations FROM pgwf.jobs WHERE job_id = $1`, jobID).Scan(&total, &consecutive); err != nil {
		t.Fatalf("read counters: %v", err)
	}
	if total != 1 || consecutive != 1 {
		t.Fatalf("expected counters 1/1, got %d/%d", total, consecutive)
	}

	if _, err := testDB.Exec(`UPDATE pgwf.jobs SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE job_id = $1`, jobID); err != nil {
		t.Fatalf("expire active lease: %v", err)
	}

	expectNoWork(t, []string{"cap.crash"})

	var status string
	if err := testDB.QueryRow(`SELECT status FROM pgwf.jobs_with_status WHERE job_id = $1`, jobID).Scan(&status); err != nil {
		t.Fatalf("read status: %v", err)
	}
	if status != "CRASH_CONCERN" {
		t.Fatalf("expected CRASH_CONCERN status, got %s", status)
	}

	var viewConsecutive int64
	if err := testDB.QueryRow(`SELECT consecutive_expirations FROM pgwf.jobs_with_status WHERE job_id = $1`, jobID).Scan(&viewConsecutive); err != nil {
		t.Fatalf("read view counters: %v", err)
	}
	if viewConsecutive != 1 {
		t.Fatalf("expected view consecutive 1, got %d", viewConsecutive)
	}

	var traceCount int
	if err := testDB.QueryRow(`SELECT count(*) FROM pgwf.jobs_trace WHERE job_id = $1 AND event_type = 'lease_expiration_counter_incremented'`, jobID).Scan(&traceCount); err != nil {
		t.Fatalf("count increment traces: %v", err)
	}
	if traceCount != 1 {
		t.Fatalf("expected 1 increment trace, got %d", traceCount)
	}

	if _, err := testDB.Exec(`SELECT pgwf.clear_crash_concern($1, $2, $3)`, jobID, "operator", "ack"); err != nil {
		t.Fatalf("clear crash concern failed: %v", err)
	}

	if err := testDB.QueryRow(`SELECT consecutive_expirations, lease_expiration_count FROM pgwf.jobs WHERE job_id = $1`, jobID).Scan(&consecutive, &total); err != nil {
		t.Fatalf("read counters after clear: %v", err)
	}
	if consecutive != 0 || total != 1 {
		t.Fatalf("expected counters 0/1 after clear, got %d/%d", consecutive, total)
	}

	if err := testDB.QueryRow(`SELECT status FROM pgwf.jobs_with_status WHERE job_id = $1`, jobID).Scan(&status); err != nil {
		t.Fatalf("read status after clear: %v", err)
	}
	if status != "READY" {
		t.Fatalf("expected READY after clear, got %s", status)
	}

	leasedAfterClear, _ := leaseSingleJob(t, []string{"cap.crash"})
	if leasedAfterClear != jobID {
		t.Fatalf("expected to lease %s after clearing, got %s", jobID, leasedAfterClear)
	}

	var clearTraceCount int
	if err := testDB.QueryRow(`SELECT count(*) FROM pgwf.jobs_trace WHERE job_id = $1 AND event_type = 'crash_concern_cleared'`, jobID).Scan(&clearTraceCount); err != nil {
		t.Fatalf("count clear traces: %v", err)
	}
	if clearTraceCount != 1 {
		t.Fatalf("expected 1 clear trace, got %d", clearTraceCount)
	}
}

func TestRescheduleClearsCrashConcern(t *testing.T) {
	resetTables(t)

	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "job-resched", "submitter", "cap.resched"); err != nil {
		t.Fatalf("submit job-resched: %v", err)
	}

	jobID, _ := leaseSingleJob(t, []string{"cap.resched"})

	if _, err := testDB.Exec(`UPDATE pgwf.jobs SET lease_expires_at = clock_timestamp() - interval '1 minute' WHERE job_id = $1`, jobID); err != nil {
		t.Fatalf("force lease expiry: %v", err)
	}

	reacquiredJob, newLease := leaseSingleJob(t, []string{"cap.resched"})
	if reacquiredJob != jobID {
		t.Fatalf("expected to reacquire %s got %s", jobID, reacquiredJob)
	}

	var total, consecutive int64
	if err := testDB.QueryRow(`SELECT lease_expiration_count, consecutive_expirations FROM pgwf.jobs WHERE job_id = $1`, jobID).Scan(&total, &consecutive); err != nil {
		t.Fatalf("read counters: %v", err)
	}
	if total != 1 || consecutive != 1 {
		t.Fatalf("expected counters 1/1 before reschedule, got %d/%d", total, consecutive)
	}

	var rescheduled string
	err := testDB.QueryRow(
		`SELECT job_id FROM pgwf.reschedule_job($1, $2, $3, $4)`,
		jobID,
		newLease,
		"worker",
		"cap.resched",
	).Scan(&rescheduled)
	if err != nil {
		t.Fatalf("reschedule_job failed: %v", err)
	}
	if rescheduled != jobID {
		t.Fatalf("unexpected rescheduled job id %s", rescheduled)
	}

	if err := testDB.QueryRow(`SELECT lease_expiration_count, consecutive_expirations FROM pgwf.jobs WHERE job_id = $1`, jobID).Scan(&total, &consecutive); err != nil {
		t.Fatalf("read counters after reschedule: %v", err)
	}
	if total != 1 || consecutive != 0 {
		t.Fatalf("expected counters 1/0 after reschedule, got %d/%d", total, consecutive)
	}
}

func TestCompleteUnheldCrashConcern(t *testing.T) {
	resetTables(t)

	if _, err := testDB.Exec(`SELECT pgwf.set_crash_concern_threshold($1)`, 1); err != nil {
		t.Fatalf("set crash threshold: %v", err)
	}

	if _, err := testDB.Exec(`SELECT pgwf.submit_job($1, $2, $3)`, "job-crash-complete", "submitter", "cap.crash.complete"); err != nil {
		t.Fatalf("submit job-crash-complete: %v", err)
	}

	jobID, _ := leaseSingleJob(t, []string{"cap.crash.complete"})

	if _, err := testDB.Exec(`UPDATE pgwf.jobs SET lease_expires_at = clock_timestamp() - interval '1 minute' WHERE job_id = $1`, jobID); err != nil {
		t.Fatalf("force lease expiry: %v", err)
	}

	// reacquire once to trigger the crash concern threshold
	reacquiredJob, _ := leaseSingleJob(t, []string{"cap.crash.complete"})
	if reacquiredJob != jobID {
		t.Fatalf("expected to reacquire %s got %s", jobID, reacquiredJob)
	}

	// expire immediately so status flips to CRASH_CONCERN
	if _, err := testDB.Exec(`UPDATE pgwf.jobs SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE job_id = $1`, jobID); err != nil {
		t.Fatalf("expire active lease: %v", err)
	}

	var status string
	if err := testDB.QueryRow(`SELECT status FROM pgwf.jobs_with_status WHERE job_id = $1`, jobID).Scan(&status); err != nil {
		t.Fatalf("read status: %v", err)
	}
	if status != "CRASH_CONCERN" {
		t.Fatalf("expected CRASH_CONCERN status, got %s", status)
	}

	if _, err := testDB.Exec(`SELECT pgwf.complete_unheld_job($1, $2)`, jobID, "operator"); err != nil {
		t.Fatalf("complete_unheld_job failed: %v", err)
	}

	if hasRow(t, `SELECT 1 FROM pgwf.jobs WHERE job_id = $1`, jobID) {
		t.Fatalf("expected job to be archived")
	}

	var archivedCount int
	if err := testDB.QueryRow(`SELECT count(*) FROM pgwf.jobs_archive WHERE job_id = $1`, jobID).Scan(&archivedCount); err != nil {
		t.Fatalf("count archived rows: %v", err)
	}
	if archivedCount != 1 {
		t.Fatalf("expected job archived exactly once, got %d", archivedCount)
	}
}

func leaseSingleJob(t *testing.T, caps []string) (string, string) {
	t.Helper()
	rows, err := testDB.Query(`SELECT job_id, lease_id FROM pgwf.get_work($1, $2, $3, $4)`, "worker", pqStringArray(caps), 30, 1)
	if err != nil {
		t.Fatalf("get_work failed: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatalf("expected a row")
	}

	var jobID, leaseID string
	if err := rows.Scan(&jobID, &leaseID); err != nil {
		t.Fatalf("scan: %v", err)
	}

	if rows.Next() {
		t.Fatalf("expected a single row")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iteration error: %v", err)
	}

	return jobID, leaseID
}

func hasRow(t *testing.T, query string, args ...interface{}) bool {
	t.Helper()
	rows, err := testDB.Query(query, args...)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()
	has := rows.Next()
	if !has {
		if err := rows.Err(); err != nil {
			t.Fatalf("row iteration error: %v", err)
		}
	}
	return has
}

func setTrace(t *testing.T, enabled bool) {
	t.Helper()
	if _, err := testDB.Exec(`SELECT pgwf.set_trace($1)`, enabled); err != nil {
		t.Fatalf("set_trace(%v): %v", enabled, err)
	}
}

func traceCount(t *testing.T) int {
	t.Helper()
	var count int
	if err := testDB.QueryRow(`SELECT count(*) FROM pgwf.jobs_trace`).Scan(&count); err != nil {
		t.Fatalf("count traces: %v", err)
	}
	return count
}

func expectErrorContains(t *testing.T, err error, substr string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error containing %q, got nil", substr)
	}
	if !strings.Contains(err.Error(), substr) {
		t.Fatalf("expected error to contain %q, got %v", substr, err)
	}
}

func expectNoWork(t *testing.T, caps []string) {
	t.Helper()
	rows, err := testDB.Query(`SELECT job_id FROM pgwf.get_work($1, $2, $3, $4)`, "worker", pqStringArray(caps), 30, 1)
	if err != nil {
		t.Fatalf("get_work: %v", err)
	}
	defer rows.Close()
	if rows.Next() {
		var jobID string
		if err := rows.Scan(&jobID); err != nil {
			t.Fatalf("scan job: %v", err)
		}
		t.Fatalf("expected no work, but leased job %s", jobID)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("get_work rows err: %v", err)
	}
}

func setNotify(t *testing.T, enabled bool) {
	t.Helper()
	if _, err := testDB.Exec(`SELECT pgwf.set_notify($1)`, enabled); err != nil {
		t.Fatalf("set_notify(%v): %v", enabled, err)
	}
}

func newNotifyListener(t *testing.T, channel string) *pq.Listener {
	t.Helper()
	listener := pq.NewListener(testConnStr, 100*time.Millisecond, time.Second, nil)
	if err := listener.Listen(channel); err != nil {
		t.Fatalf("listen %s: %v", channel, err)
	}
	return listener
}

func drainNotifications(listener *pq.Listener) {
	for {
		select {
		case <-listener.Notify:
		default:
			return
		}
	}
}

func waitForNotification(listener *pq.Listener, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-listener.Notify:
		return true
	case <-timer.C:
		return false
	}
}

// pqStringArray converts a slice into driver.Valuer/Scanner compatible type without importing lib/pq everywhere.
type pqStringArray []string

func (a pqStringArray) Value() (driver.Value, error) {
	return pq.StringArray(a).Value()
}

func (a *pqStringArray) Scan(src interface{}) error {
	return (*pq.StringArray)(a).Scan(src)
}
