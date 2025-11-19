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
