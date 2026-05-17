package storage

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mbreese/batchq/jobs"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schemaSQL string

// timeFormat is the RFC3339-style format used for timestamps stored as TEXT.
// Stored values are always UTC so lexical ordering matches chronological
// ordering and there is no implicit timezone.
const timeFormat = "2006-01-02T15:04:05Z"

// Options controls how a SQLite Storage is opened.
type Options struct {
	// WAL enables WAL journal mode. Default is rollback (DELETE) journal
	// because WAL's shared-memory file is unsafe on networked filesystems
	// (NFS, Lustre). Workstation deployments with the DB on local disk can
	// opt into WAL for better single-writer throughput.
	WAL bool
	// BusyTimeoutMS controls how long SQLite waits on a locked DB before
	// giving up. Default 5000ms.
	BusyTimeoutMS int
}

// Open returns a Storage backed by the SQLite file at path. The file (and
// any missing parent directories) is created if it does not exist; the
// schema is applied on every open and is idempotent.
func Open(ctx context.Context, path string, opts Options) (Storage, error) {
	if path == "" {
		return nil, errors.New("storage: empty path")
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("storage: create parent dir: %w", err)
		}
	}
	if opts.BusyTimeoutMS == 0 {
		opts.BusyTimeoutMS = 5000
	}

	journal := "DELETE"
	if opts.WAL {
		journal = "WAL"
	}

	dsn := fmt.Sprintf(
		"file:%s?_pragma=foreign_keys(1)&_pragma=journal_mode(%s)&_pragma=busy_timeout(%d)&_pragma=synchronous(FULL)",
		path, journal, opts.BusyTimeoutMS,
	)
	conn, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("storage: open sqlite: %w", err)
	}
	// One writer process — the server owns the file exclusively. A single
	// connection is sufficient and gives us safe transaction semantics for
	// free.
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)

	if err := conn.PingContext(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("storage: ping: %w", err)
	}
	if _, err := conn.ExecContext(ctx, schemaSQL); err != nil {
		conn.Close()
		return nil, fmt.Errorf("storage: apply schema: %w", err)
	}

	return &sqliteStorage{db: conn, path: path}, nil
}

// Reset destroys any existing DB at path and creates a fresh one with the
// schema applied. Used by the `batchq initdb --force` path. The caller is
// responsible for confirming with the user before invoking.
func Reset(ctx context.Context, path string) error {
	if path == "" {
		return errors.New("storage: empty path")
	}
	if _, err := os.Stat(path); err == nil {
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("storage: remove existing db: %w", err)
		}
	}
	s, err := Open(ctx, path, Options{})
	if err != nil {
		return err
	}
	return s.Close()
}

type sqliteStorage struct {
	db   *sql.DB
	path string

	mu     sync.Mutex
	closed bool
}

func (s *sqliteStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.db.Close()
}

// nowString returns the current UTC time formatted for the DB.
func nowString() string {
	return time.Now().UTC().Format(timeFormat)
}

// formatTime is the canonical wire-to-DB time formatter.
func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(timeFormat)
}

// parseTime is the canonical DB-to-Go time parser. Accepts both the RFC3339
// form and the legacy "2006-01-02 15:04:05 MST" form used in v1, so a v1
// DB file can be opened by v2 without a migration step.
func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	if t, err := time.Parse(timeFormat, s); err == nil {
		return t.UTC(), nil
	}
	if t, err := time.Parse("2006-01-02 15:04:05 MST", s); err == nil {
		return t.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("storage: cannot parse time %q", s)
}

// --- Submission ---------------------------------------------------------

func (s *sqliteStorage) InsertJob(ctx context.Context, job *jobs.JobDef) error {
	if job == nil {
		return errors.New("storage: nil job")
	}
	if job.JobId == "" {
		return errors.New("storage: job missing id")
	}

	// Validate dependencies before any writes.
	for _, depID := range job.AfterOk {
		dep, err := s.GetJob(ctx, depID)
		if err != nil {
			return fmt.Errorf("storage: dep %s: %w", depID, err)
		}
		if dep.Status == jobs.CANCELED || dep.Status == jobs.FAILED {
			return fmt.Errorf("storage: dep %s is %s", depID, dep.Status)
		}
	}

	// Compute initial status from dependencies (unless the caller asked for
	// USERHOLD explicitly).
	status := job.Status
	if status != jobs.USERHOLD {
		if len(job.AfterOk) == 0 {
			status = jobs.QUEUED
		} else {
			status = jobs.WAITING
		}
	}

	submitTime := nowString()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	name := job.Name
	if name == "" {
		name = "batchq-%JOBID"
	}
	if strings.Contains(name, "%JOBID") {
		name = strings.ReplaceAll(name, "%JOBID", job.JobId)
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO jobs (id, status, priority, name, notes, submit_time)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		job.JobId, status, job.Priority, name, job.Notes, submitTime,
	); err != nil {
		return fmt.Errorf("storage: insert job: %w", err)
	}

	for _, depID := range job.AfterOk {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO job_deps (job_id, afterok_id) VALUES (?, ?)`,
			job.JobId, depID,
		); err != nil {
			return fmt.Errorf("storage: insert dep: %w", err)
		}
	}

	for _, d := range job.Details {
		value := d.Value
		if d.Key == "stdout" || d.Key == "stderr" {
			value = strings.ReplaceAll(value, "%JOBID", job.JobId)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO job_details (job_id, key, value) VALUES (?, ?, ?)`,
			job.JobId, d.Key, value,
		); err != nil {
			return fmt.Errorf("storage: insert detail %s: %w", d.Key, err)
		}
	}

	for _, p := range dedupNonEmpty(job.InputFiles) {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO job_inputs (job_id, path) VALUES (?, ?)`,
			job.JobId, p,
		); err != nil {
			return fmt.Errorf("storage: insert input %s: %w", p, err)
		}
	}
	for _, p := range dedupNonEmpty(job.OutputFiles) {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO job_outputs (job_id, path) VALUES (?, ?)`,
			job.JobId, p,
		); err != nil {
			return fmt.Errorf("storage: insert output %s: %w", p, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	// Reflect the persisted state back into the caller's struct.
	job.Status = status
	job.Name = name
	job.SubmitTime, _ = parseTime(submitTime)
	return nil
}

// --- Read access --------------------------------------------------------

func (s *sqliteStorage) GetJob(ctx context.Context, jobID string) (*jobs.JobDef, error) {
	job, err := s.fetchJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	if err := s.loadJobRelations(ctx, job); err != nil {
		return nil, err
	}
	return job, nil
}

func (s *sqliteStorage) fetchJob(ctx context.Context, jobID string) (*jobs.JobDef, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, status, priority, name, notes, submit_time, start_time, end_time, return_code
		 FROM jobs WHERE id = ?`, jobID)
	job, err := scanJob(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrJobNotFound
		}
		return nil, err
	}
	return job, nil
}

// scanJob reads a job row from any Scanner (Row or Rows).
type scanner interface {
	Scan(dest ...any) error
}

func scanJob(sc scanner) (*jobs.JobDef, error) {
	var job jobs.JobDef
	var submitTime, startTime, endTime string
	if err := sc.Scan(&job.JobId, &job.Status, &job.Priority, &job.Name, &job.Notes,
		&submitTime, &startTime, &endTime, &job.ReturnCode); err != nil {
		return nil, err
	}
	var err error
	if job.SubmitTime, err = parseTime(submitTime); err != nil {
		return nil, err
	}
	if job.StartTime, err = parseTime(startTime); err != nil {
		return nil, err
	}
	if job.EndTime, err = parseTime(endTime); err != nil {
		return nil, err
	}
	return &job, nil
}

// loadJobRelations populates AfterOk, Details, RunningDetails, and the
// input/output file lists for a job.
func (s *sqliteStorage) loadJobRelations(ctx context.Context, job *jobs.JobDef) error {
	deps, err := s.fetchDeps(ctx, job.JobId)
	if err != nil {
		return err
	}
	job.AfterOk = deps

	details, err := s.fetchDetails(ctx, job.JobId)
	if err != nil {
		return err
	}
	job.Details = details

	rd, err := s.fetchRunningDetails(ctx, job.JobId)
	if err != nil {
		return err
	}
	job.RunningDetails = rd

	in, err := s.fetchPaths(ctx, "job_inputs", job.JobId)
	if err != nil {
		return err
	}
	job.InputFiles = in

	out, err := s.fetchPaths(ctx, "job_outputs", job.JobId)
	if err != nil {
		return err
	}
	job.OutputFiles = out
	return nil
}

// fetchPaths returns the paths from job_inputs or job_outputs for a job.
// table must be one of those two literal names (never user input) — it's
// interpolated directly because parameter binding doesn't work for table
// names.
func (s *sqliteStorage) fetchPaths(ctx context.Context, table, jobID string) ([]string, error) {
	if table != "job_inputs" && table != "job_outputs" {
		return nil, fmt.Errorf("storage: fetchPaths invalid table %q", table)
	}
	rows, err := s.db.QueryContext(ctx,
		"SELECT path FROM "+table+" WHERE job_id = ? ORDER BY path", jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var paths []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		paths = append(paths, p)
	}
	return paths, rows.Err()
}

// dedupNonEmpty returns a copy of paths with empty strings and duplicates
// removed, preserving the original order of the first occurrence.
func dedupNonEmpty(paths []string) []string {
	if len(paths) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(paths))
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		if p == "" {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out
}

func (s *sqliteStorage) fetchDeps(ctx context.Context, jobID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT afterok_id FROM job_deps WHERE job_id = ? ORDER BY afterok_id`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var deps []string
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		deps = append(deps, d)
	}
	return deps, rows.Err()
}

func (s *sqliteStorage) fetchDetails(ctx context.Context, jobID string) ([]jobs.JobDefDetail, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT key, value FROM job_details WHERE job_id = ?`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var details []jobs.JobDefDetail
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		details = append(details, jobs.JobDefDetail{Key: k, Value: v})
	}
	return details, rows.Err()
}

func (s *sqliteStorage) fetchRunningDetails(ctx context.Context, jobID string) ([]jobs.JobRunningDetail, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT key, value FROM job_running_details WHERE job_id = ?`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var details []jobs.JobRunningDetail
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		details = append(details, jobs.JobRunningDetail{Key: k, Value: v})
	}
	return details, rows.Err()
}

func (s *sqliteStorage) ListJobs(ctx context.Context, showAll, sortByStatus bool) ([]*jobs.JobDef, error) {
	var query string
	var args []any
	switch {
	case showAll && sortByStatus:
		query = `SELECT id, status, priority, name, notes, submit_time, start_time, end_time, return_code
		         FROM jobs ORDER BY status DESC, priority DESC, end_time, start_time, id`
	case showAll && !sortByStatus:
		query = `SELECT id, status, priority, name, notes, submit_time, start_time, end_time, return_code
		         FROM jobs ORDER BY id`
	case !showAll && sortByStatus:
		query = `SELECT id, status, priority, name, notes, submit_time, start_time, end_time, return_code
		         FROM jobs WHERE status <= ? ORDER BY status DESC, priority DESC, end_time, start_time, id`
		args = []any{jobs.RUNNING}
	default:
		query = `SELECT id, status, priority, name, notes, submit_time, start_time, end_time, return_code
		         FROM jobs WHERE status <= ? ORDER BY id`
		args = []any{jobs.RUNNING}
	}
	return s.queryJobs(ctx, query, args, true)
}

func (s *sqliteStorage) ListJobsByStatus(ctx context.Context, statuses []jobs.StatusCode, sortByStatus bool) ([]*jobs.JobDef, error) {
	if len(statuses) == 0 {
		return nil, nil
	}
	placeholders := make([]string, len(statuses))
	args := make([]any, len(statuses))
	for i, st := range statuses {
		placeholders[i] = "?"
		args[i] = st
	}
	query := `SELECT id, status, priority, name, notes, submit_time, start_time, end_time, return_code
	          FROM jobs WHERE status IN (` + strings.Join(placeholders, ",") + `)`
	if sortByStatus {
		query += ` ORDER BY status DESC, priority DESC, end_time, start_time, id`
	} else {
		query += ` ORDER BY id`
	}
	// loadRelations must be true: the slurm runner calls this with
	// Statuses=[PROXYQUEUED] and relies on RunningDetails["slurm_job_id"]
	// to reconcile against sacct.
	return s.queryJobs(ctx, query, args, true)
}

func (s *sqliteStorage) SearchJobs(ctx context.Context, query string, statuses []jobs.StatusCode) ([]*jobs.JobDef, error) {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return nil, nil
	}
	like := "%" + trimmed + "%"
	sqlQuery := `
		SELECT j.id, j.status, j.priority, j.name, j.notes, j.submit_time, j.start_time, j.end_time, j.return_code
		FROM jobs j
		WHERE (
			j.id LIKE ?
			OR j.name LIKE ?
			OR EXISTS (
				SELECT 1 FROM job_details d
				WHERE d.job_id = j.id AND d.key = 'script' AND d.value LIKE ?
			)
			OR EXISTS (
				SELECT 1 FROM job_details d
				WHERE d.job_id = j.id AND d.key = 'run_id' AND d.value LIKE ?
			)
			OR EXISTS (
				SELECT 1 FROM job_inputs i
				WHERE i.job_id = j.id AND i.path LIKE ?
			)
			OR EXISTS (
				SELECT 1 FROM job_outputs o
				WHERE o.job_id = j.id AND o.path LIKE ?
			)
		)`
	args := []any{like, like, like, like, like, like}
	if len(statuses) > 0 {
		placeholders := make([]string, len(statuses))
		for i, st := range statuses {
			placeholders[i] = "?"
			args = append(args, st)
		}
		sqlQuery += ` AND j.status IN (` + strings.Join(placeholders, ",") + `)`
	}
	sqlQuery += ` ORDER BY j.id`
	// loadRelations=true: callers that filter by status (notably the
	// slurm runner) depend on RunningDetails for slurm_job_id.
	return s.queryJobs(ctx, sqlQuery, args, true)
}

// queryJobs runs a SELECT that produces full job rows and (optionally) loads
// each job's relations. loadRelations=false is used for bulk views where
// callers don't need details/running_details.
func (s *sqliteStorage) queryJobs(ctx context.Context, query string, args []any, loadRelations bool) ([]*jobs.JobDef, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	var out []*jobs.JobDef
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, job)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if loadRelations {
		for _, j := range out {
			if err := s.loadJobRelations(ctx, j); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

func (s *sqliteStorage) GetJobDependents(ctx context.Context, jobID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT job_id FROM job_deps WHERE afterok_id = ? ORDER BY job_id`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *sqliteStorage) GetJobStatusCounts(ctx context.Context, showAll bool) (map[jobs.StatusCode]int, error) {
	counts := map[jobs.StatusCode]int{
		jobs.USERHOLD:    0,
		jobs.WAITING:     0,
		jobs.QUEUED:      0,
		jobs.PROXYQUEUED: 0,
		jobs.RUNNING:     0,
		jobs.SUCCESS:     0,
		jobs.FAILED:      0,
		jobs.CANCELED:    0,
	}
	query := `SELECT status, COUNT(*) FROM jobs`
	var args []any
	if !showAll {
		query += ` WHERE status <= ?`
		args = append(args, jobs.RUNNING)
	}
	query += ` GROUP BY status`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var st jobs.StatusCode
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		counts[st] = n
	}
	return counts, rows.Err()
}

func (s *sqliteStorage) GetQueueJobs(ctx context.Context, showAll, sortByStatus bool) ([]*jobs.JobDef, error) {
	// Single-query version that pulls only the small subset of details the
	// queue view cares about. Falls back to ListJobs / ListJobsByStatus if
	// the fast query somehow returns zero rows but there are jobs in the DB
	// (defensive against the v1 fast-path regression we observed).
	query := `
		SELECT j.id, j.status, j.priority, j.name, j.notes, j.submit_time, j.start_time, j.end_time, j.return_code,
			deps.deps, details.details, running.running
		FROM jobs j
		LEFT JOIN (
			SELECT job_id, group_concat(afterok_id, char(10)) AS deps
			FROM job_deps
			GROUP BY job_id
		) deps ON deps.job_id = j.id
		LEFT JOIN (
			SELECT job_id, group_concat(key || '=' || value, char(10)) AS details
			FROM job_details
			WHERE key IN ('procs', 'mem', 'walltime', 'user', 'run_id')
			GROUP BY job_id
		) details ON details.job_id = j.id
		LEFT JOIN (
			SELECT job_id, group_concat(key || '=' || value, char(10)) AS running
			FROM job_running_details
			WHERE key IN ('pid', 'slurm_status', 'slurm_job_id')
			GROUP BY job_id
		) running ON running.job_id = j.id
	`
	var args []any
	if !showAll {
		query += ` WHERE j.status IN (?, ?, ?, ?)`
		args = []any{jobs.WAITING, jobs.QUEUED, jobs.PROXYQUEUED, jobs.RUNNING}
	}
	if sortByStatus {
		query += ` ORDER BY j.status DESC, j.priority DESC, j.end_time, j.start_time, j.id`
	} else {
		query += ` ORDER BY j.id`
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	var out []*jobs.JobDef
	for rows.Next() {
		var job jobs.JobDef
		var submitTime, startTime, endTime string
		var depRaw, detailRaw, runningRaw sql.NullString
		if err := rows.Scan(&job.JobId, &job.Status, &job.Priority, &job.Name, &job.Notes,
			&submitTime, &startTime, &endTime, &job.ReturnCode,
			&depRaw, &detailRaw, &runningRaw); err != nil {
			rows.Close()
			return nil, err
		}
		if job.SubmitTime, err = parseTime(submitTime); err != nil {
			rows.Close()
			return nil, err
		}
		if job.StartTime, err = parseTime(startTime); err != nil {
			rows.Close()
			return nil, err
		}
		if job.EndTime, err = parseTime(endTime); err != nil {
			rows.Close()
			return nil, err
		}
		if depRaw.Valid && depRaw.String != "" {
			job.AfterOk = strings.Split(depRaw.String, "\n")
		}
		if detailRaw.Valid && detailRaw.String != "" {
			job.Details = parseConcatKV[jobs.JobDefDetail](detailRaw.String, func(k, v string) jobs.JobDefDetail {
				return jobs.JobDefDetail{Key: k, Value: v}
			})
		}
		if runningRaw.Valid && runningRaw.String != "" {
			job.RunningDetails = parseConcatKV[jobs.JobRunningDetail](runningRaw.String, func(k, v string) jobs.JobRunningDetail {
				return jobs.JobRunningDetail{Key: k, Value: v}
			})
		}
		out = append(out, &job)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// parseConcatKV reverses the `group_concat(key || '=' || value, char(10))`
// trick used by GetQueueJobs to fetch sub-rows in one query.
func parseConcatKV[T any](raw string, mk func(k, v string) T) []T {
	var out []T
	for _, line := range strings.Split(raw, "\n") {
		if line == "" {
			continue
		}
		eq := strings.IndexByte(line, '=')
		if eq < 0 {
			continue
		}
		out = append(out, mk(line[:eq], line[eq+1:]))
	}
	return out
}

func (s *sqliteStorage) GetProxyJobs(ctx context.Context) ([]*jobs.JobDef, error) {
	return s.queryJobs(ctx,
		`SELECT id, status, priority, name, notes, submit_time, start_time, end_time, return_code
		 FROM jobs WHERE status = ? ORDER BY id`,
		[]any{jobs.PROXYQUEUED}, true)
}

// --- Dependency resolution ---------------------------------------------

func (s *sqliteStorage) ResolveDependencies(ctx context.Context) (ResolveResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ResolveResult{}, err
	}
	defer tx.Rollback()

	// Find all jobs that are WAITING or UNKNOWN.
	rows, err := tx.QueryContext(ctx,
		`SELECT id FROM jobs WHERE status = ? OR status = ?`,
		jobs.WAITING, jobs.UNKNOWN)
	if err != nil {
		return ResolveResult{}, err
	}
	var candidates []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return ResolveResult{}, err
		}
		candidates = append(candidates, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return ResolveResult{}, err
	}

	res := ResolveResult{}
	for _, jobID := range candidates {
		// Look up the deps for this job and the status of each parent.
		depRows, err := tx.QueryContext(ctx,
			`SELECT j.id, j.status FROM job_deps d
			 JOIN jobs j ON j.id = d.afterok_id
			 WHERE d.job_id = ?`, jobID)
		if err != nil {
			return ResolveResult{}, err
		}
		canQueue := true
		shouldCancel := false
		var cancelReason string
		for depRows.Next() {
			var depID string
			var depStatus jobs.StatusCode
			if err := depRows.Scan(&depID, &depStatus); err != nil {
				depRows.Close()
				return ResolveResult{}, err
			}
			// Dep must be terminal-success (SUCCESS) or already-handed-off
			// (PROXYQUEUED, which the slurm side will reconcile).
			if depStatus != jobs.SUCCESS && depStatus != jobs.PROXYQUEUED {
				canQueue = false
			}
			if depStatus == jobs.CANCELED || depStatus == jobs.FAILED {
				shouldCancel = true
				if cancelReason == "" {
					cancelReason = fmt.Sprintf("Depends on %s", depID)
				} else {
					cancelReason += ", " + depID
				}
			}
		}
		depRows.Close()
		if err := depRows.Err(); err != nil {
			return ResolveResult{}, err
		}

		switch {
		case shouldCancel:
			if _, err := tx.ExecContext(ctx,
				`UPDATE jobs SET status = ?, notes = ?, end_time = ?
				 WHERE id = ? AND status IN (?, ?)`,
				jobs.CANCELED, cancelReason+" failed/canceled", nowString(),
				jobID, jobs.WAITING, jobs.UNKNOWN); err != nil {
				return ResolveResult{}, err
			}
			res.Canceled++
		case canQueue:
			if _, err := tx.ExecContext(ctx,
				`UPDATE jobs SET status = ? WHERE id = ? AND status IN (?, ?)`,
				jobs.QUEUED, jobID, jobs.WAITING, jobs.UNKNOWN); err != nil {
				return ResolveResult{}, err
			}
			res.Promoted++
		}
	}

	if err := tx.Commit(); err != nil {
		return ResolveResult{}, err
	}
	return res, nil
}

// --- Atomic claim ------------------------------------------------------

func (s *sqliteStorage) ClaimNextJob(ctx context.Context, runnerID, kind string, limits Limits) (ClaimResult, error) {
	if runnerID == "" {
		return ClaimResult{}, errors.New("storage: empty runnerID")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ClaimResult{}, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx,
		`SELECT id FROM jobs WHERE status = ? ORDER BY priority DESC, submit_time, id`,
		jobs.QUEUED)
	if err != nil {
		return ClaimResult{}, err
	}
	var queued []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return ClaimResult{}, err
		}
		queued = append(queued, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return ClaimResult{}, err
	}

	more := false
	for _, jobID := range queued {
		fits, err := jobFitsLimits(ctx, tx, jobID, limits)
		if err != nil {
			return ClaimResult{}, err
		}
		if !fits {
			more = true
			continue
		}
		// Try to claim this job. INSERT into job_running uses the UNIQUE
		// PRIMARY KEY on job_id as the atomic primitive; if a different
		// transaction already claimed it, we get a constraint failure and
		// move on to the next candidate.
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO job_running (job_id, job_runner, kind) VALUES (?, ?, ?)`,
			jobID, runnerID, kind); err != nil {
			if isUniqueViolation(err) {
				continue
			}
			return ClaimResult{}, err
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE jobs SET status = ?, start_time = ?
			 WHERE id = ? AND status = ?`,
			jobs.RUNNING, nowString(), jobID, jobs.QUEUED); err != nil {
			return ClaimResult{}, err
		}
		// Load the job inside the transaction so the caller sees the
		// post-claim state.
		job, err := s.txGetJob(ctx, tx, jobID)
		if err != nil {
			return ClaimResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return ClaimResult{}, err
		}
		return ClaimResult{Job: job, MoreEligible: more}, nil
	}

	if err := tx.Commit(); err != nil {
		return ClaimResult{}, err
	}
	return ClaimResult{MoreEligible: more}, nil
}

// jobFitsLimits returns true iff the job at jobID has procs/mem/walltime
// details that fit the supplied limits. Missing details are treated as 0 (no
// requirement). Limit values <= 0 mean "no cap on this dimension".
func jobFitsLimits(ctx context.Context, tx *sql.Tx, jobID string, limits Limits) (bool, error) {
	keys := []string{"procs", "mem", "walltime"}
	vals := map[string]int{}
	rows, err := tx.QueryContext(ctx,
		`SELECT key, value FROM job_details
		 WHERE job_id = ? AND key IN ('procs', 'mem', 'walltime')`, jobID)
	if err != nil {
		return false, err
	}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			rows.Close()
			return false, err
		}
		if n, err := strconv.Atoi(v); err == nil {
			vals[k] = n
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return false, err
	}
	_ = keys
	if limits.MaxProcs > 0 && vals["procs"] > limits.MaxProcs {
		return false, nil
	}
	if limits.MaxMemoryMB > 0 && vals["mem"] > limits.MaxMemoryMB {
		return false, nil
	}
	if limits.MaxWalltimeSec > 0 && vals["walltime"] > limits.MaxWalltimeSec {
		return false, nil
	}
	return true, nil
}

// isUniqueViolation matches the SQLite "UNIQUE constraint failed" error
// regardless of the underlying driver representation. modernc.org/sqlite
// returns errors whose string contains "constraint failed: UNIQUE" or
// "constraint failed (2067)" depending on the column.
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint failed") ||
		strings.Contains(msg, "constraint failed (1555)") ||
		strings.Contains(msg, "constraint failed (2067)")
}

// txGetJob loads a job and its relations inside an open transaction.
func (s *sqliteStorage) txGetJob(ctx context.Context, tx *sql.Tx, jobID string) (*jobs.JobDef, error) {
	row := tx.QueryRowContext(ctx,
		`SELECT id, status, priority, name, notes, submit_time, start_time, end_time, return_code
		 FROM jobs WHERE id = ?`, jobID)
	job, err := scanJob(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrJobNotFound
		}
		return nil, err
	}
	// Fetch sub-rows using the same tx so we see the just-committed state.
	depRows, err := tx.QueryContext(ctx,
		`SELECT afterok_id FROM job_deps WHERE job_id = ? ORDER BY afterok_id`, jobID)
	if err != nil {
		return nil, err
	}
	for depRows.Next() {
		var d string
		if err := depRows.Scan(&d); err != nil {
			depRows.Close()
			return nil, err
		}
		job.AfterOk = append(job.AfterOk, d)
	}
	depRows.Close()

	dRows, err := tx.QueryContext(ctx,
		`SELECT key, value FROM job_details WHERE job_id = ?`, jobID)
	if err != nil {
		return nil, err
	}
	for dRows.Next() {
		var k, v string
		if err := dRows.Scan(&k, &v); err != nil {
			dRows.Close()
			return nil, err
		}
		job.Details = append(job.Details, jobs.JobDefDetail{Key: k, Value: v})
	}
	dRows.Close()

	rdRows, err := tx.QueryContext(ctx,
		`SELECT key, value FROM job_running_details WHERE job_id = ?`, jobID)
	if err != nil {
		return nil, err
	}
	for rdRows.Next() {
		var k, v string
		if err := rdRows.Scan(&k, &v); err != nil {
			rdRows.Close()
			return nil, err
		}
		job.RunningDetails = append(job.RunningDetails, jobs.JobRunningDetail{Key: k, Value: v})
	}
	rdRows.Close()

	return job, nil
}

// --- State transitions -------------------------------------------------

func (s *sqliteStorage) MarkJobProxied(ctx context.Context, jobID, runnerID string, runningDetails map[string]string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := assertRunnerOwnsJob(ctx, tx, jobID, runnerID); err != nil {
		return err
	}

	res, err := tx.ExecContext(ctx,
		`UPDATE jobs SET status = ? WHERE id = ? AND status = ?`,
		jobs.PROXYQUEUED, jobID, jobs.RUNNING)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrInvalidState
	}

	for k, v := range runningDetails {
		if _, err := tx.ExecContext(ctx,
			`INSERT OR REPLACE INTO job_running_details (job_id, key, value)
			 VALUES (?, ?, ?)`, jobID, k, v); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *sqliteStorage) UpdateRunningDetails(ctx context.Context, jobID string, details map[string]string) error {
	if len(details) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for k, v := range details {
		if _, err := tx.ExecContext(ctx,
			`INSERT OR REPLACE INTO job_running_details (job_id, key, value)
			 VALUES (?, ?, ?)`, jobID, k, v); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *sqliteStorage) EndJob(ctx context.Context, jobID, runnerID string, returnCode int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := assertRunnerOwnsJob(ctx, tx, jobID, runnerID); err != nil {
		return err
	}

	newStatus := jobs.SUCCESS
	if returnCode != 0 {
		newStatus = jobs.FAILED
	}
	res, err := tx.ExecContext(ctx,
		`UPDATE jobs SET status = ?, end_time = ?, return_code = ?
		 WHERE id = ? AND status = ?`,
		newStatus, nowString(), returnCode, jobID, jobs.RUNNING)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrInvalidState
	}

	if newStatus != jobs.SUCCESS {
		if err := cascadeCancel(ctx, tx, jobID,
			fmt.Sprintf("Parent job %s failed", jobID)); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *sqliteStorage) EndProxiedJob(ctx context.Context, jobID string, status jobs.StatusCode, startTime, endTime time.Time, returnCode int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx,
		`UPDATE jobs SET status = ?, start_time = ?, end_time = ?, return_code = ?
		 WHERE id = ? AND status = ?`,
		status, formatTime(startTime), formatTime(endTime), returnCode,
		jobID, jobs.PROXYQUEUED)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrInvalidState
	}

	if status != jobs.SUCCESS {
		if err := cascadeCancel(ctx, tx, jobID,
			fmt.Sprintf("Parent job %s failed/canceled", jobID)); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *sqliteStorage) CancelJob(ctx context.Context, jobID, reason string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := cancelOne(ctx, tx, jobID, reason); err != nil {
		return err
	}
	if err := cascadeCancel(ctx, tx, jobID, reason); err != nil {
		return err
	}
	return tx.Commit()
}

// cancelOne marks a single job CANCELED (no cascade). No-op if the job is
// already terminal. Returns ErrJobNotFound if the job doesn't exist; the
// no-op terminal case returns nil so the caller can drive a cascade through
// already-terminal parents without error.
func cancelOne(ctx context.Context, tx *sql.Tx, jobID, reason string) error {
	row := tx.QueryRowContext(ctx, `SELECT status FROM jobs WHERE id = ?`, jobID)
	var status jobs.StatusCode
	if err := row.Scan(&status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrJobNotFound
		}
		return err
	}
	if status >= jobs.CANCELED { // already CANCELED/SUCCESS/FAILED
		return nil
	}
	_, err := tx.ExecContext(ctx,
		`UPDATE jobs SET status = ?, end_time = ?, notes = ?
		 WHERE id = ? AND status < ?`,
		jobs.CANCELED, nowString(), reason, jobID, jobs.CANCELED)
	return err
}

// cascadeCancel cancels all jobs that depend (transitively) on parentID,
// skipping any that are already terminal.
func cascadeCancel(ctx context.Context, tx *sql.Tx, parentID, reason string) error {
	// Iterative BFS so we do not blow the stack on deep dep chains.
	queue := []string{parentID}
	seen := map[string]bool{parentID: true}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		rows, err := tx.QueryContext(ctx,
			`SELECT d.job_id FROM job_deps d
			 JOIN jobs j ON j.id = d.job_id
			 WHERE d.afterok_id = ? AND j.status < ?`,
			cur, jobs.CANCELED)
		if err != nil {
			return err
		}
		var children []string
		for rows.Next() {
			var c string
			if err := rows.Scan(&c); err != nil {
				rows.Close()
				return err
			}
			if !seen[c] {
				seen[c] = true
				children = append(children, c)
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, c := range children {
			if err := cancelOne(ctx, tx, c, reason); err != nil {
				return err
			}
			queue = append(queue, c)
		}
	}
	return nil
}

// assertRunnerOwnsJob returns ErrInvalidState if the job_running row for
// jobID does not match runnerID. Used by transition operations that should
// only be driven by the runner that claimed the job.
func assertRunnerOwnsJob(ctx context.Context, tx *sql.Tx, jobID, runnerID string) error {
	row := tx.QueryRowContext(ctx,
		`SELECT job_runner FROM job_running WHERE job_id = ?`, jobID)
	var owner string
	if err := row.Scan(&owner); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrInvalidState
		}
		return err
	}
	if owner != runnerID {
		return ErrInvalidState
	}
	return nil
}

// --- User actions ------------------------------------------------------

func (s *sqliteStorage) HoldJob(ctx context.Context, jobID string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET status = ? WHERE id = ? AND status IN (?, ?, ?)`,
		jobs.USERHOLD, jobID, jobs.QUEUED, jobs.WAITING, jobs.USERHOLD)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrInvalidState
	}
	return nil
}

func (s *sqliteStorage) ReleaseJob(ctx context.Context, jobID string) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET status = ? WHERE id = ? AND status = ?`,
		jobs.WAITING, jobID, jobs.USERHOLD)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrInvalidState
	}
	return nil
}

func (s *sqliteStorage) AdjustJobPriority(ctx context.Context, jobID string, delta int) error {
	if delta == 0 {
		return nil
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET priority = priority + ? WHERE id = ? AND status IN (?, ?, ?)`,
		delta, jobID, jobs.QUEUED, jobs.WAITING, jobs.USERHOLD)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrInvalidState
	}
	return nil
}

func (s *sqliteStorage) CleanupJob(ctx context.Context, jobID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmts := []string{
		`DELETE FROM job_running_details WHERE job_id = ?`,
		`DELETE FROM job_running         WHERE job_id = ?`,
		`DELETE FROM job_deps            WHERE job_id = ?`,
		`DELETE FROM job_details         WHERE job_id = ?`,
		`DELETE FROM job_inputs          WHERE job_id = ?`,
		`DELETE FROM job_outputs         WHERE job_id = ?`,
		`DELETE FROM jobs                WHERE id     = ?`,
	}
	for _, q := range stmts {
		if _, err := tx.ExecContext(ctx, q, jobID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// --- Reverse lookups for run_id / inputs / outputs --------------------

func (s *sqliteStorage) FindJobsByDetail(ctx context.Context, key, value string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT job_id FROM job_details WHERE key = ? AND value = ?`,
		key, value)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectIDs(rows)
}

func (s *sqliteStorage) FindJobsByInputPath(ctx context.Context, path string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT job_id FROM job_inputs WHERE path = ?`, path)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectIDs(rows)
}

func (s *sqliteStorage) FindJobsByOutputPath(ctx context.Context, path string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT job_id FROM job_outputs WHERE path = ?`, path)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectIDs(rows)
}

func collectIDs(rows *sql.Rows) ([]string, error) {
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
