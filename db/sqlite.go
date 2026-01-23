package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mbreese/batchq/jobs"
	"github.com/mbreese/batchq/support"
)

const defaultUpdateFrequency = 2 * time.Minute

type SqliteBatchQ struct {
	fname           string
	dbConn          *sql.DB
	connectCount    int
	conLock         sync.Mutex
	lastUpdate      *time.Time
	updateFrequency time.Duration
}

func openSqlite3(fname string) *SqliteBatchQ {
	// f, err := os.Open(fname)
	// if err == nil {
	// 	f.Close()
	// } else {
	// 	// InitDB(fname)
	// }

	db := SqliteBatchQ{fname: fname, updateFrequency: defaultUpdateFrequency}
	return &db
}

func initSqlite3(fname string, force bool) error {
	fmt.Printf("Initializing sqlite db: %s\n", fname)

	if f, err := os.Open(fname); err == nil {
		f.Close()
		if !force {
			return errors.New("DB file exists! You must remove it or give the --force option")
		} else {
			os.Remove(fname)
		}
	}

	if _, err := os.Stat(path.Dir(fname)); os.IsNotExist(err) {
		os.MkdirAll(path.Dir(fname), 0755) // creates intermediate directories too
	}

	db, err := sql.Open("sqlite3", fname)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Not recommended for network drives...
	//    PRAGMA journal_mode=WAL;

	sql := `
	PRAGMA foreign_keys = ON;

	DROP TABLE IF EXISTS job_running_details;
	DROP TABLE IF EXISTS job_running;
	DROP TABLE IF EXISTS job_deps;
	DROP TABLE IF EXISTS job_details;
	DROP TABLE IF EXISTS jobs;

	CREATE TABLE jobs (
		id TEXT PRIMARY KEY,
		status INT DEFAULT 0 NOT NULL,
		priority INT DEFAULT 0 NOT NULL,
		name TEXT,
		notes TEXT,
		submit_time TEXT DEFAULT "",
		start_time TEXT DEFAULT "",
		end_time TEXT DEFAULT "",
		return_code INT DEFAULT 0
			);

	CREATE TABLE job_details (
		job_id TEXT REFERENCES jobs(id),
		key TEXT,
		value TEXT,
		PRIMARY KEY (job_id, key)
			);

	CREATE TABLE job_deps (
		job_id TEXT REFERENCES jobs(id),
		afterok_id TEXT REFERENCES jobs(id),
		PRIMARY KEY (job_id, afterok_id)
			);

	CREATE TABLE job_running (
		job_id TEXT REFERENCES jobs(id) UNIQUE PRIMARY KEY,
		job_runner TEXT
			);

	CREATE TABLE job_running_details (
		job_id TEXT REFERENCES jobs(id),
		key TEXT,
		value TEXT,
		PRIMARY KEY (job_id, key)
			);
	`

	// fmt.Println(sql)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err = db.ExecContext(ctx, sql)
	if err != nil {
		// log.Fatalf("Exec error: %v", err)
		return err
	}
	fmt.Println("Done.")
	return nil
}

// Each method will connect on it's own, this is because there is no row-level locking of the database, and
// this db could be accessed from different processes (potentially over a network share). So, in order to
// keep locking to a minimum, we'll just open the db and close it for each function call.

func (db *SqliteBatchQ) connect() *sql.DB {
	// debug.PrintStack()

	db.conLock.Lock()
	if db.dbConn != nil {
		db.connectCount++
		// fmt.Printf("connect existing (%d):\n", db.connectCount)
		db.conLock.Unlock()
		return db.dbConn
	}
	// fmt.Printf("Opening database: %s\n", db.fname)
	if _, err := os.Open(db.fname); err != nil {
		log.Fatal("Missing database. Please initialize it first!")
	}

	conn, err := sql.Open("sqlite3", fmt.Sprintf("file://%s?_txlock=immediate&_busy_timeout=5000&_synchronous=full&_fk=true", db.fname))
	// conn, err := sql.Open("sqlite3", fmt.Sprintf("file://%s?_journal_mode=wal&_txlock=immediate&_busy_timeout=5000&_synchronous=full&_fk=true", db.fname))
	//conn, err := sql.Open("sqlite3", fmt.Sprintf("%s?_busy_timeout=5000", db.fname))
	if err != nil {
		log.Fatal(err)
	}

	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)
	db.dbConn = conn
	db.connectCount = 1
	// fmt.Printf("connect new (%d):\n", db.connectCount)
	db.conLock.Unlock()
	return conn
}

func (db *SqliteBatchQ) close() {
	db.conLock.Lock()
	db.connectCount--
	if db.connectCount <= 0 {
		if db.dbConn != nil {
			// fmt.Printf("disconnect (%d) closing:\n", db.connectCount)
			db.dbConn.Close()
			db.dbConn = nil
		}
	} else {
		// fmt.Printf("disconnect (%d):\n", db.connectCount)
	}
	db.conLock.Unlock()
}

func (db *SqliteBatchQ) SubmitJob(ctx context.Context, job *jobs.JobDef) *jobs.JobDef {
	conn := db.connect()
	defer db.close()

	for _, depid := range job.AfterOk {
		dep := db.GetJob(ctx, depid)
		if dep == nil || dep.Status == jobs.CANCELED || dep.Status == jobs.FAILED {
			// bad dependency
			return nil
		}
	}

	newStatus := job.Status
	if newStatus != jobs.USERHOLD {
		if len(job.AfterOk) == 0 {
			newStatus = jobs.QUEUED
		} else {
			newStatus = jobs.WAITING
		}
	}

	jobId := support.NewUUID()
	job.JobId = jobId

	if tx, err := conn.BeginTx(ctx, nil); err != nil {
		log.Println(err)
		return nil
	} else {
		sql := "INSERT INTO jobs (id,status,name,notes,submit_time) VALUES (?,?,?,?,?)"
		_, err := tx.ExecContext(ctx, sql, jobId, newStatus, job.Name, "", support.GetNowUTCString())
		if err != nil {
			tx.Rollback()
			log.Fatal(err)

		} else {
			job.Status = newStatus
			if job.Name == "" {
				job.Name = "batchq-%JOBID"
			}
			if strings.Contains(job.Name, "%JOBID") {
				job.Name = strings.Replace(job.Name, "%JOBID", jobId, -1)
				sql2 := "UPDATE jobs SET name = ? WHERE id = ?"
				_, err3 := tx.ExecContext(ctx, sql2, job.Name, jobId)
				if err3 != nil {
					tx.Rollback()
					log.Fatal(err3)
				}
			}
			for _, depid := range job.AfterOk {
				if _, err3 := tx.ExecContext(ctx, "INSERT INTO job_deps (job_id, afterok_id) VALUES (?,?)", job.JobId, depid); err3 != nil {
					tx.Rollback()
					log.Fatal(err3)
				}
			}
			for _, detail := range job.Details {
				if detail.Key == "stderr" || detail.Key == "stdout" {
					detail.Value = strings.Replace(detail.Value, "%JOBID", jobId, -1)
				}
				if _, err3 := tx.ExecContext(ctx, "INSERT INTO job_details (job_id, key, value) VALUES (?,?,?)", job.JobId, detail.Key, detail.Value); err3 != nil {
					tx.Rollback()
					log.Fatal(err3)
				}
			}
		}
		if err2 := tx.Commit(); err2 != nil {
			tx.Rollback()
		}

	}
	return job
}

func (db *SqliteBatchQ) GetJobs(ctx context.Context, showAll bool, sortByStatus bool) []*jobs.JobDef {
	conn := db.connect()
	defer db.close()

	var sql string
	var args []any
	if showAll {
		if sortByStatus {
			sql = "SELECT id,status,priority,name,notes,submit_time,start_time,end_time,return_code FROM jobs ORDER BY status DESC, priority DESC, end_time, start_time, id"
		} else {
			sql = "SELECT id,status,priority,name,notes,submit_time,start_time,end_time,return_code FROM jobs ORDER BY id"
		}
		args = []any{}
	} else {
		if sortByStatus {
			sql = "SELECT id,status,priority,name,notes,submit_time,start_time,end_time,return_code FROM jobs WHERE status <= ? ORDER BY status DESC, priority DESC, end_time, start_time, id"
		} else {
			sql = "SELECT id,status,priority,name,notes,submit_time,start_time,end_time,return_code FROM jobs WHERE status <= ? ORDER BY id"
		}
		args = []any{jobs.RUNNING}
	}

	rows, err := conn.QueryContext(ctx, sql, args...)
	if err != nil {
		log.Fatal(err)
	}

	var retjobs []*jobs.JobDef

	for rows.Next() {
		var job jobs.JobDef
		var submitTime string
		var startTime string
		var endTime string

		err := rows.Scan(&job.JobId, &job.Status, &job.Priority, &job.Name, &job.Notes, &submitTime, &startTime, &endTime, &job.ReturnCode)
		if err != nil {
			log.Fatal(err)
		}

		// We need to parse the stored timestamps...
		if submitTime != "" {
			job.SubmitTime, err = time.Parse("2006-01-02 15:04:05 MST", submitTime)
			if err != nil {
				log.Fatal(err)
			}
		}
		if startTime != "" {
			job.StartTime, err = time.Parse("2006-01-02 15:04:05 MST", startTime)
			if err != nil {
				log.Fatal(err)
			}
		}
		if endTime != "" {
			job.EndTime, err = time.Parse("2006-01-02 15:04:05 MST", endTime)
			if err != nil {
				log.Fatal(err)
			}
		}

		retjobs = append(retjobs, &job)
	}
	rows.Close()

	for _, job := range retjobs {
		// Load job dependencies (we are the child, looking for parents)
		sql2 := "SELECT afterok_id FROM job_deps WHERE job_id = ? ORDER BY afterok_id"
		// TODO: confirm afterok_id exists
		rows2, err2 := conn.QueryContext(ctx, sql2, job.JobId)
		if err2 != nil {
			log.Fatal(err2)
		}
		var deps []string

		for rows2.Next() {
			var parentId string
			rows2.Scan(&parentId)
			deps = append(deps, parentId)
		}
		rows2.Close()

		job.AfterOk = deps

		// Load job dependencies (we are the child, looking for parents)
		sql3 := "SELECT key, value FROM job_details WHERE job_id = ?"
		// TODO: confirm afterok_id exists
		rows3, err3 := conn.QueryContext(ctx, sql3, job.JobId)
		if err3 != nil {
			log.Fatal(err3)
		}
		var details []jobs.JobDefDetail

		for rows3.Next() {
			var key string
			var val string
			rows3.Scan(&key, &val)
			details = append(details, jobs.JobDefDetail{Key: key, Value: val})
		}
		rows3.Close()

		job.Details = details

		if job.Status == jobs.RUNNING || job.Status == jobs.PROXYQUEUED {
			sql4 := "SELECT key, value FROM job_running_details WHERE job_id = ?"
			rows4, err4 := conn.QueryContext(ctx, sql4, job.JobId)
			if err4 != nil {
				log.Fatal(err4)
			}
			var runningDetails []jobs.JobRunningDetail

			for rows4.Next() {
				var key string
				var val string
				rows4.Scan(&key, &val)
				runningDetails = append(runningDetails, jobs.JobRunningDetail{Key: key, Value: val})
			}
			rows4.Close()

			job.RunningDetails = runningDetails

		}
	}

	return retjobs
}

func (db *SqliteBatchQ) GetJobsByStatus(ctx context.Context, statuses []jobs.StatusCode, sortByStatus bool) []*jobs.JobDef {
	if len(statuses) == 0 {
		return []*jobs.JobDef{}
	}

	conn := db.connect()
	defer db.close()

	placeholders := make([]string, len(statuses))
	args := make([]any, len(statuses))
	for i, status := range statuses {
		placeholders[i] = "?"
		args[i] = status
	}

	sql := "SELECT id,status,priority,name,notes,submit_time,start_time,end_time,return_code FROM jobs WHERE status IN (" + strings.Join(placeholders, ",") + ")"
	if sortByStatus {
		sql += " ORDER BY status DESC, priority DESC, end_time, start_time, id"
	} else {
		sql += " ORDER BY id"
	}

	rows, err := conn.QueryContext(ctx, sql, args...)
	if err != nil {
		log.Fatal(err)
	}

	var retjobs []*jobs.JobDef

	for rows.Next() {
		var job jobs.JobDef
		var submitTime string
		var startTime string
		var endTime string

		err := rows.Scan(&job.JobId, &job.Status, &job.Priority, &job.Name, &job.Notes, &submitTime, &startTime, &endTime, &job.ReturnCode)
		if err != nil {
			log.Fatal(err)
		}

		if submitTime != "" {
			job.SubmitTime, err = time.Parse("2006-01-02 15:04:05 MST", submitTime)
			if err != nil {
				log.Fatal(err)
			}
		}
		if startTime != "" {
			job.StartTime, err = time.Parse("2006-01-02 15:04:05 MST", startTime)
			if err != nil {
				log.Fatal(err)
			}
		}
		if endTime != "" {
			job.EndTime, err = time.Parse("2006-01-02 15:04:05 MST", endTime)
			if err != nil {
				log.Fatal(err)
			}
		}

		retjobs = append(retjobs, &job)
	}
	rows.Close()

	return retjobs
}

func (db *SqliteBatchQ) GetJobStatusCounts(ctx context.Context, showAll bool) map[jobs.StatusCode]int {
	conn := db.connect()
	defer db.close()

	counts := map[jobs.StatusCode]int{
		jobs.USERHOLD:   0,
		jobs.WAITING:    0,
		jobs.QUEUED:     0,
		jobs.PROXYQUEUED: 0,
		jobs.RUNNING:    0,
		jobs.SUCCESS:    0,
		jobs.FAILED:     0,
		jobs.CANCELED:   0,
	}

	sql := "SELECT status, COUNT(*) FROM jobs"
	var args []any
	if !showAll {
		sql += " WHERE status <= ?"
		args = []any{jobs.RUNNING}
	}
	sql += " GROUP BY status"

	rows, err := conn.QueryContext(ctx, sql, args...)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var status jobs.StatusCode
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			log.Fatal(err)
		}
		counts[status] = count
	}

	return counts
}

func (db *SqliteBatchQ) GetQueueJobs(ctx context.Context, showAll bool, sortByStatus bool) []*jobs.JobDef {
	conn := db.connect()
	defer db.close()

	query := `
	SELECT j.id, j.status, j.priority, j.name, j.notes, j.submit_time, j.start_time, j.end_time, j.return_code,
		deps.deps, details.details, running.running
	FROM jobs j
	LEFT JOIN (
		SELECT job_id, group_concat(afterok_id, '\n') AS deps
		FROM job_deps
		GROUP BY job_id
	) deps ON deps.job_id = j.id
	LEFT JOIN (
		SELECT job_id, group_concat(key || '=' || value, '\n') AS details
		FROM job_details
		WHERE key IN ('procs', 'mem', 'walltime', 'user')
		GROUP BY job_id
	) details ON details.job_id = j.id
	LEFT JOIN (
		SELECT job_id, group_concat(key || '=' || value, '\n') AS running
		FROM job_running_details
		WHERE key IN ('pid', 'slurm_status', 'slurm_job_id')
		GROUP BY job_id
	) running ON running.job_id = j.id
	`
	var args []any
	if !showAll {
		query += " WHERE j.status IN (?, ?, ?, ?)"
		args = []any{jobs.WAITING, jobs.QUEUED, jobs.PROXYQUEUED, jobs.RUNNING}
	}
	if sortByStatus {
		query += " ORDER BY j.status DESC, j.priority DESC, j.end_time, j.start_time, j.id"
	} else {
		query += " ORDER BY j.id"
	}

	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || (ctx != nil && ctx.Err() != nil) {
			log.Printf("GetQueueJobs query canceled: %v", err)
			return nil
		}
		log.Fatal(err)
	}
	defer rows.Close()

	var retjobs []*jobs.JobDef

	for rows.Next() {
		var job jobs.JobDef
		var submitTime string
		var startTime string
		var endTime string
		var depRaw sql.NullString
		var detailRaw sql.NullString
		var runningRaw sql.NullString

		err := rows.Scan(&job.JobId, &job.Status, &job.Priority, &job.Name, &job.Notes, &submitTime, &startTime, &endTime, &job.ReturnCode, &depRaw, &detailRaw, &runningRaw)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || (ctx != nil && ctx.Err() != nil) {
				log.Printf("GetQueueJobs scan canceled: %v", err)
				return retjobs
			}
			log.Fatal(err)
		}

		if submitTime != "" {
			job.SubmitTime, err = time.Parse("2006-01-02 15:04:05 MST", submitTime)
			if err != nil {
				log.Fatal(err)
			}
		}
		if startTime != "" {
			job.StartTime, err = time.Parse("2006-01-02 15:04:05 MST", startTime)
			if err != nil {
				log.Fatal(err)
			}
		}
		if endTime != "" {
			job.EndTime, err = time.Parse("2006-01-02 15:04:05 MST", endTime)
			if err != nil {
				log.Fatal(err)
			}
		}

		if depRaw.Valid && depRaw.String != "" {
			job.AfterOk = strings.Split(depRaw.String, "\n")
		}
		if detailRaw.Valid && detailRaw.String != "" {
			job.Details = parseJobDetails(detailRaw.String)
		}
		if runningRaw.Valid && runningRaw.String != "" {
			job.RunningDetails = parseRunningDetails(runningRaw.String)
		}

		retjobs = append(retjobs, &job)
	}
	if err := rows.Err(); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || (ctx != nil && ctx.Err() != nil) {
			log.Printf("GetQueueJobs rows canceled: %v", err)
			return retjobs
		}
		log.Fatal(err)
	}

	if len(retjobs) == 0 {
		var total int
		row := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM jobs")
		if err := row.Scan(&total); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || (ctx != nil && ctx.Err() != nil) {
				log.Printf("GetQueueJobs count canceled: %v", err)
				return retjobs
			}
			log.Fatal(err)
		}
		if total > 0 {
			if showAll {
				log.Printf("GetQueueJobs fast path returned 0 rows (showAll=%t, total=%d); falling back to legacy query", showAll, total)
			} else {
				var activeCount int
				row := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM jobs WHERE status IN (?, ?, ?, ?)",
					jobs.WAITING, jobs.QUEUED, jobs.PROXYQUEUED, jobs.RUNNING)
				if err := row.Scan(&activeCount); err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || (ctx != nil && ctx.Err() != nil) {
						log.Printf("GetQueueJobs active count canceled: %v", err)
						return retjobs
					}
					log.Fatal(err)
				}
				log.Printf("GetQueueJobs fast path returned 0 rows (showAll=%t, active=%d, total=%d); falling back to legacy query", showAll, activeCount, total)
			}

			if explainRows, err := conn.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query, args...); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || (ctx != nil && ctx.Err() != nil) {
					log.Printf("GetQueueJobs explain canceled: %v", err)
					return retjobs
				}
				log.Fatal(err)
			} else {
				for explainRows.Next() {
					var id int
					var parent int
					var notused int
					var detail string
					if err := explainRows.Scan(&id, &parent, &notused, &detail); err != nil {
						if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || (ctx != nil && ctx.Err() != nil) {
							log.Printf("GetQueueJobs explain scan canceled: %v", err)
							explainRows.Close()
							return retjobs
						}
						log.Fatal(err)
					}
					log.Printf("GetQueueJobs fast path plan: %d %d %d %s", id, parent, notused, detail)
				}
				if err := explainRows.Err(); err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || (ctx != nil && ctx.Err() != nil) {
						log.Printf("GetQueueJobs explain rows canceled: %v", err)
						explainRows.Close()
						return retjobs
					}
					log.Fatal(err)
				}
				explainRows.Close()
			}

			rows, err := conn.QueryContext(ctx, "SELECT CAST(status AS TEXT), typeof(status), COUNT(*) FROM jobs GROUP BY status, typeof(status) ORDER BY status")
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || (ctx != nil && ctx.Err() != nil) {
					log.Printf("GetQueueJobs histogram canceled: %v", err)
					return retjobs
				}
				log.Fatal(err)
			}
			for rows.Next() {
				var statusText string
				var statusType string
				var count int
				if err := rows.Scan(&statusText, &statusType, &count); err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || (ctx != nil && ctx.Err() != nil) {
						log.Printf("GetQueueJobs histogram scan canceled: %v", err)
						rows.Close()
						return retjobs
					}
					log.Fatal(err)
				}
				log.Printf("GetQueueJobs status histogram: status=%s type=%s count=%d", statusText, statusType, count)
			}
			if err := rows.Err(); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || (ctx != nil && ctx.Err() != nil) {
					log.Printf("GetQueueJobs histogram rows canceled: %v", err)
					rows.Close()
					return retjobs
				}
				log.Fatal(err)
			}
			rows.Close()

			if showAll {
				return db.GetJobs(ctx, showAll, sortByStatus)
			}
			return db.GetJobsByStatus(ctx, []jobs.StatusCode{
				jobs.WAITING,
				jobs.QUEUED,
				jobs.PROXYQUEUED,
				jobs.RUNNING,
			}, sortByStatus)
		}
	}

	return retjobs
}

func parseJobDetails(raw string) []jobs.JobDefDetail {
	var details []jobs.JobDefDetail
	for _, line := range strings.Split(raw, "\n") {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		details = append(details, jobs.JobDefDetail{Key: parts[0], Value: parts[1]})
	}
	return details
}

func parseRunningDetails(raw string) []jobs.JobRunningDetail {
	var details []jobs.JobRunningDetail
	for _, line := range strings.Split(raw, "\n") {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		details = append(details, jobs.JobRunningDetail{Key: parts[0], Value: parts[1]})
	}
	return details
}

// func (db *SqliteBatchQ) FetchNext(ctx context.Context, freeProc int, freeMemMB int, freeTimeSec int) (*jobs.JobDef, bool) {
func (db *SqliteBatchQ) FetchNext(ctx context.Context, limits []JobLimit) (*jobs.JobDef, bool) {
	// First, update all WAITING jobs to QUEUED if they have no outstanding dependencies
	// fmt.Println("Updating queue...")
	db.updateQueue(ctx)
	// fmt.Println("done...")

	// Next, look for a QUEUED job that first the proc/mem/time limits
	// fmt.Println("getting connection...", db.connectCount)
	conn := db.connect()
	// fmt.Println("got connection...", db.connectCount)
	defer db.close()

	fmt.Println("Getting jobs...")
	sql := "SELECT id FROM jobs WHERE status = ? ORDER BY priority DESC, submit_time, id;"
	var queueJobIds []string
	if rows, err := conn.QueryContext(ctx, sql, jobs.QUEUED); err != nil {
		log.Fatal(err)
	} else {
		for rows.Next() {
			var jobId string

			err := rows.Scan(&jobId)
			if err != nil {
				log.Fatal(err)
			}
			// fmt.Printf("found queued job: %d\n", jobId)
			queueJobIds = append(queueJobIds, jobId)
		}
		rows.Close()
	}
	// fmt.Println(queueJobIds)

	for _, jobId := range queueJobIds {
		// fmt.Println("Checking job requirements / limits for job: ", jobId)
		job := db.GetJob(ctx, jobId)
		pass := true

		for _, limit := range limits {
			if limit.value == -1 {
				continue
			}
			switch limit.limitType {
			case JobLimitTypeMemory:
				if val := job.GetDetail("mem", ""); val != "" {
					if jobMem, err := strconv.Atoi(val); err != nil {
						log.Fatal(err)
					} else {
						if jobMem > limit.value {
							pass = false
						}
					}
				}
			case JobLimitTypeProc:
				if val := job.GetDetail("procs", ""); val != "" {
					if jobProcs, err := strconv.Atoi(val); err != nil {
						log.Fatal(err)
					} else {
						if jobProcs > limit.value {
							pass = false
						}
					}
				}
			case JobLimitTypeTime:
				if val := job.GetDetail("walltime", ""); val != "" {
					if jobTime, err := strconv.Atoi(val); err != nil {
						log.Fatal(err)
					} else {
						if jobTime > limit.value {
							pass = false
						}
					}
				}
			}
		}

		// log.Printf("QUEUED job %d (%s, %s, %s) => (c:%t, m:%t, t:%t)\n",
		// 	jobId,
		// 	job.GetDetail("procs", ""),
		// 	job.GetDetail("mem", ""),
		// 	job.GetDetail("walltime", ""),
		// 	passProc, passMem, passTime)

		if pass {
			// log.Printf("Returning job %d for execution\n", job.JobId)
			return job, true
		}
	}

	return nil, len(queueJobIds) > 0
}

func (db *SqliteBatchQ) updateQueue(ctx context.Context, parentJobId ...string) {
	if len(parentJobId) == 0 && db.lastUpdate != nil {
		now := time.Now().UTC()
		if now.Sub(*db.lastUpdate) < db.updateFrequency {
			return
		}
	}

	conn := db.connect()
	defer db.close()

	var sql string
	var sqlArgs []any

	if len(parentJobId) == 0 {
		sql = "SELECT id FROM jobs WHERE status = ? OR status = ?"
		sqlArgs = []any{jobs.WAITING, jobs.UNKNOWN}
	} else {
		sql = "SELECT id FROM jobs, job_deps WHERE jobs.id = job_deps.job_id AND (jobs.status = ? OR jobs.status = ?) AND job_deps.afterok_id = ?"
		sqlArgs = []any{jobs.WAITING, jobs.UNKNOWN, parentJobId[0]}
	}

	var possibleJobIds []string
	var queueJobIds []string
	var cancelJobIds []string
	var cancelReasons []string
	if rows, err := conn.QueryContext(ctx, sql, sqlArgs...); err != nil {
		log.Fatal(err)
	} else {
		for rows.Next() {
			var jobId string

			err := rows.Scan(&jobId)
			if err != nil {
				log.Fatal(err)
			}
			// fmt.Printf("found waiting/unknown job: %d\n", jobId)
			possibleJobIds = append(possibleJobIds, jobId)
		}
		rows.Close()
	}

	if len(possibleJobIds) > 0 {
		fmt.Printf("found waiting/unknown jobs (n=%d)\n", len(possibleJobIds))

		for _, jobId := range possibleJobIds {
			job := db.GetJob(ctx, jobId)
			enqueue := true
			cancel := false
			cancelReason := ""
			fmt.Printf("  checking job %s dependencies (n=%d)\n", jobId, len(job.AfterOk))
			for _, depid := range job.AfterOk {
				dep := db.GetJob(ctx, depid)
				// was dep successful or successfully submitted elsewhere?
				if dep.Status != jobs.SUCCESS && dep.Status != jobs.PROXYQUEUED {
					enqueue = false
				}
				if dep.Status == jobs.CANCELED || dep.Status == jobs.FAILED {
					cancel = true
					if cancelReason == "" {
						cancelReason = fmt.Sprintf("Depends on %s", depid)
					} else {
						cancelReason = fmt.Sprintf("%s, %s", cancelReason, depid)
					}
					enqueue = false
				}
			}
			if cancel {
				cancelReason = cancelReason + " failed/canceled"
				cancelJobIds = append(cancelJobIds, jobId)
				cancelReasons = append(cancelReasons, cancelReason)
			} else if enqueue {
				// fmt.Printf("moving to queue: %d\n", jobId)
				queueJobIds = append(queueJobIds, jobId)
			}
		}
		// fmt.Printf("updating waiting => canceled jobs\n")
		for i, jobId := range cancelJobIds {
			sql2 := "UPDATE jobs SET status = ?, notes = ? WHERE id = ?"
			_, err := conn.ExecContext(ctx, sql2, jobs.CANCELED, cancelReasons[i], jobId)
			if err != nil {
				log.Fatal(err)
			}
			// fmt.Printf("moved to queue: %d\n", jobId)
		}
		// fmt.Printf("updating waiting => queued jobs\n")
		for _, jobId := range queueJobIds {
			sql2 := "UPDATE jobs SET status = ? WHERE id = ?"
			_, err := conn.ExecContext(ctx, sql2, jobs.QUEUED, jobId)
			if err != nil {
				log.Fatal(err)
			}
			// fmt.Printf("moved to queue: %d\n", jobId)
		}
	}

	// update the last run time...
	// but only if we are doing a full update (no parentJobId)
	if len(parentJobId) == 0 {
		now := time.Now().UTC()
		db.lastUpdate = &now
	}
}

func (db *SqliteBatchQ) GetJob(ctx context.Context, jobId string) *jobs.JobDef {
	conn := db.connect()
	defer db.close()

	sql := "SELECT id,status,priority,name,notes,submit_time,start_time,end_time,return_code FROM jobs WHERE id = ?"
	rows, err := conn.QueryContext(ctx, sql, jobId)
	if err != nil {
		log.Fatal(err)
	}

	if rows.Next() {
		var job jobs.JobDef
		var submitTime string
		var startTime string
		var endTime string

		err := rows.Scan(&job.JobId, &job.Status, &job.Priority, &job.Name, &job.Notes, &submitTime, &startTime, &endTime, &job.ReturnCode)
		if err != nil {
			log.Fatal(err)
		}
		rows.Close()
		// We need to parse the stored timestamps...
		if submitTime != "" {
			job.SubmitTime, err = time.Parse("2006-01-02 15:04:05 MST", submitTime)
			if err != nil {
				log.Fatal(err)
			}
		}
		if startTime != "" {
			job.StartTime, err = time.Parse("2006-01-02 15:04:05 MST", startTime)
			if err != nil {
				log.Fatal(err)
			}
		}
		if endTime != "" {
			job.EndTime, err = time.Parse("2006-01-02 15:04:05 MST", endTime)
			if err != nil {
				log.Fatal(err)
			}
		}

		// Load job dependencies (we are the child, looking for parents)
		sql2 := "SELECT afterok_id FROM job_deps WHERE job_id = ? ORDER BY afterok_id"
		rows2, err2 := conn.QueryContext(ctx, sql2, job.JobId)
		if err2 != nil {
			log.Fatal(err2)
		}
		var deps []string

		for rows2.Next() {
			var parentId string
			rows2.Scan(&parentId)
			deps = append(deps, parentId)
		}
		rows2.Close()

		job.AfterOk = deps

		// Load job details (procs, mem, env, etc...)
		sql3 := "SELECT key, value FROM job_details WHERE job_id = ?"
		rows3, err3 := conn.QueryContext(ctx, sql3, job.JobId)
		if err3 != nil {
			log.Fatal(err3)
		}
		var details []jobs.JobDefDetail

		for rows3.Next() {
			var key string
			var val string
			rows3.Scan(&key, &val)
			details = append(details, jobs.JobDefDetail{Key: key, Value: val})
		}
		rows3.Close()

		job.Details = details

		sql4 := "SELECT key, value FROM job_running_details WHERE job_id = ?"
		rows4, err4 := conn.QueryContext(ctx, sql4, job.JobId)
		if err4 != nil {
			log.Fatal(err4)
		}
		var runningDetails []jobs.JobRunningDetail

		for rows4.Next() {
			var key string
			var val string
			rows4.Scan(&key, &val)
			runningDetails = append(runningDetails, jobs.JobRunningDetail{Key: key, Value: val})
		}
		rows4.Close()

		job.RunningDetails = runningDetails

		return &job
	}
	rows.Close()
	return nil
}

func (db *SqliteBatchQ) GetJobDependents(ctx context.Context, jobId string) []string {
	conn := db.connect()
	defer db.close()

	sql := "SELECT job_id FROM job_deps WHERE afterok_id = ? ORDER BY job_id"
	rows, err := conn.QueryContext(ctx, sql, jobId)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var dependents []string
	for rows.Next() {
		var childId string
		if err := rows.Scan(&childId); err != nil {
			log.Fatal(err)
		}
		dependents = append(dependents, childId)
	}
	return dependents
}

func (db *SqliteBatchQ) CancelJob(ctx context.Context, jobId string, reason string) bool {
	conn := db.connect()
	defer db.close()

	job := db.GetJob(ctx, jobId)
	if job == nil {
		return false
	}
	switch job.Status {
	case jobs.CANCELED:
		// already canceled
		return false
	case jobs.SUCCESS, jobs.FAILED:
		// already done
		return false
	}

	sql2 := "UPDATE jobs SET status = ?, end_time = ?, notes = ? WHERE id = ? AND status < ?"
	res, err := conn.ExecContext(ctx, sql2, jobs.CANCELED, support.GetNowUTCString(), reason, jobId, jobs.CANCELED)
	if err != nil {
		log.Fatal(err)
	}

	// cancel any child jobs
	if rowcount, err2 := res.RowsAffected(); rowcount == 1 && err2 == nil {
		childIds := []string{}
		// Load job dependencies
		sql2 := "SELECT job_id FROM job_deps, jobs WHERE job_deps.afterok_id = ? AND job_deps.job_id = jobs.id AND jobs.status < ?"
		rows2, err2 := conn.QueryContext(ctx, sql2, jobId, jobs.CANCELED)
		if err2 != nil {
			log.Fatal(err2)
		}

		for rows2.Next() {
			var childId string
			rows2.Scan(&childId)
			childIds = append(childIds, childId)
		}
		rows2.Close()

		for _, cid := range childIds {
			// recursively delete
			if !db.CancelJob(ctx, cid, reason) {
				return false
			}
		}
		return true
	}
	return false
}
func (db *SqliteBatchQ) ProxyEndJob(ctx context.Context, jobId string, newStatus jobs.StatusCode, startTime string, endTime string, returnCode int) bool {
	conn := db.connect()
	defer db.close()

	sql2 := "UPDATE jobs SET status = ?, start_time = ?, end_time = ?, return_code = ? WHERE id = ? and status = ?"

	// fmt.Printf("updating job final status: %d\n", status)
	res, err := conn.ExecContext(ctx, sql2,
		newStatus,
		startTime,
		endTime,
		returnCode,
		jobId,
		jobs.PROXYQUEUED)

	if err != nil {
		log.Fatal(err)
		return false
	}

	if rowcount, err2 := res.RowsAffected(); rowcount == 1 && err2 == nil {
		// fmt.Printf("done\n")
		// db.scanQueue(ctx, jobId, status == jobs.SUCCESS)

		// if the status is CANCELED or ERROR, look for child jobs to update their queue status
		// cancel any child jobs
		if newStatus != jobs.SUCCESS {
			childIds := []string{}
			// Load job dependencies
			sql2 := "SELECT job_id FROM job_deps, jobs WHERE job_deps.afterok_id = ? AND job_deps.job_id = jobs.id AND jobs.status < ?"
			rows2, err2 := conn.QueryContext(ctx, sql2, jobId, jobs.CANCELED)
			if err2 != nil {
				log.Fatal(err2)
			}

			for rows2.Next() {
				var childId string
				rows2.Scan(&childId)
				childIds = append(childIds, childId)
			}
			rows2.Close()

			for _, cid := range childIds {
				// recursively delete
				if !db.CancelJob(ctx, cid, fmt.Sprintf("Parent job %s failed/canceled", jobId)) {
					return false
				}
			}
		}
		// force an update of the queue on next call
		db.lastUpdate = nil
		return true
	}
	// fmt.Printf("done??\n")
	return false
}

func (db *SqliteBatchQ) ProxyQueueJob(ctx context.Context, jobId string, jobRunner string, runDetail map[string]string) bool {
	conn := db.connect()

	sql := "INSERT INTO job_running (job_id, job_runner) VALUES (?,?)"
	_, err := conn.ExecContext(ctx, sql, jobId, jobRunner)
	if err != nil {
		db.close()
		return false
	}

	db.close()

	// The UNIQUE constraint on the row should mean we are the ones that inserted
	// this row. But we will double check.
	time.Sleep(time.Duration((50 + rand.Intn(100))) * time.Millisecond)

	conn = db.connect()
	defer db.close()

	sql1 := "SELECT job_runner FROM job_running WHERE job_id = ?"
	rows1, err1 := conn.QueryContext(ctx, sql1, jobId)
	if err1 != nil {
		return false
	}

	for rows1.Next() {
		var dbJobRunner string
		rows1.Scan(&dbJobRunner)

		if jobRunner != dbJobRunner {
			rows1.Close()
			// oops, we aren't the runner of record. bailout.
			return false
		}
	}
	rows1.Close()

	if tx, err := conn.BeginTx(ctx, nil); err == nil {
		sql2 := "UPDATE jobs SET status = ? WHERE id = ?"

		_, err2 := tx.ExecContext(ctx, sql2, jobs.PROXYQUEUED, jobId)
		if err2 != nil {
			tx.Rollback()
			return false
		}

		for k, v := range runDetail {
			sql3 := "INSERT INTO job_running_details (job_id, key, value) VALUES (?,?,?)"

			_, err3 := tx.ExecContext(ctx, sql3, jobId, k, v)
			if err3 != nil {
				tx.Rollback()
				return false
			}
		}
		if err2 := tx.Commit(); err2 != nil {
			tx.Rollback()
			return false
		}
	}

	// TODO: Update child jobs to QUEUED too?
	db.updateQueue(ctx, jobId)
	return true
}

func (db *SqliteBatchQ) UpdateJobRunningDetails(ctx context.Context, jobId string, details map[string]string) bool {
	conn := db.connect()
	defer db.close()

	if tx, err := conn.BeginTx(ctx, nil); err == nil {
		for k, v := range details {
			sql3 := "INSERT OR REPLACE INTO job_running_details (job_id, key, value) VALUES (?,?,?)"

			_, err3 := tx.ExecContext(ctx, sql3, jobId, k, v)
			if err3 != nil {
				tx.Rollback()
				return false
			}
		}
		if err2 := tx.Commit(); err2 != nil {
			tx.Rollback()
			return false
		}
	}
	return true
}

func (db *SqliteBatchQ) StartJob(ctx context.Context, jobId string, jobRunner string, runDetail map[string]string) bool {
	conn := db.connect()

	sql := "INSERT INTO job_running (job_id, job_runner) VALUES (?,?)"
	_, err := conn.ExecContext(ctx, sql, jobId, jobRunner)
	if err != nil {
		db.close()
		return false
	}

	db.close()

	// The UNIQUE constraint on the row should mean we are the ones that inserted
	// this row. But we will double check.
	time.Sleep(time.Duration((50 + rand.Intn(100))) * time.Millisecond)

	conn = db.connect()
	defer db.close()

	sql1 := "SELECT job_runner FROM job_running WHERE job_id = ?"
	rows1, err1 := conn.QueryContext(ctx, sql1, jobId)
	if err1 != nil {
		return false
	}

	for rows1.Next() {
		var dbJobRunner string
		rows1.Scan(&dbJobRunner)

		if jobRunner != dbJobRunner {
			rows1.Close()
			// oops, we aren't the runner of record. bailout.
			return false
		}
	}
	rows1.Close()

	if tx, err := conn.BeginTx(ctx, nil); err == nil {
		sql2 := "UPDATE jobs SET status = ?, start_time = ? WHERE id = ?"

		_, err2 := tx.ExecContext(ctx, sql2, jobs.RUNNING, support.GetNowUTCString(), jobId)
		if err2 != nil {
			tx.Rollback()
			return false
		}

		for k, v := range runDetail {
			sql3 := "INSERT INTO job_running_details (job_id, key, value) VALUES (?,?,?)"

			_, err3 := tx.ExecContext(ctx, sql3, jobId, k, v)
			if err3 != nil {
				tx.Rollback()
				return false
			}
		}
		if err2 := tx.Commit(); err2 != nil {
			tx.Rollback()
			return false
		}
	}

	return true
}

func (db *SqliteBatchQ) EndJob(ctx context.Context, jobId string, jobRunner string, returnCode int) bool {
	conn := db.connect()
	defer db.close()

	sql1 := "SELECT job_runner FROM job_running WHERE job_id = ?"
	rows1, err1 := conn.QueryContext(ctx, sql1, jobId)
	if err1 != nil {
		log.Fatal(err1)
	}

	for rows1.Next() {
		var dbJobRunner string
		rows1.Scan(&dbJobRunner)

		if jobRunner != dbJobRunner {
			// oops, we aren't the runner of record. bailout.
			fmt.Println("Attempted to end job from a different runner.")
			rows1.Close()
			return false
		}
	}
	rows1.Close()

	// MAYBE: remove job_runner records here if necessary... probably nice
	//        to keep them around, but this is where you'd remove them. As
	//        a bonus, canceled jobs will also end up here, so you could
	//        remove the records here too...

	sql2 := "UPDATE jobs SET status = ?, end_time = ?, return_code = ? WHERE id = ? and status = ?"

	status := jobs.SUCCESS
	if returnCode != 0 {
		status = jobs.FAILED
	}
	// fmt.Printf("updating job final status: %d\n", status)
	res, err := conn.ExecContext(ctx, sql2, status, support.GetNowUTCString(), returnCode, jobId, jobs.RUNNING)
	if err != nil {
		log.Fatal(err)
	}

	if rowcount, err2 := res.RowsAffected(); rowcount == 1 && err2 == nil {
		// fmt.Printf("done\n")
		// db.scanQueue(ctx, jobId, status == jobs.SUCCESS)

		// force an update of the queue on next call
		db.updateQueue(ctx, jobId)

		return true
	}
	// fmt.Printf("done??\n")
	return false
}

func (db *SqliteBatchQ) Close() {
	db.conLock.Lock()
	db.connectCount = 0
	if db.dbConn != nil {
		db.dbConn.Close()
		db.dbConn = nil
	}
	db.conLock.Unlock()
}

func (db *SqliteBatchQ) CleanupJob(ctx context.Context, jobId string) bool {
	conn := db.connect()
	defer db.close()

	sql := []string{
		"DELETE FROM job_running_details WHERE job_id = ?",
		"DELETE FROM job_running WHERE job_id = ?",
		"DELETE FROM job_deps WHERE job_id = ?",
		"DELETE FROM job_details WHERE job_id = ?",
		"DELETE FROM jobs WHERE id = ?",
	}

	if tx, err := conn.BeginTx(ctx, nil); err != nil {
		log.Fatal(err)
	} else {
		for _, s := range sql {
			if _, err := tx.ExecContext(ctx, s, jobId); err != nil {
				tx.Rollback()
				log.Fatal(err)
			}
		}
		if err2 := tx.Commit(); err2 != nil {
			tx.Rollback()
			return false
		}
	}
	return true
}

// increase the priority for a queued/held/waiting job to the top of the queue
func (db *SqliteBatchQ) TopJob(ctx context.Context, jobId string) bool {
	conn := db.connect()
	defer db.close()

	sql2 := "UPDATE jobs SET priority = max(0, priority + 1) WHERE id = ? AND (status = ? OR status  = ? OR status  = ?)"
	res, err := conn.ExecContext(ctx, sql2, jobId, jobs.QUEUED, jobs.WAITING, jobs.USERHOLD)
	if err != nil {
		log.Fatal(err)
	}
	if rowcount, err2 := res.RowsAffected(); rowcount == 1 && err2 == nil {
		return true
	}
	return false
}

// decrease the priority for a queued/held/waiting job
func (db *SqliteBatchQ) NiceJob(ctx context.Context, jobId string) bool {
	conn := db.connect()
	defer db.close()

	sql2 := "UPDATE jobs SET priority = min(0, priority - 1) WHERE id = ? AND (status = ? OR status  = ? OR status  = ?)"
	res, err := conn.ExecContext(ctx, sql2, jobId, jobs.QUEUED, jobs.WAITING, jobs.USERHOLD)
	if err != nil {
		log.Fatal(err)
	}
	if rowcount, err2 := res.RowsAffected(); rowcount == 1 && err2 == nil {
		return true
	}
	return false
}

func (db *SqliteBatchQ) HoldJob(ctx context.Context, jobId string) bool {
	conn := db.connect()
	defer db.close()

	sql2 := "UPDATE jobs SET status = ? WHERE id = ? AND (status = ? OR status  = ? OR status  = ?)"
	res, err := conn.ExecContext(ctx, sql2, jobs.USERHOLD, jobId, jobs.QUEUED, jobs.WAITING, jobs.USERHOLD)
	if err != nil {
		log.Fatal(err)
	}
	if rowcount, err2 := res.RowsAffected(); rowcount == 1 && err2 == nil {
		return true
	}
	return false
}

func (db *SqliteBatchQ) ReleaseJob(ctx context.Context, jobId string) bool {
	conn := db.connect()
	defer db.close()

	sql2 := "UPDATE jobs SET status = ? WHERE id = ? AND status = ?"
	res, err := conn.ExecContext(ctx, sql2, jobs.WAITING, jobId, jobs.USERHOLD)
	if err != nil {
		log.Fatal(err)
	}
	if rowcount, err2 := res.RowsAffected(); rowcount == 1 && err2 == nil {
		// force an update of the queue on next call
		db.lastUpdate = nil
		return true
	}
	return false
}

func (db *SqliteBatchQ) GetProxyJobs(ctx context.Context) []*jobs.JobDef {
	conn := db.connect()
	defer db.close()

	sql := "SELECT id,status,name,notes,submit_time,start_time,end_time,return_code FROM jobs WHERE status = ? ORDER BY id"
	args := []any{jobs.PROXYQUEUED}

	rows, err := conn.QueryContext(ctx, sql, args...)
	if err != nil {
		log.Fatal(err)
	}

	var retjobs []*jobs.JobDef

	for rows.Next() {
		var job jobs.JobDef
		var submitTime string
		var startTime string
		var endTime string

		err := rows.Scan(&job.JobId, &job.Status, &job.Name, &job.Notes, &submitTime, &startTime, &endTime, &job.ReturnCode)
		if err != nil {
			log.Fatal(err)
		}

		// We need to parse the stored timestamps...
		if submitTime != "" {
			job.SubmitTime, err = time.Parse("2006-01-02 15:04:05 MST", submitTime)
			if err != nil {
				log.Fatal(err)
			}
		}
		if startTime != "" {
			job.StartTime, err = time.Parse("2006-01-02 15:04:05 MST", startTime)
			if err != nil {
				log.Fatal(err)
			}
		}
		if endTime != "" {
			job.EndTime, err = time.Parse("2006-01-02 15:04:05 MST", endTime)
			if err != nil {
				log.Fatal(err)
			}
		}

		retjobs = append(retjobs, &job)
	}
	rows.Close()

	for _, job := range retjobs {
		// Load job dependencies (we are the child, looking for parents)
		sql2 := "SELECT afterok_id FROM job_deps WHERE job_id = ? ORDER BY afterok_id"
		// TODO: confirm afterok_id exists
		rows2, err2 := conn.QueryContext(ctx, sql2, job.JobId)
		if err2 != nil {
			log.Fatal(err2)
		}
		var deps []string

		for rows2.Next() {
			var parentId string
			rows2.Scan(&parentId)
			deps = append(deps, parentId)
		}
		rows2.Close()

		job.AfterOk = deps

		// Load job dependencies (we are the child, looking for parents)
		sql3 := "SELECT key, value FROM job_details WHERE job_id = ?"
		// TODO: confirm afterok_id exists
		rows3, err3 := conn.QueryContext(ctx, sql3, job.JobId)
		if err3 != nil {
			log.Fatal(err3)
		}
		var details []jobs.JobDefDetail

		for rows3.Next() {
			var key string
			var val string
			rows3.Scan(&key, &val)
			details = append(details, jobs.JobDefDetail{Key: key, Value: val})
		}
		rows3.Close()

		job.Details = details

		if job.Status == jobs.RUNNING || job.Status == jobs.PROXYQUEUED {
			sql4 := "SELECT key, value FROM job_running_details WHERE job_id = ?"
			rows4, err4 := conn.QueryContext(ctx, sql4, job.JobId)
			if err4 != nil {
				log.Fatal(err4)
			}
			var runningDetails []jobs.JobRunningDetail

			for rows4.Next() {
				var key string
				var val string
				rows4.Scan(&key, &val)
				runningDetails = append(runningDetails, jobs.JobRunningDetail{Key: key, Value: val})
			}
			rows4.Close()

			job.RunningDetails = runningDetails

		}
	}

	return retjobs
}
