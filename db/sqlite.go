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

type SqliteBatchQ struct {
	fname        string
	dbConn       *sql.DB
	connectCount int
	conLock      sync.Mutex
}

func openSqlite3(fname string) *SqliteBatchQ {
	// f, err := os.Open(fname)
	// if err == nil {
	// 	f.Close()
	// } else {
	// 	// InitDB(fname)
	// }

	db := SqliteBatchQ{fname: fname}
	return &db
}

func initSqlite3(fname string, force bool, startingJobId int) error {
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
	sql := `
	PRAGMA foreign_keys = ON;
    PRAGMA journal_mode=WAL;

	DROP TABLE IF EXISTS job_running_details;
	DROP TABLE IF EXISTS job_running;
	DROP TABLE IF EXISTS job_deps;
	DROP TABLE IF EXISTS job_details;
	DROP TABLE IF EXISTS jobs;

	CREATE TABLE jobs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		status INT DEFAULT 0 NOT NULL,
		name TEXT,
		submit_time TEXT DEFAULT "",
		start_time TEXT DEFAULT "",
		end_time TEXT DEFAULT "",
		return_code INT DEFAULT 0
			);

	CREATE TABLE job_details (
		job_id INTEGER REFERENCES jobs(id),
		key TEXT,
		value TEXT,
		PRIMARY KEY (job_id, key)
			);

	CREATE TABLE job_deps (
		job_id INTEGER REFERENCES jobs(id),
		afterok_id INTEGER REFERENCES jobs(id),
		PRIMARY KEY (job_id, afterok_id)
			);

	CREATE TABLE job_running (
		job_id INTEGER REFERENCES jobs(id) UNIQUE PRIMARY KEY,
		job_runner TEXT
			);

	CREATE TABLE job_running_details (
		job_id INTEGER REFERENCES jobs(id),
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
	if startingJobId > 1 {
		// see: https://stackoverflow.com/questions/692856/set-start-value-for-autoincrement-in-sqlite
		sql = fmt.Sprintf(`BEGIN TRANSACTION;
UPDATE sqlite_sequence SET seq = %d WHERE name = 'jobs';
INSERT INTO sqlite_sequence (name,seq) SELECT 'jobs', %d WHERE NOT EXISTS (SELECT changes() AS change FROM sqlite_sequence WHERE change <> 0);
COMMIT;`, startingJobId-1, startingJobId-1)
		_, err = db.ExecContext(ctx, sql)
		if err != nil {
			// log.Fatalf("Exec error: %v", err)
			return err
		}
	}
	fmt.Println("Done.")
	return nil
}

// Each method will connect on it's own, this is because there is no row-level locking of the database, and
// this db could be accessed from different processes (potentially over a network share). So, in order to
// keep locking to a minimum, we'll just open the db and close it for each function call.

func (db *SqliteBatchQ) connect() *sql.DB {
	db.conLock.Lock()
	if db.dbConn != nil {
		db.connectCount++
		db.conLock.Unlock()
		return db.dbConn
	}
	// fmt.Printf("Opening database: %s\n", db.fname)
	if _, err := os.Open(db.fname); err != nil {
		log.Fatal("Missing database. Please initialize it first!")
	}

	conn, err := sql.Open("sqlite3", fmt.Sprintf("%s?_busy_timeout=5000", db.fname))
	if err != nil {
		log.Fatal(err)
	}
	conn.SetMaxOpenConns(1)
	db.dbConn = conn
	db.connectCount = 1
	db.conLock.Unlock()
	return conn
}

func (db *SqliteBatchQ) close() {
	db.conLock.Lock()
	db.connectCount--
	if db.connectCount == 0 {
		if db.dbConn != nil {
			db.dbConn.Close()
			db.dbConn = nil
		}
	}
	db.conLock.Unlock()
}

func (db *SqliteBatchQ) SubmitJob(ctx context.Context, job *jobs.JobDef) *jobs.JobDef {
	conn := db.connect()
	defer db.close()
	if tx, err := conn.BeginTx(ctx, nil); err != nil {
		log.Fatal(err)
	} else {

		for _, depid := range job.AfterOk {
			dep := db.GetJob(ctx, depid)
			if dep == nil || dep.Status == jobs.CANCELLED || dep.Status == jobs.FAILED {
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

		sql := "INSERT INTO jobs (status,name,submit_time) VALUES (?,?,?)"
		res, err := tx.ExecContext(ctx, sql, newStatus, job.Name, support.GetNowTS())
		if err != nil {
			tx.Rollback()
			log.Fatal(err)

		} else {
			if jobId, err2 := res.LastInsertId(); err2 != nil {
				tx.Rollback()
				log.Fatal(err2)

			} else {
				job.JobId = int(jobId)
				job.Status = newStatus
				if job.Name == "" {
					job.Name = "job-%JOBID"
				}
				if strings.Contains(job.Name, "%JOBID") {
					job.Name = strings.Replace(job.Name, "%JOBID", fmt.Sprintf("%d", jobId), -1)
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
						detail.Value = strings.Replace(detail.Value, "%JOBID", fmt.Sprintf("%d", jobId), -1)
					}
					if _, err3 := tx.ExecContext(ctx, "INSERT INTO job_details (job_id, key, value) VALUES (?,?,?)", job.JobId, detail.Key, detail.Value); err3 != nil {
						tx.Rollback()
						log.Fatal(err3)
					}
				}
			}
		}
		if err2 := tx.Commit(); err2 != nil {
			tx.Rollback()
		}
	}
	return job
}

func (db *SqliteBatchQ) GetJobs(ctx context.Context, showAll bool) []jobs.JobDef {
	conn := db.connect()
	defer db.close()

	var sql string
	var args []any
	if showAll {
		sql = "SELECT id,status,name,submit_time,start_time,end_time,return_code FROM jobs ORDER BY submit_time"
		args = []any{}
	} else {
		sql = "SELECT id,status,name,submit_time,start_time,end_time,return_code FROM jobs WHERE status <= ? ORDER BY submit_time"
		args = []any{jobs.RUNNING}
	}

	rows, err := conn.QueryContext(ctx, sql, args...)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var retjobs []jobs.JobDef
	for rows.Next() {
		var job jobs.JobDef
		var submitTime string
		var startTime string
		var endTime string

		err := rows.Scan(&job.JobId, &job.Status, &job.Name, &submitTime, &startTime, &endTime, &job.ReturnCode)
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

		// Load job dependencies (we are the child, looking for parents)
		sql2 := "SELECT afterok_id FROM job_deps WHERE job_id = ? ORDER BY afterok_id"
		// TODO: confirm afterok_id exists
		rows2, err2 := conn.QueryContext(ctx, sql2, job.JobId)
		if err2 != nil {
			log.Fatal(err2)
		}
		defer rows2.Close()
		var deps []int

		for rows2.Next() {
			var parentId int
			rows2.Scan(&parentId)
			deps = append(deps, parentId)
		}

		job.AfterOk = deps

		// Load job dependencies (we are the child, looking for parents)
		sql3 := "SELECT key, value FROM job_details WHERE job_id = ?"
		// TODO: confirm afterok_id exists
		rows3, err3 := conn.QueryContext(ctx, sql3, job.JobId)
		if err3 != nil {
			log.Fatal(err3)
		}
		defer rows3.Close()
		var details []jobs.JobDefDetail

		for rows3.Next() {
			var key string
			var val string
			rows3.Scan(&key, &val)
			details = append(details, jobs.JobDefDetail{Key: key, Value: val})
		}

		job.Details = details

		retjobs = append(retjobs, job)
	}

	return retjobs
}

func (db *SqliteBatchQ) FetchNext(ctx context.Context, freeProc int, freeMemMB int, freeTimeSec int) (*jobs.JobDef, bool) {
	// First, update all WAITING jobs to QUEUED if they have no outstanding dependencies
	db.updateQueue(ctx)

	// Next, look for a QUEUED job that first the proc/mem/time limits
	conn := db.connect()
	defer db.close()

	sql := "SELECT id FROM jobs WHERE status = ?"
	var queueJobIds []int
	if rows, err := conn.QueryContext(ctx, sql, jobs.QUEUED); err != nil {
		log.Fatal(err)
	} else {
		defer rows.Close()
		for rows.Next() {
			var jobId int

			err := rows.Scan(&jobId)
			if err != nil {
				log.Fatal(err)
			}
			// fmt.Printf("found queued job: %d\n", jobId)
			queueJobIds = append(queueJobIds, jobId)
		}
	}
	for _, jobId := range queueJobIds {
		job := db.GetJob(ctx, jobId)
		passProc := true
		passMem := true
		passTime := true

		if freeProc > 0 {
			if val := job.GetDetail("procs", ""); val != "" {
				if jobProcs, err := strconv.Atoi(val); err != nil {
					log.Fatal(err)
				} else {
					if jobProcs > freeProc {
						passProc = false
					}
				}
			}
		}

		if freeMemMB > 0 {
			if val := job.GetDetail("mem", ""); val != "" {
				if jobMem, err := strconv.Atoi(val); err != nil {
					log.Fatal(err)
				} else {
					if jobMem > freeMemMB {
						passMem = false
					}
				}
			}
		}

		if freeTimeSec > 0 {
			if val := job.GetDetail("walltime", ""); val != "" {
				if jobTime, err := strconv.Atoi(val); err != nil {
					log.Fatal(err)
				} else {
					if jobTime > freeTimeSec {
						passTime = false
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

		if passProc && passMem && passTime {
			return job, true
		}
	}

	return nil, false
}

func (db *SqliteBatchQ) updateQueue(ctx context.Context) {
	conn := db.connect()
	defer db.close()

	sql := "SELECT id FROM jobs WHERE status = ? OR status = ?"
	var possibleJobIds []int
	var queueJobIds []int
	var cancelJobIds []int
	if rows, err := conn.QueryContext(ctx, sql, jobs.UNKNOWN, jobs.WAITING); err != nil {
		log.Fatal(err)
	} else {
		defer rows.Close()
		for rows.Next() {
			var jobId int

			err := rows.Scan(&jobId)
			if err != nil {
				log.Fatal(err)
			}
			// fmt.Printf("found waiting/unknown job: %d\n", jobId)
			possibleJobIds = append(possibleJobIds, jobId)
		}
	}

	// fmt.Printf("found waiting/unknown jobs: %v\n", possibleJobIds)

	for _, jobId := range possibleJobIds {
		job := db.GetJob(ctx, jobId)
		enqueue := true
		cancel := false
		for _, depid := range job.AfterOk {
			dep := db.GetJob(ctx, depid)
			if dep.Status != jobs.SUCCESS {
				enqueue = false
			}
			if dep.Status == jobs.CANCELLED || dep.Status == jobs.FAILED {
				cancel = true
				enqueue = false
			}
		}
		if cancel {
			cancelJobIds = append(cancelJobIds, jobId)
		} else if enqueue {
			// fmt.Printf("moving to queue: %d\n", jobId)
			queueJobIds = append(queueJobIds, jobId)
		}
	}
	for _, jobId := range cancelJobIds {
		sql2 := "UPDATE jobs SET status = ? WHERE id = ?"
		_, err := conn.ExecContext(ctx, sql2, jobs.CANCELLED, jobId)
		if err != nil {
			log.Fatal(err)
		}
		// fmt.Printf("moved to queue: %d\n", jobId)
	}
	for _, jobId := range queueJobIds {
		sql2 := "UPDATE jobs SET status = ? WHERE id = ?"
		_, err := conn.ExecContext(ctx, sql2, jobs.QUEUED, jobId)
		if err != nil {
			log.Fatal(err)
		}
		// fmt.Printf("moved to queue: %d\n", jobId)
	}
}

func (db *SqliteBatchQ) GetJob(ctx context.Context, jobId int) *jobs.JobDef {
	conn := db.connect()
	defer db.close()

	sql := "SELECT id,status,name,submit_time,start_time,end_time,return_code FROM jobs WHERE id = ?"
	rows, err := conn.QueryContext(ctx, sql, jobId)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	if rows.Next() {
		var job jobs.JobDef
		var submitTime string
		var startTime string
		var endTime string

		err := rows.Scan(&job.JobId, &job.Status, &job.Name, &submitTime, &startTime, &endTime, &job.ReturnCode)
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

		// Load job dependencies (we are the child, looking for parents)
		sql2 := "SELECT afterok_id FROM job_deps WHERE job_id = ? ORDER BY afterok_id"
		rows2, err2 := conn.QueryContext(ctx, sql2, job.JobId)
		if err2 != nil {
			log.Fatal(err2)
		}
		defer rows2.Close()
		var deps []int

		for rows2.Next() {
			var parentId int
			rows2.Scan(&parentId)
			deps = append(deps, parentId)
		}

		job.AfterOk = deps
		// Load job dependencies (we are the child, looking for parents)
		sql3 := "SELECT key, value FROM job_details WHERE job_id = ?"
		// TODO: confirm afterok_id exists
		rows3, err3 := conn.QueryContext(ctx, sql3, job.JobId)
		if err3 != nil {
			log.Fatal(err3)
		}
		defer rows3.Close()
		var details []jobs.JobDefDetail

		for rows3.Next() {
			var key string
			var val string
			rows3.Scan(&key, &val)
			details = append(details, jobs.JobDefDetail{Key: key, Value: val})
		}

		job.Details = details

		return &job
	}

	return nil
}
func (db *SqliteBatchQ) CancelJob(ctx context.Context, jobId int) bool {
	conn := db.connect()
	defer db.close()

	sql2 := "UPDATE jobs SET status = ?, end_time = ? WHERE id = ? AND status != ? AND status != ?"
	res, err := conn.ExecContext(ctx, sql2, jobs.CANCELLED, support.GetNowTS(), jobId, jobs.FAILED, jobs.SUCCESS)
	if err != nil {
		log.Fatal(err)
	}
	if rowcount, err2 := res.RowsAffected(); rowcount == 1 && err2 == nil {
		childIds := []int{}
		// Load job dependencies
		sql2 := "SELECT job_id FROM job_deps, jobs WHERE afterok_id = ? AND job_deps.job_id = jobs.id AND jobs.status != ?"
		rows2, err2 := conn.QueryContext(ctx, sql2, jobId, jobs.CANCELLED)
		if err2 != nil {
			log.Fatal(err2)
		}
		defer rows2.Close()

		for rows2.Next() {
			var childId int
			rows2.Scan(&childId)
			childIds = append(childIds, childId)
		}

		for _, cid := range childIds {
			// recursively delete
			if !db.CancelJob(ctx, cid) {
				return false
			}
		}

		return true
	}
	return false
}

func (db *SqliteBatchQ) StartJob(ctx context.Context, jobId int, jobRunner string, runDetail map[string]string) bool {
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
	defer rows1.Close()

	for rows1.Next() {
		var dbJobRunner string
		rows1.Scan(&dbJobRunner)

		if jobRunner != dbJobRunner {
			// oops, we aren't the runner of record. bailout.
			return false
		}
	}

	if tx, err := conn.BeginTx(ctx, nil); err == nil {
		sql2 := "UPDATE jobs SET status = ?, start_time = ? WHERE id = ?"

		_, err2 := tx.ExecContext(ctx, sql2, jobs.RUNNING, support.GetNowTS(), jobId)
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

func (db *SqliteBatchQ) EndJob(ctx context.Context, jobId int, jobRunner string, returnCode int) bool {
	conn := db.connect()
	defer db.close()

	sql1 := "SELECT job_runner FROM job_running WHERE job_id = ?"
	rows1, err1 := conn.QueryContext(ctx, sql1, jobId)
	if err1 != nil {
		log.Fatal(err1)
	}
	defer rows1.Close()

	for rows1.Next() {
		var dbJobRunner string
		rows1.Scan(&dbJobRunner)

		if jobRunner != dbJobRunner {
			// oops, we aren't the runner of record. bailout.
			fmt.Println("Attempted to end job from a different runner.")
			return false
		}
	}

	// MAYBE: remove job_runner records here if necessary... probably nice
	//        to keep them around, but this is where you'd remove them. As
	//        a bonus, cancelled jobs will also end up here, so you could
	//        remove the records here too...

	sql2 := "UPDATE jobs SET status = ?, end_time = ?, return_code = ? WHERE id = ? and status = ?"

	status := jobs.SUCCESS
	if returnCode != 0 {
		status = jobs.FAILED
	}
	// fmt.Printf("updating job final status: %d\n", status)
	res, err := conn.ExecContext(ctx, sql2, status, support.GetNowTS(), returnCode, jobId, jobs.RUNNING)
	if err != nil {
		log.Fatal(err)
	}

	if rowcount, err2 := res.RowsAffected(); rowcount == 1 && err2 == nil {
		// fmt.Printf("done\n")
		// db.scanQueue(ctx, jobId, status == jobs.SUCCESS)
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

func (db *SqliteBatchQ) CleanupJob(ctx context.Context, jobId int) bool {
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

func (db *SqliteBatchQ) HoldJob(ctx context.Context, jobId int) bool {
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
func (db *SqliteBatchQ) ReleaseJob(ctx context.Context, jobId int) bool {
	conn := db.connect()
	defer db.close()

	sql2 := "UPDATE jobs SET status = ? WHERE id = ? AND status = ?"
	res, err := conn.ExecContext(ctx, sql2, jobs.WAITING, jobId, jobs.USERHOLD)
	if err != nil {
		log.Fatal(err)
	}
	if rowcount, err2 := res.RowsAffected(); rowcount == 1 && err2 == nil {
		return true
	}
	return false
}
