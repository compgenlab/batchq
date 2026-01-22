package db

import (
	"context"
	"fmt"

	"github.com/mbreese/batchq/jobs"
)

type BatchDB interface {
	// add a new job to the queue
	SubmitJob(ctx context.Context, job *jobs.JobDef) *jobs.JobDef

	// fetch the next job that fits these limits
	FetchNext(ctx context.Context, limits []JobLimit) (*jobs.JobDef, bool)
	// FetchNext(ctx context.Context, freeProc int, freeMemMB int, freeTimeSec int) (*jobs.JobDef, bool)
	GetJob(ctx context.Context, jobId int) *jobs.JobDef
	// list all jobs
	GetJobs(ctx context.Context, showAll bool, sortByStatus bool) []*jobs.JobDef

	// Cancel a job
	CancelJob(ctx context.Context, jobId int, reason string) bool
	// mark job as started
	StartJob(ctx context.Context, jobId int, jobRunner string, details map[string]string) bool // was the starting successful?
	// Mark that the job has been submitted to another queue (ex: Slurm)
	ProxyQueueJob(ctx context.Context, jobId int, jobRunner string, details map[string]string) bool
	// Find all the proxied jobs
	GetProxyJobs(ctx context.Context) []*jobs.JobDef
	// Mark proxied job as done
	ProxyEndJob(ctx context.Context, jobId int, status jobs.StatusCode, startTime string, endTime string, returnCode int) bool
	// update the running details for a job
	UpdateJobRunningDetails(ctx context.Context, jobId int, details map[string]string) bool
	// mark job as ended
	EndJob(ctx context.Context, jobId int, jobRunner string, returnCode int) bool

	// Remove all job data from the database
	CleanupJob(ctx context.Context, jobId int) bool

	// Increase a job's priority
	TopJob(ctx context.Context, jobId int) bool
	// Decrease a job's priority
	NiceJob(ctx context.Context, jobId int) bool

	// Hold a job from running
	HoldJob(ctx context.Context, jobId int) bool
	// Release a held job
	ReleaseJob(ctx context.Context, jobId int) bool

	Close()
}

type JobLimitType int

const (
	JobLimitTypeMemory JobLimitType = iota
	JobLimitTypeTime
	JobLimitTypeProc
)

type JobLimit struct {
	limitType JobLimitType
	value     int
}

func JobLimitMemoryMB(val int) JobLimit {
	return JobLimit{limitType: JobLimitTypeMemory, value: val}
}

func JobLimitTimeSec(val int) JobLimit {
	return JobLimit{limitType: JobLimitTypeTime, value: val}
}

func JobLimitProc(val int) JobLimit {
	return JobLimit{limitType: JobLimitTypeProc, value: val}
}

func OpenDB(dbpath string) (BatchDB, error) {
	if dbpath[:10] == "sqlite3://" {
		return openSqlite3(dbpath[10:]), nil
	}
	return nil, fmt.Errorf("bad dbpath: %s", dbpath)
}

func InitDB(dbpath string, force bool, startJobId int) error {
	if dbpath[:10] == "sqlite3://" {
		return initSqlite3(dbpath[10:], force, startJobId)
	}
	return fmt.Errorf("bad dbpath: %s", dbpath)
}
