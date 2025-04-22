package db

import (
	"context"
	"fmt"

	"github.com/mbreese/batchq/jobs"
)

type BatchDB interface {
	SubmitJob(ctx context.Context, job *jobs.JobDef) *jobs.JobDef
	FetchNext(ctx context.Context, freeProc int, freeMemMB int, freeTimeSec int) (*jobs.JobDef, bool)
	GetJob(ctx context.Context, jobId int) *jobs.JobDef
	GetJobs(ctx context.Context, showAll bool) []jobs.JobDef
	CancelJob(ctx context.Context, jobId int) bool
	StartJob(ctx context.Context, jobId int, jobRunner string, details map[string]string) bool // was the starting successful?
	EndJob(ctx context.Context, jobId int, jobRunner string, returnCode int) bool
	CleanupJob(ctx context.Context, jobId int) bool
	HoldJob(ctx context.Context, jobId int) bool
	ReleaseJob(ctx context.Context, jobId int) bool
	Close()
}

func OpenDB(dbpath string) (BatchDB, error) {
	if dbpath[:10] == "sqlite3://" {
		return openSqlite3(dbpath[10:]), nil
	}
	return nil, fmt.Errorf("bad dbpath: %s", dbpath)
}

func InitDB(dbpath string, force bool) error {
	if dbpath[:10] == "sqlite3://" {
		return initSqlite3(dbpath[10:], force)
	}
	return fmt.Errorf("bad dbpath: %s", dbpath)
}
