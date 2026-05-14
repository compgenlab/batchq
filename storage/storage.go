// Package storage defines the persistence interface for batchq and a SQLite
// implementation. The interface is the server-internal contract over which a
// future Postgres backend can be swapped in.
package storage

import (
	"context"
	"errors"
	"time"

	"github.com/mbreese/batchq/jobs"
)

// ErrJobNotFound is returned when a job ID does not exist.
var ErrJobNotFound = errors.New("job not found")

// ErrInvalidState is returned when an operation is not valid for the job's
// current state (e.g. ending a job that isn't running).
var ErrInvalidState = errors.New("invalid state for operation")

// Limits constrains which queued jobs a runner is willing to claim. A value
// of -1 means "no limit on this dimension".
type Limits struct {
	MaxProcs        int
	MaxMemoryMB     int
	MaxWalltimeSec  int
}

// ClaimResult is the outcome of a ClaimNextJob call.
type ClaimResult struct {
	// Job is the claimed job (already transitioned to RUNNING) or nil if
	// nothing was claimed.
	Job *jobs.JobDef
	// MoreEligible is true if there are QUEUED jobs in the database that did
	// not fit the supplied limits. The runner can use this to decide whether
	// to keep polling soon vs sleep longer.
	MoreEligible bool
}

// ResolveResult summarizes a ResolveDependencies pass.
type ResolveResult struct {
	Promoted int // jobs moved WAITING -> QUEUED
	Canceled int // jobs canceled due to failed/canceled parents
}

// Storage is the persistence contract used by the batchq server. All methods
// return errors instead of panicking; the server must not crash on bad input.
type Storage interface {
	// Close releases any resources. Safe to call multiple times.
	Close() error

	// InsertJob persists a new job. The caller is responsible for setting
	// job.JobId (UUID); InsertJob will compute the initial status from the
	// dependency list (QUEUED if no deps, WAITING otherwise, USERHOLD if the
	// caller set it explicitly).
	InsertJob(ctx context.Context, job *jobs.JobDef) error

	// GetJob loads one job (with details, running details, and deps) by ID.
	// Returns ErrJobNotFound if the job does not exist.
	GetJob(ctx context.Context, jobID string) (*jobs.JobDef, error)

	// ListJobs returns every job, optionally restricted to active statuses
	// only. sortByStatus controls the ORDER BY clause.
	ListJobs(ctx context.Context, showAll, sortByStatus bool) ([]*jobs.JobDef, error)

	// ListJobsByStatus returns jobs whose status is in the given set.
	ListJobsByStatus(ctx context.Context, statuses []jobs.StatusCode, sortByStatus bool) ([]*jobs.JobDef, error)

	// SearchJobs returns jobs whose ID, name, or script content matches the
	// query (substring). If statuses is non-empty, results are further
	// restricted to those statuses.
	SearchJobs(ctx context.Context, query string, statuses []jobs.StatusCode) ([]*jobs.JobDef, error)

	// GetJobDependents returns the IDs of jobs that depend on the given job.
	GetJobDependents(ctx context.Context, jobID string) ([]string, error)

	// GetJobStatusCounts returns a count of jobs per status. If !showAll,
	// only active statuses (<= RUNNING) are counted.
	GetJobStatusCounts(ctx context.Context, showAll bool) (map[jobs.StatusCode]int, error)

	// GetQueueJobs returns a minimal-detail listing suitable for the queue
	// view. Includes deps and a small subset of job_details / running_details.
	GetQueueJobs(ctx context.Context, showAll, sortByStatus bool) ([]*jobs.JobDef, error)

	// GetProxyJobs returns all jobs currently in PROXYQUEUED state. Used by
	// the slurm runner to reconcile SLURM state.
	GetProxyJobs(ctx context.Context) ([]*jobs.JobDef, error)

	// ResolveDependencies promotes WAITING jobs whose deps have all succeeded
	// (or proxied) to QUEUED, and cancels WAITING jobs whose parents
	// failed/canceled. Should be called periodically by the service layer.
	ResolveDependencies(ctx context.Context) (ResolveResult, error)

	// ClaimNextJob atomically transitions a single QUEUED job that fits the
	// supplied limits to RUNNING, recording runnerID and kind as the owner.
	// Returns the claimed job along with MoreEligible indicating whether
	// further QUEUED jobs (that did not fit the limits) remain.
	ClaimNextJob(ctx context.Context, runnerID, kind string, limits Limits) (ClaimResult, error)

	// MarkJobProxied transitions a RUNNING job (claimed by runnerID) to
	// PROXYQUEUED and merges runningDetails. Used by the slurm runner after
	// a successful sbatch.
	MarkJobProxied(ctx context.Context, jobID, runnerID string, runningDetails map[string]string) error

	// UpdateRunningDetails upserts key/value pairs into a job's running
	// details. Used for heartbeats and slurm state.
	UpdateRunningDetails(ctx context.Context, jobID string, details map[string]string) error

	// EndJob transitions a RUNNING job to SUCCESS or FAILED based on
	// returnCode. On failure, dependents are cascade-canceled.
	EndJob(ctx context.Context, jobID, runnerID string, returnCode int) error

	// EndProxiedJob transitions a PROXYQUEUED job to a terminal status with
	// times recorded from SLURM. On non-success, dependents are
	// cascade-canceled.
	EndProxiedJob(ctx context.Context, jobID string, status jobs.StatusCode, startTime, endTime time.Time, returnCode int) error

	// CancelJob marks a job CANCELED (with reason) and cascades to dependents.
	// No-op for already-terminal jobs.
	CancelJob(ctx context.Context, jobID, reason string) error

	// HoldJob moves a QUEUED/WAITING/USERHOLD job to USERHOLD.
	HoldJob(ctx context.Context, jobID string) error

	// ReleaseJob moves a USERHOLD job back to WAITING (so dependency
	// resolution can decide whether to promote it to QUEUED).
	ReleaseJob(ctx context.Context, jobID string) error

	// AdjustJobPriority changes a job's priority by delta (positive or
	// negative). Only valid for QUEUED/WAITING/USERHOLD jobs.
	AdjustJobPriority(ctx context.Context, jobID string, delta int) error

	// CleanupJob removes a job and all associated rows (details, running
	// details, deps, running claim). Caller must verify the job is in a
	// terminal state.
	CleanupJob(ctx context.Context, jobID string) error

	// FindJobsByDetail returns the job IDs that carry a particular
	// (key, value) row in job_details. Used for the run_id filter on
	// GET /jobs.
	FindJobsByDetail(ctx context.Context, key, value string) ([]string, error)

	// FindJobsByInputPath / FindJobsByOutputPath return job IDs that
	// list path as an input / output file, respectively.
	FindJobsByInputPath(ctx context.Context, path string) ([]string, error)
	FindJobsByOutputPath(ctx context.Context, path string) ([]string, error)
}
