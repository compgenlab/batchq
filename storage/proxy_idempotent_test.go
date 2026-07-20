package storage

import (
	"testing"
	"time"

	"github.com/compgenlab/batchq/jobs"
)

func runningDetail(job *jobs.JobDef, key string) string {
	for _, d := range job.RunningDetails {
		if d.Key == key {
			return d.Value
		}
	}
	return ""
}

// MarkJobProxied must be idempotent: a retry after it already committed (its
// response lost to a transient) succeeds and keeps the recorded slurm id, rather
// than failing with ErrInvalidState. This is what lets the runner retry the
// record after sbatch without orphaning the job.
func TestMarkJobProxiedIdempotent(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	if err := s.InsertJob(ctx, mkJob("j1", map[string]string{"script": "x"})); err != nil {
		t.Fatalf("InsertJob: %v", err)
	}
	if _, err := s.ClaimNextJob(ctx, "r", "slurm", Limits{}); err != nil {
		t.Fatalf("ClaimNextJob: %v", err)
	}
	det := map[string]string{"slurm_array_id": "12345"}

	if err := s.MarkJobProxied(ctx, "j1", "r", det); err != nil {
		t.Fatalf("MarkJobProxied #1: %v", err)
	}
	// Retry (as if the first response was lost to a Lustre stall).
	if err := s.MarkJobProxied(ctx, "j1", "r", det); err != nil {
		t.Fatalf("MarkJobProxied #2 (retry) should be idempotent, got: %v", err)
	}

	job, err := s.GetJob(ctx, "j1")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if job.Status != jobs.PROXYQUEUED {
		t.Fatalf("status = %v, want PROXYQUEUED", job.Status)
	}
	if got := runningDetail(job, "slurm_array_id"); got != "12345" {
		t.Fatalf("slurm_array_id = %q, want 12345", got)
	}
}

// MarkJobProxied on a job that is neither RUNNING nor already PROXYQUEUED is a
// genuine conflict.
func TestMarkJobProxiedRejectsWrongState(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	if err := s.InsertJob(ctx, mkJob("j1", map[string]string{"script": "x"})); err != nil {
		t.Fatalf("InsertJob: %v", err)
	}
	// Still QUEUED (never claimed) → not RUNNING, not PROXYQUEUED.
	if err := s.MarkJobProxied(ctx, "j1", "r", nil); err == nil {
		t.Fatal("MarkJobProxied on a QUEUED job should fail")
	}
}

// EndProxiedJob must be idempotent for a replay to the SAME terminal status.
func TestEndProxiedJobIdempotent(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)
	if err := s.InsertJob(ctx, mkJob("j1", map[string]string{"script": "x"})); err != nil {
		t.Fatalf("InsertJob: %v", err)
	}
	if _, err := s.ClaimNextJob(ctx, "r", "slurm", Limits{}); err != nil {
		t.Fatalf("ClaimNextJob: %v", err)
	}
	if err := s.MarkJobProxied(ctx, "j1", "r", map[string]string{"slurm_job_id": "9"}); err != nil {
		t.Fatalf("MarkJobProxied: %v", err)
	}
	now := ctxNow()

	if err := s.EndProxiedJob(ctx, "j1", jobs.SUCCESS, now, now, 0, ""); err != nil {
		t.Fatalf("EndProxiedJob #1: %v", err)
	}
	if err := s.EndProxiedJob(ctx, "j1", jobs.SUCCESS, now, now, 0, ""); err != nil {
		t.Fatalf("EndProxiedJob #2 (retry) should be idempotent, got: %v", err)
	}
	job, err := s.GetJob(ctx, "j1")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if job.Status != jobs.SUCCESS {
		t.Fatalf("status = %v, want SUCCESS", job.Status)
	}

	// A replay to a DIFFERENT terminal status is a conflict.
	if err := s.EndProxiedJob(ctx, "j1", jobs.FAILED, now, now, 1, ""); err == nil {
		t.Fatal("EndProxiedJob to a different terminal status should conflict")
	}
}

func ctxNow() time.Time { return time.Unix(1_700_000_000, 0).UTC() }
