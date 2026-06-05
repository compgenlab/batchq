package runner

// Regression tests for the SLURM runner's afterok-dependency handling.
//
// buildSBatchScript used to call SlurmGetJobState (which shells out to
// sacct/squeue) to look up the parent's live SLURM state for any dep that
// batchq considered RUNNING/PROXYQUEUED. If sacct hadn't caught up yet —
// a transient lag right after sbatch returned or right after a parent
// finished — the function returned ("", nil) and the caller marked the
// child FAILED with no slurm metadata and no reason. SLURM, with
// --kill-on-invalid-dep=yes, handles all the cases that lookup was
// trying to catch, so the lookup was redundant and is now removed.
//
// The fact that this test can pass with no sacct binary on the host
// is itself the assertion: if the implementation ever regresses and
// starts calling SlurmGetJobState, it'll error out here.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mbreese/batchq/api"
	"github.com/mbreese/batchq/jobs"
)

func TestSlurmDepTrustsRecordedID(t *testing.T) {
	c, _ := startServerForRunner(t)
	ctx := context.Background()

	parent, err := c.SubmitJob(ctx, &api.SubmitJobRequest{
		Name:    "parent",
		Details: map[string]string{"script": "#!/bin/sh\necho parent\n"},
	})
	if err != nil {
		t.Fatalf("SubmitJob parent: %v", err)
	}

	if _, err := c.ClaimNextJob(ctx, "test-runner", "slurm", "", 0, 0, 0, nil); err != nil {
		t.Fatalf("ClaimNextJob: %v", err)
	}
	const slurmID = "987654"
	if err := c.MarkJobProxied(ctx, "test-runner", parent.JobID, map[string]string{
		"slurm_job_id": slurmID,
	}); err != nil {
		t.Fatalf("MarkJobProxied: %v", err)
	}

	child, err := c.SubmitJob(ctx, &api.SubmitJobRequest{
		Name:    "child",
		AfterOk: []string{parent.JobID},
		Details: map[string]string{"script": "#!/bin/sh\necho child\n"},
	})
	if err != nil {
		t.Fatalf("SubmitJob child: %v", err)
	}

	childDTO, err := c.GetJob(ctx, child.JobID)
	if err != nil {
		t.Fatalf("GetJob child: %v", err)
	}

	r := NewSlurmRunner(c)
	src, err := r.buildSBatchScript(ctx, childDTO)
	if err != nil {
		t.Fatalf("buildSBatchScript: %v", err)
	}
	if src == "" {
		t.Fatalf("buildSBatchScript returned empty src — this is the regressed bug")
	}
	wantDep := "#SBATCH -d afterok:" + slurmID
	if !strings.Contains(src, wantDep) {
		t.Fatalf("script missing %q:\n%s", wantDep, src)
	}
	if !strings.Contains(src, "#SBATCH --kill-on-invalid-dep=yes") {
		t.Fatalf("script missing --kill-on-invalid-dep=yes (required for trust-and-defer to work):\n%s", src)
	}
}

// A dep that already succeeded in batchq should be silently skipped — no
// afterok line emitted, no error.
func TestSlurmDepSkipsSucceeded(t *testing.T) {
	c, _ := startServerForRunner(t)
	ctx := context.Background()

	parent, _ := c.SubmitJob(ctx, &api.SubmitJobRequest{
		Name:    "parent",
		Details: map[string]string{"script": "#!/bin/sh\necho parent\n"},
	})
	if _, err := c.ClaimNextJob(ctx, "test-runner", "simple", "", 0, 0, 0, nil); err != nil {
		t.Fatalf("ClaimNextJob: %v", err)
	}
	if err := c.EndJob(ctx, "test-runner", parent.JobID, 0, ""); err != nil {
		t.Fatalf("EndJob: %v", err)
	}

	child, _ := c.SubmitJob(ctx, &api.SubmitJobRequest{
		Name:    "child",
		AfterOk: []string{parent.JobID},
		Details: map[string]string{"script": "#!/bin/sh\necho child\n"},
	})

	childDTO, _ := c.GetJob(ctx, child.JobID)
	r := NewSlurmRunner(c)
	src, err := r.buildSBatchScript(ctx, childDTO)
	if err != nil {
		t.Fatalf("buildSBatchScript: %v", err)
	}
	if strings.Contains(src, "afterok") {
		t.Fatalf("expected no afterok for a SUCCESS dep, got:\n%s", src)
	}
}

// EndJob with a non-empty notes argument should persist the reason into
// jobs.notes so it appears on the detail page for a FAILED job. CANCELED
// already does this via CancelJob; FAILED used to silently drop it.
func TestEndJobNotesPersisted(t *testing.T) {
	c, _ := startServerForRunner(t)
	ctx := context.Background()

	dto, err := c.SubmitJob(ctx, &api.SubmitJobRequest{
		Name:    "boom",
		Details: map[string]string{"script": "#!/bin/sh\nexit 1\n"},
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	if _, err := c.ClaimNextJob(ctx, "runner-x", "simple", "", 0, 0, 0, nil); err != nil {
		t.Fatalf("ClaimNextJob: %v", err)
	}
	const reason = "missing UID in job details"
	if err := c.EndJob(ctx, "runner-x", dto.JobID, 1, reason); err != nil {
		t.Fatalf("EndJob: %v", err)
	}

	// Re-fetch and verify notes are visible on the DTO that feeds the
	// detail-page template.
	deadline := time.Now().Add(2 * time.Second)
	var got *api.JobDTO
	for time.Now().Before(deadline) {
		got, err = c.GetJob(ctx, dto.JobID)
		if err == nil && got != nil && got.Status == jobs.FAILED.String() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Status != jobs.FAILED.String() {
		t.Fatalf("status: %s want FAILED", got.Status)
	}
	if got.Notes != reason {
		t.Fatalf("notes: got %q, want %q", got.Notes, reason)
	}
}
