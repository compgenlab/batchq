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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mbreese/batchq/api"
	"github.com/mbreese/batchq/client"
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

// A gather depending on a whole array (afterok:<array_id>) must build a valid
// afterok directive even though array tasks record slurm_array_id +
// slurm_task_index (never slurm_job_id). Whole-array deps collapse to one
// afterok:<slurm_array_id> per SLURM sub-array. Regression: this used to error
// "missing slurm_job_id" and cancel the gather.
func TestSlurmDepArrayWholeCollapses(t *testing.T) {
	c, _ := startServerForRunner(t)
	ctx := context.Background()

	arr, err := c.SubmitArray(ctx, &api.SubmitArrayRequest{
		SubmitJobRequest: api.SubmitJobRequest{
			Name:    "fanout",
			Details: map[string]string{"script": "#!/bin/sh\necho task\n"},
		},
		ArrayIndices: []int{0, 1, 2},
	})
	if err != nil {
		t.Fatalf("SubmitArray: %v", err)
	}

	// Claim all tasks and proxy them under a single SLURM array id.
	resp, err := c.ClaimNextArrayBatch(ctx, "test-runner", "slurm", "", -1, -1, -1, nil, -1, 0, false)
	if err != nil {
		t.Fatalf("ClaimNextArrayBatch: %v", err)
	}
	if resp.ArrayID == "" || len(resp.Tasks) != 3 {
		t.Fatalf("expected 3-task array claim, got ArrayID=%q tasks=%d", resp.ArrayID, len(resp.Tasks))
	}
	for _, task := range resp.Tasks {
		if err := c.MarkJobProxied(ctx, "test-runner", task.JobID, map[string]string{
			"slurm_array_id":   "500",
			"slurm_task_index": strconv.Itoa(task.Index),
		}); err != nil {
			t.Fatalf("MarkJobProxied task %d: %v", task.Index, err)
		}
	}

	gather, err := c.SubmitJob(ctx, &api.SubmitJobRequest{
		Name:      "gather",
		ArrayDeps: []string{"afterok:" + arr.ArrayID},
		Details:   map[string]string{"script": "#!/bin/sh\necho gather\n"},
	})
	if err != nil {
		t.Fatalf("SubmitJob gather: %v", err)
	}

	src := buildGatherScript(t, c, gather.JobID)
	wantDep := "#SBATCH -d afterok:500\n"
	if !strings.Contains(src, wantDep) {
		t.Fatalf("script missing %q (whole-array collapse):\n%s", wantDep, src)
	}
	if strings.Contains(src, "500_") {
		t.Fatalf("expected collapsed afterok:500, got per-element ids:\n%s", src)
	}
}

// When a single batchq array is drip-fed as multiple SLURM arrays, a whole-array
// gather collapses to one afterok entry per distinct slurm_array_id.
func TestSlurmDepArraySpansMultipleSlurmArrays(t *testing.T) {
	c, _ := startServerForRunner(t)
	ctx := context.Background()

	arr, err := c.SubmitArray(ctx, &api.SubmitArrayRequest{
		SubmitJobRequest: api.SubmitJobRequest{
			Name:    "fanout",
			Details: map[string]string{"script": "#!/bin/sh\necho task\n"},
		},
		ArrayIndices: []int{0, 1, 2},
	})
	if err != nil {
		t.Fatalf("SubmitArray: %v", err)
	}

	resp, err := c.ClaimNextArrayBatch(ctx, "test-runner", "slurm", "", -1, -1, -1, nil, -1, 0, false)
	if err != nil {
		t.Fatalf("ClaimNextArrayBatch: %v", err)
	}
	// Split the tasks across two SLURM arrays: indices 0,1 -> "500", 2 -> "501".
	for _, task := range resp.Tasks {
		slurmArray := "500"
		if task.Index == 2 {
			slurmArray = "501"
		}
		if err := c.MarkJobProxied(ctx, "test-runner", task.JobID, map[string]string{
			"slurm_array_id":   slurmArray,
			"slurm_task_index": strconv.Itoa(task.Index),
		}); err != nil {
			t.Fatalf("MarkJobProxied task %d: %v", task.Index, err)
		}
	}

	gather, err := c.SubmitJob(ctx, &api.SubmitJobRequest{
		Name:      "gather",
		ArrayDeps: []string{"afterok:" + arr.ArrayID},
		Details:   map[string]string{"script": "#!/bin/sh\necho gather\n"},
	})
	if err != nil {
		t.Fatalf("SubmitJob gather: %v", err)
	}

	src := buildGatherScript(t, c, gather.JobID)
	wantDep := "#SBATCH -d afterok:500:501\n"
	if !strings.Contains(src, wantDep) {
		t.Fatalf("script missing %q (multi-sub-array collapse):\n%s", wantDep, src)
	}
}

// A partial (task-address) array dep falls back to a per-element afterok id,
// since the gather does not depend on the whole sub-array.
func TestSlurmDepArrayPartialPerElement(t *testing.T) {
	c, _ := startServerForRunner(t)
	ctx := context.Background()

	arr, err := c.SubmitArray(ctx, &api.SubmitArrayRequest{
		SubmitJobRequest: api.SubmitJobRequest{
			Name:    "fanout",
			Details: map[string]string{"script": "#!/bin/sh\necho task\n"},
		},
		ArrayIndices: []int{0, 1, 2},
	})
	if err != nil {
		t.Fatalf("SubmitArray: %v", err)
	}

	resp, err := c.ClaimNextArrayBatch(ctx, "test-runner", "slurm", "", -1, -1, -1, nil, -1, 0, false)
	if err != nil {
		t.Fatalf("ClaimNextArrayBatch: %v", err)
	}
	for _, task := range resp.Tasks {
		if err := c.MarkJobProxied(ctx, "test-runner", task.JobID, map[string]string{
			"slurm_array_id":   "500",
			"slurm_task_index": strconv.Itoa(task.Index),
		}); err != nil {
			t.Fatalf("MarkJobProxied task %d: %v", task.Index, err)
		}
	}

	// Depend on just task 1, not the whole array.
	gather, err := c.SubmitJob(ctx, &api.SubmitJobRequest{
		Name:      "gather",
		ArrayDeps: []string{"afterok:" + arr.ArrayID + "_1"},
		Details:   map[string]string{"script": "#!/bin/sh\necho gather\n"},
	})
	if err != nil {
		t.Fatalf("SubmitJob gather: %v", err)
	}

	src := buildGatherScript(t, c, gather.JobID)
	wantDep := "#SBATCH -d afterok:500_1\n"
	if !strings.Contains(src, wantDep) {
		t.Fatalf("script missing %q (partial dep per-element):\n%s", wantDep, src)
	}
}

// Handing the last array task to SLURM (MarkJobProxied -> PROXYQUEUED) must
// promote a gather that depends on the whole array from WAITING to QUEUED, even
// without a subsequent claim. Otherwise a runner that breaks out of its submit
// loop on a saturated live-queue budget would strand the gather in WAITING.
func TestMarkProxiedPromotesDependentGather(t *testing.T) {
	c, _ := startServerForRunner(t)
	ctx := context.Background()

	arr, err := c.SubmitArray(ctx, &api.SubmitArrayRequest{
		SubmitJobRequest: api.SubmitJobRequest{
			Name:    "fanout",
			Details: map[string]string{"script": "#!/bin/sh\necho task\n"},
		},
		ArrayIndices: []int{0, 1, 2},
	})
	if err != nil {
		t.Fatalf("SubmitArray: %v", err)
	}

	// Submit the gather BEFORE the tasks are proxied, so it starts WAITING.
	gather, err := c.SubmitJob(ctx, &api.SubmitJobRequest{
		Name:      "gather",
		ArrayDeps: []string{"afterok:" + arr.ArrayID},
		Details:   map[string]string{"script": "#!/bin/sh\necho gather\n"},
	})
	if err != nil {
		t.Fatalf("SubmitJob gather: %v", err)
	}
	if got, _ := c.GetJob(ctx, gather.JobID); got.Status != jobs.WAITING.String() {
		t.Fatalf("gather status = %s, want WAITING before deps are proxied", got.Status)
	}

	resp, err := c.ClaimNextArrayBatch(ctx, "test-runner", "slurm", "", -1, -1, -1, nil, -1, 0, false)
	if err != nil {
		t.Fatalf("ClaimNextArrayBatch: %v", err)
	}
	for i, task := range resp.Tasks {
		if err := c.MarkJobProxied(ctx, "test-runner", task.JobID, map[string]string{
			"slurm_array_id":   "500",
			"slurm_task_index": strconv.Itoa(task.Index),
		}); err != nil {
			t.Fatalf("MarkJobProxied task %d: %v", task.Index, err)
		}
		// Gather should remain WAITING until the LAST task is proxied.
		got, _ := c.GetJob(ctx, gather.JobID)
		wantStatus := jobs.WAITING.String()
		if i == len(resp.Tasks)-1 {
			wantStatus = jobs.QUEUED.String()
		}
		if got.Status != wantStatus {
			t.Fatalf("after proxying %d/%d tasks: gather status = %s, want %s",
				i+1, len(resp.Tasks), got.Status, wantStatus)
		}
	}
}

// buildGatherScript fetches a job and builds its sbatch script, failing the test
// if the build errors or returns empty (the regressed cancel path).
func buildGatherScript(t *testing.T, c *client.Client, jobID string) string {
	t.Helper()
	ctx := context.Background()
	dto, err := c.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("GetJob %s: %v", jobID, err)
	}
	r := NewSlurmRunner(c)
	src, err := r.buildSBatchScript(ctx, dto)
	if err != nil {
		t.Fatalf("buildSBatchScript: %v", err)
	}
	if src == "" {
		t.Fatalf("buildSBatchScript returned empty src — gather would be canceled")
	}
	if !strings.Contains(src, "#SBATCH --kill-on-invalid-dep=yes") {
		t.Fatalf("script missing --kill-on-invalid-dep=yes:\n%s", src)
	}
	return src
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
