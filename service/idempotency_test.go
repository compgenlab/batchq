package service

import (
	"testing"

	"github.com/compgenlab/batchq/api"
	"github.com/compgenlab/batchq/support"
)

// countJobs returns how many jobs exist (all statuses).
func countJobs(t *testing.T, svc *Service) int {
	t.Helper()
	dtos, err := svc.ListJobs(ctxT(t), ListJobsOptions{ShowAll: true})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	return len(dtos)
}

// Submitting twice with the same client-assigned JobID must return the SAME job,
// not create a duplicate — the idempotency contract that lets submit retry
// safely through a transient.
func TestSubmitJobIdempotentWithClientID(t *testing.T) {
	svc := newService(t)
	ctx := ctxT(t)
	id := support.NewUUID()
	req := func() *api.SubmitJobRequest {
		return &api.SubmitJobRequest{JobID: id, Name: "hi", Details: map[string]string{"script": "echo hi"}}
	}

	dto1, err := svc.SubmitJob(ctx, req())
	if err != nil {
		t.Fatalf("SubmitJob #1: %v", err)
	}
	if dto1.JobID != id {
		t.Fatalf("job id = %q, want the client id %q", dto1.JobID, id)
	}

	dto2, err := svc.SubmitJob(ctx, req())
	if err != nil {
		t.Fatalf("SubmitJob #2 (retry): %v", err)
	}
	if dto2.JobID != id {
		t.Fatalf("retry job id = %q, want %q", dto2.JobID, id)
	}
	if n := countJobs(t, svc); n != 1 {
		t.Fatalf("job count = %d, want 1 (no duplicate)", n)
	}
}

// A non-UUID client id is rejected.
func TestSubmitJobRejectsBadClientID(t *testing.T) {
	svc := newService(t)
	_, err := svc.SubmitJob(ctxT(t), &api.SubmitJobRequest{JobID: "not-a-uuid", Details: map[string]string{"script": "x"}})
	if err == nil {
		t.Fatal("expected error for non-UUID job_id")
	}
}

// No client id → server assigns one (historical behavior), and two submits are
// two distinct jobs.
func TestSubmitJobNoClientIDServerAssigns(t *testing.T) {
	svc := newService(t)
	ctx := ctxT(t)
	a, err := svc.SubmitJob(ctx, &api.SubmitJobRequest{Details: map[string]string{"script": "x"}})
	if err != nil {
		t.Fatalf("SubmitJob a: %v", err)
	}
	b, err := svc.SubmitJob(ctx, &api.SubmitJobRequest{Details: map[string]string{"script": "x"}})
	if err != nil {
		t.Fatalf("SubmitJob b: %v", err)
	}
	if a.JobID == "" || b.JobID == "" || a.JobID == b.JobID {
		t.Fatalf("expected two distinct server-assigned ids, got %q and %q", a.JobID, b.JobID)
	}
	if n := countJobs(t, svc); n != 2 {
		t.Fatalf("job count = %d, want 2", n)
	}
}

// Submitting an array twice with the same client-assigned ArrayID returns the
// same array — the tasks are not re-expanded.
func TestSubmitArrayIdempotentWithClientID(t *testing.T) {
	svc := newService(t)
	ctx := ctxT(t)
	aid := support.NewUUID()
	req := func() *api.SubmitArrayRequest {
		return &api.SubmitArrayRequest{
			SubmitJobRequest: api.SubmitJobRequest{Details: map[string]string{"script": "x"}},
			ArrayID:          aid,
			ArrayIndices:     []int{1, 2, 3},
		}
	}

	r1, err := svc.SubmitArray(ctx, req())
	if err != nil {
		t.Fatalf("SubmitArray #1: %v", err)
	}
	if r1.ArrayID != aid || len(r1.JobIDs) != 3 {
		t.Fatalf("array #1 = %+v, want id %s with 3 tasks", r1, aid)
	}

	r2, err := svc.SubmitArray(ctx, req())
	if err != nil {
		t.Fatalf("SubmitArray #2 (retry): %v", err)
	}
	if r2.ArrayID != aid {
		t.Fatalf("retry array id = %q, want %q", r2.ArrayID, aid)
	}
	// Same three tasks, not six.
	if n := countJobs(t, svc); n != 3 {
		t.Fatalf("total jobs = %d, want 3 (array not re-expanded)", n)
	}
}
