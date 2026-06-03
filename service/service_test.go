package service

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/mbreese/batchq/api"
	"github.com/mbreese/batchq/jobs"
	"github.com/mbreese/batchq/storage"
)

func newService(t *testing.T) *Service {
	t.Helper()
	path := filepath.Join(t.TempDir(), "batchq.db")
	st, err := storage.Open(context.Background(), path, storage.Options{})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return New(st)
}

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// SubmitJob is the heart of the service: assigns a UUID, persists, and
// returns a DTO. The wire-level invariant is that the response's job_id is
// the same UUID the server picked.
func TestSubmitJobAssignsUUID(t *testing.T) {
	svc := newService(t)
	dto, err := svc.SubmitJob(ctxT(t), &api.SubmitJobRequest{
		Name:    "hello",
		Details: map[string]string{"script": "echo hi"},
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	if dto.JobID == "" {
		t.Fatal("empty JobID")
	}
	if dto.Status != "QUEUED" {
		t.Fatalf("status: %s", dto.Status)
	}
	if dto.Details["script"] != "echo hi" {
		t.Fatalf("script detail missing: %+v", dto.Details)
	}
}

func TestSubmitJobRejectsMissingScript(t *testing.T) {
	svc := newService(t)
	_, err := svc.SubmitJob(ctxT(t), &api.SubmitJobRequest{Name: "x"})
	if err == nil {
		t.Fatal("expected error for missing script")
	}
}

func TestSubmitJobHoldStartsAsUserHold(t *testing.T) {
	svc := newService(t)
	dto, err := svc.SubmitJob(ctxT(t), &api.SubmitJobRequest{
		Hold:    true,
		Details: map[string]string{"script": "x"},
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	if dto.Status != "USERHOLD" {
		t.Fatalf("status: %s", dto.Status)
	}
}

func TestSubmitJobWithDepGoesWaiting(t *testing.T) {
	svc := newService(t)
	parent, _ := svc.SubmitJob(ctxT(t), &api.SubmitJobRequest{Details: map[string]string{"script": "x"}})
	child, err := svc.SubmitJob(ctxT(t), &api.SubmitJobRequest{
		AfterOk: []string{parent.JobID},
		Details: map[string]string{"script": "x"},
	})
	if err != nil {
		t.Fatalf("SubmitJob child: %v", err)
	}
	if child.Status != "WAITING" {
		t.Fatalf("child status: %s", child.Status)
	}
}

// EndJob with success should automatically promote waiters whose only
// remaining dep is this job, without the caller having to invoke
// ResolveDependencies manually.
func TestEndJobSuccessAutoPromotesWaiters(t *testing.T) {
	svc := newService(t)
	ctx := ctxT(t)

	parent, _ := svc.SubmitJob(ctx, &api.SubmitJobRequest{Details: map[string]string{"script": "x"}})
	child, _ := svc.SubmitJob(ctx, &api.SubmitJobRequest{
		AfterOk: []string{parent.JobID},
		Details: map[string]string{"script": "x"},
	})

	claim, err := svc.ClaimNextJob(ctx, "r1", "simple", storage.Limits{})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if claim.Job == nil || claim.Job.JobId != parent.JobID {
		t.Fatalf("claim: %+v", claim.Job)
	}

	if err := svc.EndJob(ctx, "r1", parent.JobID, 0, ""); err != nil {
		t.Fatalf("EndJob: %v", err)
	}

	got, err := svc.GetJob(ctx, child.JobID)
	if err != nil {
		t.Fatalf("GetJob child: %v", err)
	}
	if got.Status != "QUEUED" {
		t.Fatalf("child not auto-promoted; status=%s", got.Status)
	}
}

// CleanupJob should refuse to act on a job that is not in a terminal state.
// This is a service-layer invariant; storage.CleanupJob has no such guard.
func TestCleanupRefusesNonTerminal(t *testing.T) {
	svc := newService(t)
	dto, _ := svc.SubmitJob(ctxT(t), &api.SubmitJobRequest{Details: map[string]string{"script": "x"}})
	err := svc.CleanupJob(ctxT(t), dto.JobID)
	if err == nil {
		t.Fatal("expected error cleaning up QUEUED job")
	}
}

func TestCleanupOnTerminalSucceeds(t *testing.T) {
	svc := newService(t)
	ctx := ctxT(t)
	dto, _ := svc.SubmitJob(ctx, &api.SubmitJobRequest{Details: map[string]string{"script": "x"}})
	_, _ = svc.ClaimNextJob(ctx, "r", "simple", storage.Limits{})
	_ = svc.EndJob(ctx, "r", dto.JobID, 0, "")
	if err := svc.CleanupJob(ctx, dto.JobID); err != nil {
		t.Fatalf("CleanupJob: %v", err)
	}
	if _, err := svc.GetJob(ctx, dto.JobID); err != ErrJobNotFound {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}

func TestListJobsRoutesByOptions(t *testing.T) {
	svc := newService(t)
	ctx := ctxT(t)

	_, _ = svc.SubmitJob(ctx, &api.SubmitJobRequest{Name: "alpha-job", Details: map[string]string{"script": "x"}})
	_, _ = svc.SubmitJob(ctx, &api.SubmitJobRequest{Name: "beta-job", Details: map[string]string{"script": "x"}})

	all, err := svc.ListJobs(ctx, ListJobsOptions{ShowAll: true})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("len: %d", len(all))
	}

	queried, err := svc.ListJobs(ctx, ListJobsOptions{Query: "alpha"})
	if err != nil {
		t.Fatalf("ListJobs(query): %v", err)
	}
	if len(queried) != 1 || queried[0].Name != "alpha-job" {
		t.Fatalf("query results: %+v", queried)
	}

	byStatus, err := svc.ListJobs(ctx, ListJobsOptions{Statuses: []jobs.StatusCode{jobs.QUEUED}})
	if err != nil {
		t.Fatalf("ListJobs(status): %v", err)
	}
	if len(byStatus) != 2 {
		t.Fatalf("byStatus len: %d", len(byStatus))
	}
}

func TestStatusCountsWireFormat(t *testing.T) {
	svc := newService(t)
	_, _ = svc.SubmitJob(ctxT(t), &api.SubmitJobRequest{Details: map[string]string{"script": "x"}})

	counts, err := svc.GetJobStatusCounts(ctxT(t), false)
	if err != nil {
		t.Fatalf("counts: %v", err)
	}
	if counts["QUEUED"] != 1 {
		t.Fatalf("QUEUED: %d", counts["QUEUED"])
	}
}

func TestDTOConversionRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	def := &jobs.JobDef{
		JobId:      "j-1",
		Status:     jobs.RUNNING,
		Priority:   3,
		Name:       "test",
		Notes:      "n",
		SubmitTime: now.Add(-time.Hour),
		StartTime:  now.Add(-time.Minute),
		ReturnCode: 0,
		AfterOk:    []string{"p-1", "p-2"},
		Details: []jobs.JobDefDetail{
			{Key: "procs", Value: "4"},
			{Key: "mem", Value: "2048"},
		},
		RunningDetails: []jobs.JobRunningDetail{
			{Key: "pid", Value: "9999"},
		},
	}
	dto := api.JobFromDef(def)
	if dto.Status != "RUNNING" {
		t.Fatalf("Status: %s", dto.Status)
	}
	if dto.Details["procs"] != "4" || dto.Details["mem"] != "2048" {
		t.Fatalf("Details: %+v", dto.Details)
	}
	if dto.RunningDetails["pid"] != "9999" {
		t.Fatalf("RunningDetails: %+v", dto.RunningDetails)
	}
	if dto.SubmitTime == nil || dto.SubmitTime.Equal(time.Time{}) {
		t.Fatal("SubmitTime not set")
	}
	if dto.EndTime != nil {
		t.Fatal("EndTime should be nil (zero value)")
	}

	back := api.JobToDef(dto)
	if back.JobId != "j-1" || back.Priority != 3 {
		t.Fatalf("roundtrip: %+v", back)
	}
	if len(back.Details) != 2 || len(back.RunningDetails) != 1 {
		t.Fatalf("details roundtrip: %+v", back)
	}
}

func TestParseStatus(t *testing.T) {
	for name, want := range map[string]jobs.StatusCode{
		"QUEUED":      jobs.QUEUED,
		"RUNNING":     jobs.RUNNING,
		"PROXYQUEUED": jobs.PROXYQUEUED,
		"SUCCESS":     jobs.SUCCESS,
		"FAILED":      jobs.FAILED,
		"CANCELED":    jobs.CANCELED,
		"USERHOLD":    jobs.USERHOLD,
		"WAITING":     jobs.WAITING,
	} {
		got, err := api.ParseStatus(name)
		if err != nil {
			t.Errorf("ParseStatus(%q): %v", name, err)
		}
		if got != want {
			t.Errorf("ParseStatus(%q) = %v, want %v", name, got, want)
		}
	}
	if _, err := api.ParseStatus("BOGUS"); err == nil {
		t.Fatal("expected error on unknown status")
	}
}

func TestListJobsFiltersByRunIDAndFiles(t *testing.T) {
	svc := newService(t)
	ctx := ctxT(t)

	mustSubmit := func(name, runID string, inputs, outputs []string) string {
		t.Helper()
		details := map[string]string{"script": "echo " + name}
		if runID != "" {
			details["run_id"] = runID
		}
		dto, err := svc.SubmitJob(ctx, &api.SubmitJobRequest{
			Name:        name,
			Details:     details,
			InputFiles:  inputs,
			OutputFiles: outputs,
		})
		if err != nil {
			t.Fatalf("submit %s: %v", name, err)
		}
		return dto.JobID
	}

	mustSubmit("a", "run-A", []string{"in1"}, []string{"shared"})
	mustSubmit("b", "run-A", []string{"shared"}, []string{"out-b"})
	mustSubmit("c", "run-B", []string{"in1"}, []string{"shared"})

	// RunID filter narrows to two jobs.
	got, err := svc.ListJobs(ctx, ListJobsOptions{ShowAll: true, RunID: "run-A"})
	if err != nil {
		t.Fatalf("ListJobs run-A: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("run-A: got %d jobs, want 2", len(got))
	}

	// Produces filter — two jobs produce "shared".
	got, err = svc.ListJobs(ctx, ListJobsOptions{ShowAll: true, Produces: "shared"})
	if err != nil {
		t.Fatalf("ListJobs produces: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("produces shared: got %d, want 2", len(got))
	}

	// Combine RunID AND Produces — intersection of {a, b} ∩ {a, c} = {a}.
	got, err = svc.ListJobs(ctx, ListJobsOptions{ShowAll: true, RunID: "run-A", Produces: "shared"})
	if err != nil {
		t.Fatalf("ListJobs combined: %v", err)
	}
	if len(got) != 1 || got[0].Name != "a" {
		t.Fatalf("combined: got %v", names(got))
	}

	// Consumes filter — one job consumes "shared".
	got, err = svc.ListJobs(ctx, ListJobsOptions{ShowAll: true, Consumes: "shared"})
	if err != nil {
		t.Fatalf("ListJobs consumes: %v", err)
	}
	if len(got) != 1 || got[0].Name != "b" {
		t.Fatalf("consumes shared: got %v", names(got))
	}
}

func names(dtos []*api.JobDTO) []string {
	out := make([]string, len(dtos))
	for i, d := range dtos {
		out[i] = d.Name
	}
	return out
}

func TestValidateJobID(t *testing.T) {
	if err := ValidateJobID("good-id-123"); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	for _, bad := range []string{"", "with space", "with/slash", "with\ttab"} {
		if err := ValidateJobID(bad); err == nil {
			t.Errorf("expected err for %q", bad)
		}
	}
}
