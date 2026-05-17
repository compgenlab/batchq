package storage

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mbreese/batchq/jobs"
)

// newTestStore opens a fresh sqlite Storage in a temp directory and registers
// a Close on test cleanup.
func newTestStore(t *testing.T) Storage {
	t.Helper()
	path := filepath.Join(t.TempDir(), "batchq.db")
	s, err := Open(context.Background(), path, Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// mkJob builds an in-memory JobDef ready to pass to InsertJob.
func mkJob(id string, details map[string]string, deps ...string) *jobs.JobDef {
	j := &jobs.JobDef{JobId: id, Name: id}
	for k, v := range details {
		j.Details = append(j.Details, jobs.JobDefDetail{Key: k, Value: v})
	}
	j.AfterOk = append(j.AfterOk, deps...)
	return j
}

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// --- Basic CRUD --------------------------------------------------------

func TestInsertAndGet(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	job := mkJob("j-1", map[string]string{"procs": "4", "mem": "1024", "script": "echo hi"})
	if err := s.InsertJob(ctx, job); err != nil {
		t.Fatalf("InsertJob: %v", err)
	}
	if job.Status != jobs.QUEUED {
		t.Fatalf("status: got %s, want QUEUED", job.Status)
	}

	got, err := s.GetJob(ctx, "j-1")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.JobId != "j-1" || got.Status != jobs.QUEUED {
		t.Fatalf("got %+v", got)
	}
	if got.Name != "j-1" {
		t.Fatalf("name: got %q, want %q", got.Name, "j-1")
	}
	if len(got.Details) != 3 {
		t.Fatalf("details: got %d, want 3", len(got.Details))
	}
	if got.SubmitTime.IsZero() {
		t.Fatalf("submit_time not set")
	}
}

func TestInsertWithJobIDPlaceholderInName(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	job := &jobs.JobDef{JobId: "abc-123", Name: "batchq-%JOBID-task"}
	if err := s.InsertJob(ctx, job); err != nil {
		t.Fatalf("InsertJob: %v", err)
	}
	got, _ := s.GetJob(ctx, "abc-123")
	if got.Name != "batchq-abc-123-task" {
		t.Fatalf("name: got %q", got.Name)
	}
}

func TestInsertWithStdoutPlaceholder(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	job := mkJob("j-out", map[string]string{
		"stdout": "/logs/job-%JOBID.out",
		"stderr": "/logs/job-%JOBID.err",
	})
	if err := s.InsertJob(ctx, job); err != nil {
		t.Fatalf("InsertJob: %v", err)
	}
	got, _ := s.GetJob(ctx, "j-out")

	var stdout, stderr string
	for _, d := range got.Details {
		switch d.Key {
		case "stdout":
			stdout = d.Value
		case "stderr":
			stderr = d.Value
		}
	}
	if stdout != "/logs/job-j-out.out" {
		t.Fatalf("stdout: %q", stdout)
	}
	if stderr != "/logs/job-j-out.err" {
		t.Fatalf("stderr: %q", stderr)
	}
}

func TestGetJobNotFound(t *testing.T) {
	s := newTestStore(t)
	_, err := s.GetJob(ctxT(t), "missing")
	if err != ErrJobNotFound {
		t.Fatalf("err: got %v, want ErrJobNotFound", err)
	}
}

// --- Dependencies ------------------------------------------------------

func TestInsertWithDepWaiting(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	parent := mkJob("p", nil)
	if err := s.InsertJob(ctx, parent); err != nil {
		t.Fatalf("InsertJob parent: %v", err)
	}
	child := mkJob("c", nil, "p")
	if err := s.InsertJob(ctx, child); err != nil {
		t.Fatalf("InsertJob child: %v", err)
	}
	got, _ := s.GetJob(ctx, "c")
	if got.Status != jobs.WAITING {
		t.Fatalf("child status: got %s, want WAITING", got.Status)
	}
	if len(got.AfterOk) != 1 || got.AfterOk[0] != "p" {
		t.Fatalf("deps: %v", got.AfterOk)
	}
}

func TestResolveDepsPromotesWhenParentSucceeds(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	mustInsert(t, s, mkJob("p", nil))
	mustInsert(t, s, mkJob("c", nil, "p"))

	// Drive parent through claim → end(success).
	claim, _ := s.ClaimNextJob(ctx, "runner-1", "simple", Limits{})
	if claim.Job == nil || claim.Job.JobId != "p" {
		t.Fatalf("claim: %+v", claim)
	}
	if err := s.EndJob(ctx, "p", "runner-1", 0); err != nil {
		t.Fatalf("EndJob: %v", err)
	}
	res, err := s.ResolveDependencies(ctx)
	if err != nil {
		t.Fatalf("ResolveDependencies: %v", err)
	}
	if res.Promoted != 1 || res.Canceled != 0 {
		t.Fatalf("resolve: %+v", res)
	}
	c, _ := s.GetJob(ctx, "c")
	if c.Status != jobs.QUEUED {
		t.Fatalf("child status: got %s", c.Status)
	}
}

func TestResolveDepsCancelsWhenParentFails(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	mustInsert(t, s, mkJob("p", nil))
	mustInsert(t, s, mkJob("c", nil, "p"))

	_, _ = s.ClaimNextJob(ctx, "r", "simple", Limits{})
	if err := s.EndJob(ctx, "p", "r", 1); err != nil {
		t.Fatalf("EndJob: %v", err)
	}
	// EndJob with non-zero return code already cascade-cancels children.
	c, _ := s.GetJob(ctx, "c")
	if c.Status != jobs.CANCELED {
		t.Fatalf("child status after parent failure: got %s, want CANCELED", c.Status)
	}
}

// --- Atomic claim ------------------------------------------------------

func TestClaimNextJobBasic(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	mustInsert(t, s, mkJob("a", nil))
	mustInsert(t, s, mkJob("b", nil))

	claim, err := s.ClaimNextJob(ctx, "r1", "simple", Limits{})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if claim.Job == nil {
		t.Fatalf("expected a job")
	}
	if claim.Job.Status != jobs.RUNNING {
		t.Fatalf("status: %s", claim.Job.Status)
	}
	// Claiming again returns the other job.
	claim2, _ := s.ClaimNextJob(ctx, "r2", "simple", Limits{})
	if claim2.Job == nil {
		t.Fatalf("expected second job")
	}
	if claim.Job.JobId == claim2.Job.JobId {
		t.Fatalf("same job claimed twice")
	}
}

func TestClaimNextJobLimits(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	mustInsert(t, s, mkJob("small", map[string]string{"procs": "1"}))
	mustInsert(t, s, mkJob("big", map[string]string{"procs": "16"}))

	claim, _ := s.ClaimNextJob(ctx, "r1", "simple", Limits{MaxProcs: 4})
	if claim.Job == nil || claim.Job.JobId != "small" {
		t.Fatalf("expected small, got %+v", claim.Job)
	}
	if !claim.MoreEligible {
		t.Fatalf("expected MoreEligible=true (big remains)")
	}
}

func TestClaimNextJobEmpty(t *testing.T) {
	s := newTestStore(t)
	claim, err := s.ClaimNextJob(ctxT(t), "r1", "simple", Limits{})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if claim.Job != nil || claim.MoreEligible {
		t.Fatalf("unexpected: %+v", claim)
	}
}

// TestClaimNextJobConcurrent is the regression test for the v1
// FetchNext+StartJob race. N goroutines try to claim N jobs at once; every
// job should be claimed by exactly one goroutine.
func TestClaimNextJobConcurrent(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	const N = 10
	for i := 0; i < N; i++ {
		mustInsert(t, s, mkJob(jobIDFor(i), nil))
	}

	var wg sync.WaitGroup
	wins := make(chan string, N*2)
	for i := 0; i < N*2; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			claim, err := s.ClaimNextJob(ctx, runnerIDFor(i), "simple", Limits{})
			if err != nil {
				t.Errorf("claim: %v", err)
				return
			}
			if claim.Job != nil {
				wins <- claim.Job.JobId
			}
		}()
	}
	wg.Wait()
	close(wins)

	seen := map[string]bool{}
	count := 0
	for jobID := range wins {
		if seen[jobID] {
			t.Fatalf("job %s claimed twice", jobID)
		}
		seen[jobID] = true
		count++
	}
	if count != N {
		t.Fatalf("claimed %d, want %d", count, N)
	}
}

// --- Transitions -------------------------------------------------------

func TestEndJobSuccess(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	mustInsert(t, s, mkJob("j", nil))
	_, _ = s.ClaimNextJob(ctx, "r", "simple", Limits{})
	if err := s.EndJob(ctx, "j", "r", 0); err != nil {
		t.Fatalf("EndJob: %v", err)
	}
	got, _ := s.GetJob(ctx, "j")
	if got.Status != jobs.SUCCESS {
		t.Fatalf("status: %s", got.Status)
	}
	if got.EndTime.IsZero() {
		t.Fatalf("end_time not set")
	}
}

func TestEndJobWrongRunner(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	mustInsert(t, s, mkJob("j", nil))
	_, _ = s.ClaimNextJob(ctx, "r1", "simple", Limits{})
	if err := s.EndJob(ctx, "j", "r2-wrong", 0); err != ErrInvalidState {
		t.Fatalf("err: got %v, want ErrInvalidState", err)
	}
}

func TestMarkJobProxiedAndEndProxied(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	mustInsert(t, s, mkJob("j", nil))
	_, _ = s.ClaimNextJob(ctx, "r", "slurm", Limits{})
	if err := s.MarkJobProxied(ctx, "j", "r", map[string]string{"slurm_job_id": "1234"}); err != nil {
		t.Fatalf("MarkJobProxied: %v", err)
	}
	got, _ := s.GetJob(ctx, "j")
	if got.Status != jobs.PROXYQUEUED {
		t.Fatalf("status: %s", got.Status)
	}

	now := time.Now().UTC()
	if err := s.EndProxiedJob(ctx, "j", jobs.SUCCESS, now.Add(-time.Minute), now, 0); err != nil {
		t.Fatalf("EndProxiedJob: %v", err)
	}
	got, _ = s.GetJob(ctx, "j")
	if got.Status != jobs.SUCCESS {
		t.Fatalf("status after proxy end: %s", got.Status)
	}
}

func TestCancelJobCascades(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	mustInsert(t, s, mkJob("a", nil))
	mustInsert(t, s, mkJob("b", nil, "a"))
	mustInsert(t, s, mkJob("c", nil, "b"))

	if err := s.CancelJob(ctx, "a", "user request"); err != nil {
		t.Fatalf("CancelJob: %v", err)
	}

	for _, id := range []string{"a", "b", "c"} {
		got, _ := s.GetJob(ctx, id)
		if got.Status != jobs.CANCELED {
			t.Fatalf("%s status: got %s, want CANCELED", id, got.Status)
		}
	}
}

func TestHoldRelease(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	mustInsert(t, s, mkJob("j", nil))
	if err := s.HoldJob(ctx, "j"); err != nil {
		t.Fatalf("HoldJob: %v", err)
	}
	got, _ := s.GetJob(ctx, "j")
	if got.Status != jobs.USERHOLD {
		t.Fatalf("status: %s", got.Status)
	}
	if err := s.ReleaseJob(ctx, "j"); err != nil {
		t.Fatalf("ReleaseJob: %v", err)
	}
	got, _ = s.GetJob(ctx, "j")
	if got.Status != jobs.WAITING {
		t.Fatalf("status: %s", got.Status)
	}
}

func TestAdjustJobPriority(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	mustInsert(t, s, mkJob("j", nil))
	if err := s.AdjustJobPriority(ctx, "j", 5); err != nil {
		t.Fatalf("Adjust: %v", err)
	}
	got, _ := s.GetJob(ctx, "j")
	if got.Priority != 5 {
		t.Fatalf("priority: got %d, want 5", got.Priority)
	}
	if err := s.AdjustJobPriority(ctx, "j", -2); err != nil {
		t.Fatalf("Adjust -2: %v", err)
	}
	got, _ = s.GetJob(ctx, "j")
	if got.Priority != 3 {
		t.Fatalf("priority: got %d, want 3", got.Priority)
	}
}

func TestCleanupJob(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	mustInsert(t, s, mkJob("j", map[string]string{"script": "x"}))
	_, _ = s.ClaimNextJob(ctx, "r", "simple", Limits{})
	_ = s.UpdateRunningDetails(ctx, "j", map[string]string{"pid": "9999"})
	_ = s.EndJob(ctx, "j", "r", 0)

	if err := s.CleanupJob(ctx, "j"); err != nil {
		t.Fatalf("CleanupJob: %v", err)
	}
	if _, err := s.GetJob(ctx, "j"); err != ErrJobNotFound {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}

func TestUpdateRunningDetails(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	mustInsert(t, s, mkJob("j", nil))
	_, _ = s.ClaimNextJob(ctx, "r", "simple", Limits{})
	if err := s.UpdateRunningDetails(ctx, "j", map[string]string{"pid": "1234", "host": "node-1"}); err != nil {
		t.Fatalf("UpdateRunningDetails: %v", err)
	}
	if err := s.UpdateRunningDetails(ctx, "j", map[string]string{"pid": "5678"}); err != nil {
		t.Fatalf("UpdateRunningDetails (replace): %v", err)
	}
	got, _ := s.GetJob(ctx, "j")
	rd := map[string]string{}
	for _, d := range got.RunningDetails {
		rd[d.Key] = d.Value
	}
	if rd["pid"] != "5678" || rd["host"] != "node-1" {
		t.Fatalf("running details: %+v", rd)
	}
}

// --- Listings ----------------------------------------------------------

func TestListAndCounts(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	mustInsert(t, s, mkJob("a", nil))
	mustInsert(t, s, mkJob("b", nil))
	mustInsert(t, s, mkJob("c", nil))

	all, err := s.ListJobs(ctx, false, false)
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("count: %d", len(all))
	}

	counts, err := s.GetJobStatusCounts(ctx, false)
	if err != nil {
		t.Fatalf("counts: %v", err)
	}
	if counts[jobs.QUEUED] != 3 {
		t.Fatalf("queued: %d", counts[jobs.QUEUED])
	}
}

func TestSearchJobs(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	mustInsert(t, s, mkJob("alpha-1", map[string]string{"script": "echo hello"}))
	mustInsert(t, s, mkJob("beta-1", map[string]string{"script": "rm -rf /"}))

	results, _ := s.SearchJobs(ctx, "alpha", nil)
	if len(results) != 1 || results[0].JobId != "alpha-1" {
		t.Fatalf("results: %+v", results)
	}
	results, _ = s.SearchJobs(ctx, "rm -rf", nil)
	if len(results) != 1 || results[0].JobId != "beta-1" {
		t.Fatalf("script search: %+v", results)
	}

	// Input/output file paths should also be searchable.
	withInputs := mkJob("gamma-1", map[string]string{"script": "noop"})
	withInputs.InputFiles = []string{"/data/raw/sample-A.fastq"}
	withInputs.OutputFiles = []string{"/data/clean/sample-A.bam"}
	mustInsert(t, s, withInputs)

	results, _ = s.SearchJobs(ctx, "sample-A.fastq", nil)
	if len(results) != 1 || results[0].JobId != "gamma-1" {
		t.Fatalf("input-file search: %+v", results)
	}
	results, _ = s.SearchJobs(ctx, "clean/sample", nil)
	if len(results) != 1 || results[0].JobId != "gamma-1" {
		t.Fatalf("output-file search: %+v", results)
	}
}

func TestGetQueueJobs(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	mustInsert(t, s, mkJob("j", map[string]string{"procs": "4", "mem": "1024", "script": "x"}))
	queue, err := s.GetQueueJobs(ctx, false, false)
	if err != nil {
		t.Fatalf("GetQueueJobs: %v", err)
	}
	if len(queue) != 1 || queue[0].JobId != "j" {
		t.Fatalf("queue: %+v", queue)
	}
	gotProcs := false
	for _, d := range queue[0].Details {
		if d.Key == "procs" && d.Value == "4" {
			gotProcs = true
		}
	}
	if !gotProcs {
		t.Fatalf("procs detail missing")
	}
}

func TestGetJobDependents(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	mustInsert(t, s, mkJob("p", nil))
	mustInsert(t, s, mkJob("c1", nil, "p"))
	mustInsert(t, s, mkJob("c2", nil, "p"))

	deps, err := s.GetJobDependents(ctx, "p")
	if err != nil {
		t.Fatalf("GetJobDependents: %v", err)
	}
	if len(deps) != 2 {
		t.Fatalf("deps: %+v", deps)
	}
}

// --- helpers -----------------------------------------------------------

func mustInsert(t *testing.T, s Storage, job *jobs.JobDef) {
	t.Helper()
	if err := s.InsertJob(context.Background(), job); err != nil {
		t.Fatalf("InsertJob %s: %v", job.JobId, err)
	}
}

var jobIDCounter atomic.Int64

func jobIDFor(i int) string {
	return "job-" + itoa(i)
}

func runnerIDFor(i int) string {
	return "runner-" + itoa(i)
}

func itoa(i int) string {
	return string(rune('a'+i%26)) + "-" + intToStr(i)
}

func intToStr(i int) string {
	if i == 0 {
		return "0"
	}
	var out []byte
	for i > 0 {
		out = append([]byte{byte('0' + i%10)}, out...)
		i /= 10
	}
	return string(out)
}

// --- Phase 12: run_id, input/output files ------------------------------

func TestInsertJobPersistsInputsOutputsAndRunID(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	j := mkJob("j-files", map[string]string{
		"script": "echo hi",
		"run_id": "run-2025-Q1",
	})
	j.InputFiles = []string{"/data/in1.fq", "/data/in2.fq", "/data/in1.fq"} // dedupe
	j.OutputFiles = []string{"/data/out.bam"}
	if err := s.InsertJob(ctx, j); err != nil {
		t.Fatalf("InsertJob: %v", err)
	}

	got, err := s.GetJob(ctx, "j-files")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if len(got.InputFiles) != 2 || got.InputFiles[0] != "/data/in1.fq" || got.InputFiles[1] != "/data/in2.fq" {
		t.Fatalf("InputFiles: %v", got.InputFiles)
	}
	if len(got.OutputFiles) != 1 || got.OutputFiles[0] != "/data/out.bam" {
		t.Fatalf("OutputFiles: %v", got.OutputFiles)
	}
	if got.RunID() != "run-2025-Q1" {
		t.Fatalf("RunID: %q", got.RunID())
	}
}

func TestFindJobsByOutputPath(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	producer := mkJob("producer", map[string]string{"script": "x"})
	producer.OutputFiles = []string{"/data/shared.bam"}
	consumer := mkJob("consumer", map[string]string{"script": "x"})
	consumer.InputFiles = []string{"/data/shared.bam"}
	mustInsert(t, s, producer)
	mustInsert(t, s, consumer)

	ids, err := s.FindJobsByOutputPath(ctx, "/data/shared.bam")
	if err != nil {
		t.Fatalf("FindJobsByOutputPath: %v", err)
	}
	if len(ids) != 1 || ids[0] != "producer" {
		t.Fatalf("produces: %v", ids)
	}
	ids, err = s.FindJobsByInputPath(ctx, "/data/shared.bam")
	if err != nil {
		t.Fatalf("FindJobsByInputPath: %v", err)
	}
	if len(ids) != 1 || ids[0] != "consumer" {
		t.Fatalf("consumes: %v", ids)
	}
}

func TestFindJobsByDetailRunID(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	mustInsert(t, s, mkJob("a", map[string]string{"script": "x", "run_id": "run-A"}))
	mustInsert(t, s, mkJob("b", map[string]string{"script": "x", "run_id": "run-A"}))
	mustInsert(t, s, mkJob("c", map[string]string{"script": "x", "run_id": "run-B"}))

	ids, err := s.FindJobsByDetail(ctx, "run_id", "run-A")
	if err != nil {
		t.Fatalf("FindJobsByDetail: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("run-A: %v (want 2 jobs)", ids)
	}
}

func TestCleanupRemovesInputsOutputs(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	j := mkJob("j-clean", map[string]string{"script": "x"})
	j.InputFiles = []string{"/data/in.fq"}
	j.OutputFiles = []string{"/data/out.bam"}
	mustInsert(t, s, j)

	// Force terminal status so CleanupJob accepts it.
	if err := s.CancelJob(ctx, "j-clean", "test"); err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	if err := s.CleanupJob(ctx, "j-clean"); err != nil {
		t.Fatalf("CleanupJob: %v", err)
	}

	// All reverse lookups should be empty now.
	for _, name := range []string{"FindJobsByInputPath", "FindJobsByOutputPath"} {
		var fn func(context.Context, string) ([]string, error)
		switch name {
		case "FindJobsByInputPath":
			fn = s.FindJobsByInputPath
		case "FindJobsByOutputPath":
			fn = s.FindJobsByOutputPath
		}
		ids, err := fn(ctx, "/data/in.fq")
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(ids) != 0 {
			t.Fatalf("%s after cleanup: %v", name, ids)
		}
		ids, err = fn(ctx, "/data/out.bam")
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(ids) != 0 {
			t.Fatalf("%s after cleanup: %v", name, ids)
		}
	}
}
