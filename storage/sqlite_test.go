package storage

import (
	"context"
	"path/filepath"
	"strconv"
	"strings"
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
	if err := s.EndJob(ctx, "p", "runner-1", 0, ""); err != nil {
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
	if err := s.EndJob(ctx, "p", "r", 1, ""); err != nil {
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
	// "big" doesn't fit -> Blocked, not MoreEligible (which now means a race).
	if !claim.Blocked {
		t.Fatalf("expected Blocked=true (big remains)")
	}
	if claim.MoreEligible {
		t.Fatalf("expected MoreEligible=false (no claim race)")
	}
}

func TestClaimNextJobResources(t *testing.T) {
	claims := func(t *testing.T, jobRes, advertised map[string]string) bool {
		t.Helper()
		s := newTestStore(t)
		details := map[string]string{}
		for k, v := range jobRes {
			details[jobs.ResourcePrefix+k] = v
		}
		mustInsert(t, s, mkJob("j", details))
		claim, err := s.ClaimNextJob(ctxT(t), "r", "simple", Limits{Resources: advertised})
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		return claim.Job != nil
	}

	cases := []struct {
		name      string
		jobRes    map[string]string
		advert    map[string]string
		wantClaim bool
	}{
		{"countable fits", map[string]string{"gpu": "2"}, map[string]string{"gpu": "4"}, true},
		{"countable short", map[string]string{"gpu": "8"}, map[string]string{"gpu": "4"}, false},
		{"countable absent", map[string]string{"gpu": "1"}, nil, false},
		{"typed exact fits", map[string]string{"gpu:a100": "2"}, map[string]string{"gpu:a100": "2"}, true},
		{"typed wrong type", map[string]string{"gpu:a100": "1"}, map[string]string{"gpu:v100": "4"}, false},
		{"untyped aggregates typed", map[string]string{"gpu": "3"}, map[string]string{"gpu:a100": "2", "gpu:v100": "2"}, true},
		{"typed does not aggregate untyped", map[string]string{"gpu:a100": "2"}, map[string]string{"gpu": "4"}, false},
		{"label equal", map[string]string{"cluster": "xyz"}, map[string]string{"cluster": "xyz"}, true},
		{"label mismatch", map[string]string{"cluster": "xyz"}, map[string]string{"cluster": "abc"}, false},
		{"feature subset", map[string]string{"features": "avx512,avx2"}, map[string]string{"features": "avx512,avx2,sse4"}, true},
		{"feature missing one", map[string]string{"features": "avx512,avx999"}, map[string]string{"features": "avx512,avx2"}, false},
		{"bare flag present", map[string]string{"fastio": ""}, map[string]string{"fastio": "true"}, true},
		{"bare flag absent", map[string]string{"fastio": ""}, nil, false},
		{"no requirement, no advert", nil, nil, true},
		{"no requirement, advert present", nil, map[string]string{"gpu": "4"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := claims(t, tc.jobRes, tc.advert); got != tc.wantClaim {
				t.Fatalf("claim=%v, want %v", got, tc.wantClaim)
			}
		})
	}
}

// A runner that advertises nothing must skip a resource-tagged job but still
// claim a resource-free one later in the queue (the cardinal rule).
func TestClaimNextJobResourcesSkipsTagged(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	mustInsert(t, s, mkJob("a-gpu", map[string]string{jobs.ResourcePrefix + "gpu": "1"}))
	mustInsert(t, s, mkJob("b-free", nil))

	claim, _ := s.ClaimNextJob(ctx, "r", "simple", Limits{})
	if claim.Job == nil || claim.Job.JobId != "b-free" {
		t.Fatalf("expected b-free, got %+v", claim.Job)
	}
	// The gpu job can't run here -> Blocked, not MoreEligible. A runner at full
	// capacity uses Blocked to stop rather than poll for it forever.
	if !claim.Blocked {
		t.Fatalf("expected Blocked=true (gpu job remains)")
	}
	if claim.MoreEligible {
		t.Fatalf("expected MoreEligible=false (no claim race)")
	}
}

// A job that fits the runner's limits but is already locked in job_running
// (another runner won the race) yields MoreEligible (retry), not Blocked.
func TestClaimNextJobRace(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	mustInsert(t, s, mkJob("contested", nil))

	// Simulate a competing runner that already locked the row: insert the
	// job_running lock directly while the job is still QUEUED, so our claim's
	// INSERT hits the unique-violation (race) path.
	sq := s.(*sqliteStorage)
	if _, err := sq.db.ExecContext(ctx,
		`INSERT INTO job_running (job_id, job_runner, kind) VALUES (?, ?, ?)`,
		"contested", "other-runner", "simple"); err != nil {
		t.Fatalf("pre-lock: %v", err)
	}

	claim, err := s.ClaimNextJob(ctx, "r1", "simple", Limits{})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claim.Job != nil {
		t.Fatalf("expected no claim, got %+v", claim.Job)
	}
	if !claim.MoreEligible {
		t.Fatalf("expected MoreEligible=true (lost race)")
	}
	if claim.Blocked {
		t.Fatalf("expected Blocked=false (job fit our limits)")
	}
}

// mkArrayTask builds one array task-job carrying the array_id/array_index/
// array_size (and optional array_throttle) details the service would add.
func mkArrayTask(id, arrayID string, index, size, throttle int, extra map[string]string) *jobs.JobDef {
	j := &jobs.JobDef{JobId: id}
	j.Details = append(j.Details,
		jobs.JobDefDetail{Key: "script", Value: "#!/bin/sh\necho hi"},
		jobs.JobDefDetail{Key: "array_id", Value: arrayID},
		jobs.JobDefDetail{Key: "array_index", Value: strconv.Itoa(index)},
		jobs.JobDefDetail{Key: "array_size", Value: strconv.Itoa(size)},
	)
	if throttle > 0 {
		j.Details = append(j.Details, jobs.JobDefDetail{Key: "array_throttle", Value: strconv.Itoa(throttle)})
	}
	for k, v := range extra {
		j.Details = append(j.Details, jobs.JobDefDetail{Key: k, Value: v})
	}
	return j
}

func TestInsertArray(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	const arrayID = "arr-1"
	var tasks []*jobs.JobDef
	for _, idx := range []int{0, 1, 2} {
		task := mkArrayTask("t-"+strconv.Itoa(idx), arrayID, idx, 3, 0,
			map[string]string{"stdout": "out-%A_%a.log"})
		task.Name = "samp-%a"
		tasks = append(tasks, task)
	}
	if err := s.InsertArray(ctx, arrayID, tasks); err != nil {
		t.Fatalf("InsertArray: %v", err)
	}

	members, err := s.FindJobsByDetail(ctx, "array_id", arrayID)
	if err != nil {
		t.Fatalf("FindJobsByDetail: %v", err)
	}
	if len(members) != 3 {
		t.Fatalf("expected 3 array members, got %d", len(members))
	}

	for _, idx := range []int{0, 1, 2} {
		job, err := s.GetJob(ctx, "t-"+strconv.Itoa(idx))
		if err != nil {
			t.Fatalf("GetJob: %v", err)
		}
		if job.Status != jobs.QUEUED {
			t.Fatalf("task %d status = %v, want QUEUED", idx, job.Status)
		}
		// %a/%A are substituted in the name at insert, but deferred (left as a
		// template) in stdout/stderr so a SLURM array's single -o pattern works.
		if want := "samp-" + strconv.Itoa(idx); job.Name != want {
			t.Fatalf("task %d name = %q, want %q", idx, job.Name, want)
		}
		if got := job.GetDetail("stdout", ""); got != "out-%A_%a.log" {
			t.Fatalf("task %d stdout = %q, want template unchanged", idx, got)
		}
		if got := job.GetDetail("array_size", ""); got != "3" {
			t.Fatalf("task %d array_size = %q, want 3", idx, got)
		}
	}
}

// FindArrayMembers returns id+index for every task in one query, and nothing
// for a non-array id. Backs the N+1-free array dependency expansion.
func TestFindArrayMembers(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	const arrayID = "arr-fm"
	var tasks []*jobs.JobDef
	for _, idx := range []int{0, 1, 2} {
		tasks = append(tasks, mkArrayTask("fm-"+strconv.Itoa(idx), arrayID, idx, 3, 0, nil))
	}
	if err := s.InsertArray(ctx, arrayID, tasks); err != nil {
		t.Fatalf("InsertArray: %v", err)
	}

	members, err := s.FindArrayMembers(ctx, arrayID)
	if err != nil {
		t.Fatalf("FindArrayMembers: %v", err)
	}
	if len(members) != 3 {
		t.Fatalf("expected 3 members, got %d", len(members))
	}
	got := map[string]int{}
	for _, m := range members {
		got[m.ID] = m.Index
	}
	for _, idx := range []int{0, 1, 2} {
		id := "fm-" + strconv.Itoa(idx)
		if got[id] != idx {
			t.Errorf("member %q index = %d, want %d", id, got[id], idx)
		}
	}

	// A non-array id yields no members.
	none, err := s.FindArrayMembers(ctx, "not-an-array")
	if err != nil {
		t.Fatalf("FindArrayMembers(non-array): %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("expected 0 members for non-array id, got %d", len(none))
	}
}

// InsertArray must be atomic: a failure mid-array (here a duplicate task id)
// rolls back every task.
func TestInsertArrayAtomic(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	tasks := []*jobs.JobDef{
		mkArrayTask("dup", "arr", 0, 2, 0, nil),
		mkArrayTask("dup", "arr", 1, 2, 0, nil), // duplicate id → PK violation
	}
	if err := s.InsertArray(ctx, "arr", tasks); err == nil {
		t.Fatal("expected error from duplicate task id")
	}
	members, _ := s.FindJobsByDetail(ctx, "array_id", "arr")
	if len(members) != 0 {
		t.Fatalf("expected full rollback, found %d members", len(members))
	}
}

// A per-array throttle ("%N") caps how many tasks of one array run at once via
// the claim gate.
func TestClaimNextJobArrayThrottle(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	var tasks []*jobs.JobDef
	for _, idx := range []int{0, 1, 2} {
		tasks = append(tasks, mkArrayTask("a"+strconv.Itoa(idx), "arr", idx, 3, 1, nil))
	}
	if err := s.InsertArray(ctx, "arr", tasks); err != nil {
		t.Fatalf("InsertArray: %v", err)
	}

	c1, _ := s.ClaimNextJob(ctx, "r1", "simple", Limits{})
	if c1.Job == nil {
		t.Fatal("expected first task to be claimed")
	}
	// One task is RUNNING and throttle=1, so no second task may be claimed.
	c2, _ := s.ClaimNextJob(ctx, "r2", "simple", Limits{})
	if c2.Job != nil {
		t.Fatalf("throttle: expected no claim, got %s", c2.Job.JobId)
	}
	if !c2.Blocked {
		t.Fatal("expected Blocked=true (array at throttle)")
	}
}

// ClaimNextArrayBatch claims up to maxTasks of an array's QUEUED tasks per call
// (in index order), leaving the rest QUEUED — the drip-feed the SLURM runner
// relies on.
func TestClaimNextArrayBatch(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	var tasks []*jobs.JobDef
	for _, idx := range []int{0, 1, 2, 3, 4} {
		tasks = append(tasks, mkArrayTask("t"+strconv.Itoa(idx), "arr", idx, 5, 3, nil))
	}
	if err := s.InsertArray(ctx, "arr", tasks); err != nil {
		t.Fatalf("InsertArray: %v", err)
	}

	r1, err := s.ClaimNextArrayBatch(ctx, "slurm-1", "slurm", Limits{}, 2)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if r1.ArrayID != "arr" {
		t.Fatalf("ArrayID = %q, want arr", r1.ArrayID)
	}
	if r1.Throttle != 3 {
		t.Fatalf("Throttle = %d, want 3", r1.Throttle)
	}
	if r1.Job == nil {
		t.Fatal("expected a representative Job for the batch")
	}
	if len(r1.Tasks) != 2 || r1.Tasks[0].Index != 0 || r1.Tasks[1].Index != 1 {
		t.Fatalf("first batch tasks = %+v, want indices [0 1]", r1.Tasks)
	}

	r2, _ := s.ClaimNextArrayBatch(ctx, "slurm-2", "slurm", Limits{}, 2)
	if len(r2.Tasks) != 2 || r2.Tasks[0].Index != 2 || r2.Tasks[1].Index != 3 {
		t.Fatalf("second batch tasks = %+v, want indices [2 3]", r2.Tasks)
	}

	r3, _ := s.ClaimNextArrayBatch(ctx, "slurm-3", "slurm", Limits{}, 2)
	if len(r3.Tasks) != 1 || r3.Tasks[0].Index != 4 {
		t.Fatalf("third batch tasks = %+v, want index [4]", r3.Tasks)
	}

	r4, _ := s.ClaimNextArrayBatch(ctx, "slurm-4", "slurm", Limits{}, 2)
	if r4.ArrayID != "" || r4.Job != nil || len(r4.Tasks) != 0 {
		t.Fatalf("expected nothing left, got %+v", r4)
	}
}

// A plain (non-array) job claimed via ClaimNextArrayBatch comes back as Job, not
// an array batch.
func TestClaimNextArrayBatchPlainJob(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	mustInsert(t, s, mkJob("plain", map[string]string{"procs": "1"}))
	r, err := s.ClaimNextArrayBatch(ctx, "slurm-1", "slurm", Limits{}, -1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if r.ArrayID != "" || len(r.Tasks) != 0 {
		t.Fatalf("expected a plain job, got array %q with %d tasks", r.ArrayID, len(r.Tasks))
	}
	if r.Job == nil || r.Job.JobId != "plain" {
		t.Fatalf("expected plain job, got %+v", r.Job)
	}
}

// HoldArray/ReleaseArray/CancelArray apply to every task of an array.
func TestArrayOps(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	var tasks []*jobs.JobDef
	for _, idx := range []int{0, 1, 2} {
		tasks = append(tasks, mkArrayTask("t"+strconv.Itoa(idx), "arr", idx, 3, 0, nil))
	}
	if err := s.InsertArray(ctx, "arr", tasks); err != nil {
		t.Fatalf("InsertArray: %v", err)
	}
	ids := []string{"t0", "t1", "t2"}

	if n, err := s.HoldArray(ctx, "arr"); err != nil || n != 3 {
		t.Fatalf("HoldArray: n=%d err=%v", n, err)
	}
	for _, id := range ids {
		if j, _ := s.GetJob(ctx, id); j.Status != jobs.USERHOLD {
			t.Fatalf("%s = %v, want USERHOLD", id, j.Status)
		}
	}

	if n, err := s.ReleaseArray(ctx, "arr"); err != nil || n != 3 {
		t.Fatalf("ReleaseArray: n=%d err=%v", n, err)
	}
	for _, id := range ids {
		if j, _ := s.GetJob(ctx, id); j.Status != jobs.WAITING {
			t.Fatalf("%s = %v, want WAITING", id, j.Status)
		}
	}

	if n, err := s.CancelArray(ctx, "arr", "test"); err != nil || n != 3 {
		t.Fatalf("CancelArray: n=%d err=%v", n, err)
	}
	for _, id := range ids {
		if j, _ := s.GetJob(ctx, id); j.Status != jobs.CANCELED {
			t.Fatalf("%s = %v, want CANCELED", id, j.Status)
		}
	}
}

// Canceling an array cascades to jobs that depend on its tasks.
func TestCancelArrayCascades(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	var tasks []*jobs.JobDef
	for _, idx := range []int{0, 1} {
		tasks = append(tasks, mkArrayTask("t"+strconv.Itoa(idx), "arr", idx, 2, 0, nil))
	}
	if err := s.InsertArray(ctx, "arr", tasks); err != nil {
		t.Fatalf("InsertArray: %v", err)
	}
	mustInsert(t, s, mkJob("dep", nil, "t0")) // depends on task 0

	if _, err := s.CancelArray(ctx, "arr", "test"); err != nil {
		t.Fatalf("CancelArray: %v", err)
	}
	if j, _ := s.GetJob(ctx, "dep"); j.Status != jobs.CANCELED {
		t.Fatalf("dependent = %v, want CANCELED (cascade)", j.Status)
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
	if err := s.EndJob(ctx, "j", "r", 0, ""); err != nil {
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
	if err := s.EndJob(ctx, "j", "r2-wrong", 0, ""); err != ErrInvalidState {
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
	if err := s.EndProxiedJob(ctx, "j", jobs.SUCCESS, now.Add(-time.Minute), now, 0, ""); err != nil {
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
	_ = s.EndJob(ctx, "j", "r", 0, "")

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

	// Searching by run_id should return every job in that run.
	mustInsert(t, s, mkJob("run-a", map[string]string{"run_id": "pipeline-2025-q2"}))
	mustInsert(t, s, mkJob("run-b", map[string]string{"run_id": "pipeline-2025-q2"}))
	mustInsert(t, s, mkJob("run-c", map[string]string{"run_id": "pipeline-2024-q4"}))

	results, _ = s.SearchJobs(ctx, "2025-q2", nil)
	if len(results) != 2 {
		t.Fatalf("run-id search returned %d results, want 2: %+v", len(results), results)
	}
	seen := map[string]bool{}
	for _, j := range results {
		seen[j.JobId] = true
	}
	if !seen["run-a"] || !seen["run-b"] {
		t.Fatalf("run-id search missed expected jobs: %+v", seen)
	}
}

func TestGetQueueJobs(t *testing.T) {
	s := newTestStore(t)
	ctx := ctxT(t)

	mustInsert(t, s, mkJob("j", map[string]string{
		"procs":  "4",
		"mem":    "1024",
		"script": "x",
		"run_id": "pipeline-x",
	}))
	queue, err := s.GetQueueJobs(ctx, false, false)
	if err != nil {
		t.Fatalf("GetQueueJobs: %v", err)
	}
	if len(queue) != 1 || queue[0].JobId != "j" {
		t.Fatalf("queue: %+v", queue)
	}
	gotProcs := false
	gotRun := ""
	for _, d := range queue[0].Details {
		if d.Key == "procs" && d.Value == "4" {
			gotProcs = true
		}
		if d.Key == "run_id" {
			gotRun = d.Value
		}
	}
	if !gotProcs {
		t.Fatalf("procs detail missing")
	}
	if gotRun != "pipeline-x" {
		t.Fatalf("run_id missing from queue projection: %+v", queue[0].Details)
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

// --- DB single-owner lock ----------------------------------------------

// TestSecondOpenIsRejected proves the exclusive DB lock: a second Open of the
// same path while the first is still open fails fast with an actionable error
// (instead of a later cryptic SQLITE_BUSY from two writers colliding).
func TestSecondOpenIsRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "owned.db")
	first, err := Open(context.Background(), path, Options{})
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	defer first.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = Open(ctx, path, Options{})
	if err == nil {
		t.Fatal("second Open succeeded; expected it to be rejected while the first holds the DB")
	}
	if !strings.Contains(err.Error(), "already open by another batchq process") {
		t.Fatalf("second Open error = %q, want the single-owner message", err)
	}
}

// TestReopenAfterCloseSucceeds proves the lock is released on Close so the
// normal idle-shutdown-then-restart handoff still works.
func TestReopenAfterCloseSucceeds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "handoff.db")
	first, err := Open(context.Background(), path, Options{})
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	second, err := Open(context.Background(), path, Options{})
	if err != nil {
		t.Fatalf("reopen after close: %v", err)
	}
	defer second.Close()
}

// --- Transaction-poisoning regression ----------------------------------

// TestBeginTxRecoversPoisonedConnection reproduces the "cannot start a
// transaction within a transaction" failure: a dangling transaction on the
// single shared connection (the state a context-canceled BEGIN leaves behind)
// must not wedge every later write. beginTx's self-heal should clear it.
func TestBeginTxRecoversPoisonedConnection(t *testing.T) {
	s := newTestStore(t).(*sqliteStorage)
	ctx := ctxT(t)

	// Run a raw BEGIN that database/sql is unaware of. With MaxOpenConns(1) the
	// connection returns to the pool still inside a transaction — poisoned.
	if _, err := s.db.ExecContext(ctx, "BEGIN"); err != nil {
		t.Fatalf("seed BEGIN: %v", err)
	}

	// A plain s.db.BeginTx would now fail; InsertJob (via beginTx) must recover.
	job := mkJob("j-poison", map[string]string{"script": "echo hi"})
	if err := s.InsertJob(ctx, job); err != nil {
		t.Fatalf("InsertJob after poison: %v", err)
	}
	got, err := s.GetJob(ctx, "j-poison")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.JobId != "j-poison" || got.Status != jobs.QUEUED {
		t.Fatalf("got %+v", got)
	}

	// And the connection is healthy for the next write.
	if err := s.InsertJob(ctx, mkJob("j-after", nil)); err != nil {
		t.Fatalf("InsertJob after recovery: %v", err)
	}
}

// TestInsertJobIgnoresCanceledContext proves write transactions are decoupled
// from request-context cancellation (context.WithoutCancel): a client that
// disconnects mid-submit can no longer cancel the BEGIN and poison the
// connection. The write completes despite an already-canceled context.
func TestInsertJobIgnoresCanceledContext(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // canceled before the call

	if err := s.InsertJob(ctx, mkJob("j-canceled", map[string]string{"script": "echo hi"})); err != nil {
		t.Fatalf("InsertJob with canceled ctx: %v", err)
	}
	if _, err := s.GetJob(context.Background(), "j-canceled"); err != nil {
		t.Fatalf("GetJob: %v", err)
	}
}
