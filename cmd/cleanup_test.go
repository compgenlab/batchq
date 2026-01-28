package cmd

import (
	"context"
	"reflect"
	"testing"

	"github.com/mbreese/batchq/jobs"
)

type fakeCleanupDB struct {
	jobs       map[string]*jobs.JobDef
	dependents map[string][]string
	order      []string
}

func (f *fakeCleanupDB) GetJobsByStatus(ctx context.Context, statuses []jobs.StatusCode, sortByStatus bool) []*jobs.JobDef {
	allowed := map[jobs.StatusCode]bool{}
	for _, status := range statuses {
		allowed[status] = true
	}

	var out []*jobs.JobDef
	for _, id := range f.order {
		job := f.jobs[id]
		if job != nil && allowed[job.Status] {
			out = append(out, job)
		}
	}
	return out
}

func (f *fakeCleanupDB) GetJob(ctx context.Context, jobId string) *jobs.JobDef {
	return f.jobs[jobId]
}

func (f *fakeCleanupDB) GetJobDependents(ctx context.Context, jobId string) []string {
	return f.dependents[jobId]
}

func TestBuildCleanupPlanRemovesDependentsFirst(t *testing.T) {
	ctx := context.Background()
	db := &fakeCleanupDB{
		jobs: map[string]*jobs.JobDef{
			"P": {JobId: "P", Status: jobs.SUCCESS},
			"C": {JobId: "C", Status: jobs.SUCCESS},
		},
		dependents: map[string][]string{
			"P": {"C"},
		},
		order: []string{"P", "C"},
	}

	plan := buildCleanupPlan(ctx, db, []jobs.StatusCode{jobs.SUCCESS})
	expectedOrder := []string{"C", "P"}
	if !reflect.DeepEqual(plan.removalOrder, expectedOrder) {
		t.Fatalf("unexpected removal order: got %v want %v", plan.removalOrder, expectedOrder)
	}
	if plan.blocked["P"] || plan.blocked["C"] {
		t.Fatalf("unexpected blocked entries: %v", plan.blocked)
	}
}

func TestBuildCleanupPlanBlocksWhenDependentNotEligible(t *testing.T) {
	ctx := context.Background()
	db := &fakeCleanupDB{
		jobs: map[string]*jobs.JobDef{
			"P": {JobId: "P", Status: jobs.SUCCESS},
			"C": {JobId: "C", Status: jobs.RUNNING},
		},
		dependents: map[string][]string{
			"P": {"C"},
		},
		order: []string{"P"},
	}

	plan := buildCleanupPlan(ctx, db, []jobs.StatusCode{jobs.SUCCESS})
	if len(plan.removalOrder) != 0 {
		t.Fatalf("expected no removals, got %v", plan.removalOrder)
	}
	if !plan.blocked["P"] {
		t.Fatalf("expected parent to be blocked")
	}
}

func TestBuildCleanupPlanMultiLevelOrder(t *testing.T) {
	ctx := context.Background()
	db := &fakeCleanupDB{
		jobs: map[string]*jobs.JobDef{
			"G": {JobId: "G", Status: jobs.SUCCESS},
			"P": {JobId: "P", Status: jobs.SUCCESS},
			"C": {JobId: "C", Status: jobs.SUCCESS},
		},
		dependents: map[string][]string{
			"G": {"P"},
			"P": {"C"},
		},
		order: []string{"G", "P", "C"},
	}

	plan := buildCleanupPlan(ctx, db, []jobs.StatusCode{jobs.SUCCESS})
	expectedOrder := []string{"C", "P", "G"}
	if !reflect.DeepEqual(plan.removalOrder, expectedOrder) {
		t.Fatalf("unexpected removal order: got %v want %v", plan.removalOrder, expectedOrder)
	}
}
