package cmd

import (
	"context"
	"reflect"
	"testing"

	"github.com/mbreese/batchq/api"
	"github.com/mbreese/batchq/jobs"
)

type fakeCleanupDB struct {
	jobs       map[string]*api.JobDTO
	dependents map[string][]string
	order      []string
}

func (f *fakeCleanupDB) GetJobsByStatus(ctx context.Context, statuses []jobs.StatusCode, sortByStatus bool) []*api.JobDTO {
	allowed := map[string]bool{}
	for _, status := range statuses {
		allowed[status.String()] = true
	}

	var out []*api.JobDTO
	for _, id := range f.order {
		job := f.jobs[id]
		if job != nil && allowed[job.Status] {
			out = append(out, job)
		}
	}
	return out
}

func (f *fakeCleanupDB) GetJob(ctx context.Context, jobId string) *api.JobDTO {
	return f.jobs[jobId]
}

func (f *fakeCleanupDB) GetJobDependents(ctx context.Context, jobId string) ([]string, error) {
	return f.dependents[jobId], nil
}

func TestBuildCleanupPlanRemovesDependentsFirst(t *testing.T) {
	ctx := context.Background()
	db := &fakeCleanupDB{
		jobs: map[string]*api.JobDTO{
			"P": {JobID: "P", Status: jobs.SUCCESS.String()},
			"C": {JobID: "C", Status: jobs.SUCCESS.String()},
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
		jobs: map[string]*api.JobDTO{
			"P": {JobID: "P", Status: jobs.SUCCESS.String()},
			"C": {JobID: "C", Status: jobs.RUNNING.String()},
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
		jobs: map[string]*api.JobDTO{
			"G": {JobID: "G", Status: jobs.SUCCESS.String()},
			"P": {JobID: "P", Status: jobs.SUCCESS.String()},
			"C": {JobID: "C", Status: jobs.SUCCESS.String()},
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
