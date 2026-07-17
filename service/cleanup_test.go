package service

import (
	"reflect"
	"testing"
	"time"

	"github.com/compgenlab/batchq/api"
	"github.com/compgenlab/batchq/jobs"
	"github.com/compgenlab/batchq/storage"
)

// --- planRemoval (ported from the old client-side buildCleanupPlan tests) ---

func TestPlanRemovalDependentsFirst(t *testing.T) {
	// C depends on P; both eligible. Child must be ordered before parent.
	order, blocked := planRemoval(
		[]string{"P", "C"},
		[]storage.CleanupDepEdge{{JobID: "C", AfterokID: "P"}},
	)
	if want := []string{"C", "P"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	if blocked != 0 {
		t.Fatalf("blocked = %d, want 0", blocked)
	}
}

func TestPlanRemovalBlocksWhenDependentNotEligible(t *testing.T) {
	// C depends on P, but C is not a candidate (e.g. still RUNNING). P must be
	// blocked so it isn't deleted while C still references it.
	order, blocked := planRemoval(
		[]string{"P"},
		[]storage.CleanupDepEdge{{JobID: "C", AfterokID: "P"}},
	)
	if len(order) != 0 {
		t.Fatalf("order = %v, want empty", order)
	}
	if blocked != 1 {
		t.Fatalf("blocked = %d, want 1", blocked)
	}
}

func TestPlanRemovalMultiLevelOrder(t *testing.T) {
	// G <- P <- C (C depends on P depends on G). Post-order: C, P, G.
	order, blocked := planRemoval(
		[]string{"G", "P", "C"},
		[]storage.CleanupDepEdge{
			{JobID: "P", AfterokID: "G"},
			{JobID: "C", AfterokID: "P"},
		},
	)
	if want := []string{"C", "P", "G"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	if blocked != 0 {
		t.Fatalf("blocked = %d, want 0", blocked)
	}
}

// --- CleanupBulk (end-to-end against real storage) ---

func TestCleanupBulkDeletes(t *testing.T) {
	svc, _ := newServiceWithArchives(t)
	ctx := ctxT(t)
	id := submitTerminal(t, svc, ctx, "j")

	res, err := svc.CleanupBulk(ctx, CleanupBulkOptions{Statuses: []jobs.StatusCode{jobs.SUCCESS}}, nil)
	if err != nil {
		t.Fatalf("CleanupBulk: %v", err)
	}
	if res.Removed != 1 {
		t.Fatalf("removed = %d, want 1", res.Removed)
	}
	if _, err := svc.GetJob(ctx, id, false); err != ErrJobNotFound {
		t.Fatalf("GetJob after delete: %v, want ErrJobNotFound", err)
	}
}

func TestCleanupBulkArchives(t *testing.T) {
	svc, _ := newServiceWithArchives(t)
	ctx := ctxT(t)
	id := submitTerminal(t, svc, ctx, "j")

	res, err := svc.CleanupBulk(ctx, CleanupBulkOptions{
		Statuses: []jobs.StatusCode{jobs.SUCCESS},
		Archive:  true,
		Vacuum:   true,
	}, nil)
	if err != nil {
		t.Fatalf("CleanupBulk: %v", err)
	}
	if res.Removed != 1 || res.ArchivePath == "" || !res.Vacuumed {
		t.Fatalf("res = %+v, want Removed=1, ArchivePath set, Vacuumed", res)
	}
	// Gone from live, present via the archive fallback.
	if _, err := svc.GetJob(ctx, id, false); err != ErrJobNotFound {
		t.Fatalf("live lookup: %v, want ErrJobNotFound", err)
	}
	if _, err := svc.GetJob(ctx, id, true); err != nil {
		t.Fatalf("archive fallback lookup: %v", err)
	}
}

func TestCleanupBulkOlderThanExcludesRecent(t *testing.T) {
	svc, _ := newServiceWithArchives(t)
	ctx := ctxT(t)
	id := submitTerminal(t, svc, ctx, "fresh")

	// The job just ended, so an age filter of 1h selects nothing.
	res, err := svc.CleanupBulk(ctx, CleanupBulkOptions{
		Statuses:  []jobs.StatusCode{jobs.SUCCESS},
		OlderThan: time.Hour,
	}, nil)
	if err != nil {
		t.Fatalf("CleanupBulk: %v", err)
	}
	if res.Removed != 0 {
		t.Fatalf("removed = %d, want 0 (job is younger than the cutoff)", res.Removed)
	}
	if _, err := svc.GetJob(ctx, id, false); err != nil {
		t.Fatalf("job should still be live: %v", err)
	}
}

// CleanupBulk streams progress events: candidate selection, per-batch delete
// progress, and the vacuum step.
func TestCleanupBulkEmitsProgress(t *testing.T) {
	svc, _ := newServiceWithArchives(t)
	ctx := ctxT(t)
	submitTerminal(t, svc, ctx, "a")
	submitTerminal(t, svc, ctx, "b")

	seen := map[string]api.CleanupEvent{}
	_, err := svc.CleanupBulk(ctx,
		CleanupBulkOptions{Statuses: []jobs.StatusCode{jobs.SUCCESS}, Vacuum: true},
		func(ev api.CleanupEvent) { seen[ev.Phase] = ev },
	)
	if err != nil {
		t.Fatalf("CleanupBulk: %v", err)
	}
	if ev, ok := seen[api.CleanupPhaseSelected]; !ok || ev.Matched != 2 || ev.Total != 2 {
		t.Fatalf("selected event = %+v (present=%v), want Matched=2 Total=2", seen[api.CleanupPhaseSelected], ok)
	}
	if ev, ok := seen[api.CleanupPhaseDeleting]; !ok || ev.Done != 2 {
		t.Fatalf("deleting event = %+v (present=%v), want Done=2", seen[api.CleanupPhaseDeleting], ok)
	}
	if _, ok := seen[api.CleanupPhaseVacuum]; !ok {
		t.Fatalf("no vacuum event; saw phases %v", seen)
	}
}

// A dependency-safe bulk delete removes a parent and its dependent together.
func TestCleanupBulkDeletesDependencyChain(t *testing.T) {
	svc, _ := newServiceWithArchives(t)
	ctx := ctxT(t)

	parent := submitTerminal(t, svc, ctx, "parent")
	// child depends on parent (afterok); drive it to terminal too.
	cdto, err := svc.SubmitJob(ctx, &api.SubmitJobRequest{
		Name:    "child",
		AfterOk: []string{parent},
		Details: map[string]string{"script": "echo child"},
	})
	if err != nil {
		t.Fatalf("SubmitJob child: %v", err)
	}
	if _, err := svc.ResolveDependencies(ctx); err != nil {
		t.Fatalf("ResolveDependencies: %v", err)
	}
	if _, err := svc.ClaimNextJob(ctx, "r", "simple", "", storage.Limits{}); err != nil {
		t.Fatalf("ClaimNextJob child: %v", err)
	}
	if err := svc.EndJob(ctx, "r", cdto.JobID, 0, ""); err != nil {
		t.Fatalf("EndJob child: %v", err)
	}

	res, err := svc.CleanupBulk(ctx, CleanupBulkOptions{Statuses: []jobs.StatusCode{jobs.SUCCESS}}, nil)
	if err != nil {
		t.Fatalf("CleanupBulk: %v", err)
	}
	if res.Removed != 2 || res.Blocked != 0 {
		t.Fatalf("res = %+v, want Removed=2 Blocked=0", res)
	}
}
