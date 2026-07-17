package service

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/compgenlab/batchq/api"
	"github.com/compgenlab/batchq/storage"
)

// newServiceWithArchives is newService plus a configured archive dir, so the
// archive move and fallback lookup can be exercised end-to-end.
func newServiceWithArchives(t *testing.T) (*Service, string) {
	t.Helper()
	home := t.TempDir()
	st, err := storage.Open(context.Background(), filepath.Join(home, "batchq.db"), storage.Options{})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	archiveDir := filepath.Join(home, "archives")
	return New(st, WithArchiveDir(archiveDir)), archiveDir
}

// submitTerminal submits a job and drives it to SUCCESS so it can be archived.
func submitTerminal(t *testing.T, svc *Service, ctx context.Context, name string) string {
	t.Helper()
	dto, err := svc.SubmitJob(ctx, &api.SubmitJobRequest{
		Name:    name,
		Details: map[string]string{"script": "echo " + name},
	})
	if err != nil {
		t.Fatalf("SubmitJob %s: %v", name, err)
	}
	if _, err := svc.ClaimNextJob(ctx, "r", "simple", "", storage.Limits{}); err != nil {
		t.Fatalf("ClaimNextJob %s: %v", name, err)
	}
	if err := svc.EndJob(ctx, "r", dto.JobID, 0, ""); err != nil {
		t.Fatalf("EndJob %s: %v", name, err)
	}
	return dto.JobID
}

// ArchiveJobs must move a terminal job out of the live DB into a new read-only
// archive DB, preserving its terminal status, and the fallback lookup must then
// find it only when includeArchives is set.
func TestArchiveJobsMovesAndFallbackFinds(t *testing.T) {
	svc, archiveDir := newServiceWithArchives(t)
	ctx := ctxT(t)

	id := submitTerminal(t, svc, ctx, "arch-me")

	path, count, err := svc.ArchiveJobs(ctx, []string{id}, "")
	if err != nil {
		t.Fatalf("ArchiveJobs: %v", err)
	}
	if count != 1 {
		t.Fatalf("archived count = %d, want 1", count)
	}
	if filepath.Dir(path) != archiveDir {
		t.Fatalf("archive path %q not under %q", path, archiveDir)
	}

	// Gone from the live DB.
	if _, err := svc.GetJob(ctx, id, false); err != ErrJobNotFound {
		t.Fatalf("live GetJob after archive: got %v, want ErrJobNotFound", err)
	}
	// Present via the archive fallback, with terminal status preserved.
	dto, err := svc.GetJob(ctx, id, true)
	if err != nil {
		t.Fatalf("GetJob --archives: %v", err)
	}
	if dto.Status != "SUCCESS" {
		t.Fatalf("archived status = %q, want SUCCESS", dto.Status)
	}

	// The archive file is read-only (write-once).
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat archive: %v", err)
	}
	if perm := info.Mode().Perm(); perm&0o200 != 0 {
		t.Fatalf("archive perm = %o, want no write bits (read-only)", perm)
	}
}

// A search with IncludeArchives must union live and archived matches.
func TestListJobsIncludeArchivesUnions(t *testing.T) {
	svc, _ := newServiceWithArchives(t)
	ctx := ctxT(t)

	archived := submitTerminal(t, svc, ctx, "gone")
	if _, _, err := svc.ArchiveJobs(ctx, []string{archived}, ""); err != nil {
		t.Fatalf("ArchiveJobs: %v", err)
	}
	live := submitTerminal(t, svc, ctx, "here")

	// Without archives: only the live job is searchable.
	got, err := svc.ListJobs(ctx, ListJobsOptions{ShowAll: true})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if ids := idSet(got); ids[archived] || !ids[live] {
		t.Fatalf("live-only listing = %v, want {%s} not {%s}", ids, live, archived)
	}

	// With archives: both appear.
	got, err = svc.ListJobs(ctx, ListJobsOptions{ShowAll: true, IncludeArchives: true})
	if err != nil {
		t.Fatalf("ListJobs --archives: %v", err)
	}
	if ids := idSet(got); !ids[archived] || !ids[live] {
		t.Fatalf("archive-union listing missing a job: got %v, want both %s and %s", ids, archived, live)
	}
}

func idSet(dtos []*api.JobDTO) map[string]bool {
	out := make(map[string]bool, len(dtos))
	for _, d := range dtos {
		out[d.JobID] = true
	}
	return out
}

// ArchiveJobs must refuse to overwrite an existing archive (write-once).
func TestArchiveJobsRejectsExistingName(t *testing.T) {
	svc, _ := newServiceWithArchives(t)
	ctx := ctxT(t)

	a := submitTerminal(t, svc, ctx, "a")
	if _, _, err := svc.ArchiveJobs(ctx, []string{a}, "dup"); err != nil {
		t.Fatalf("first ArchiveJobs: %v", err)
	}
	b := submitTerminal(t, svc, ctx, "b")
	if _, _, err := svc.ArchiveJobs(ctx, []string{b}, "dup"); err == nil {
		t.Fatal("second ArchiveJobs into same name: want error, got nil")
	}
}
