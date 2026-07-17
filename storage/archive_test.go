package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/compgenlab/batchq/jobs"
)

// RestoreJob into an archive writer preserves the exact terminal fields (unlike
// InsertJob, which derives status), and OpenReadOnly can then read the archive
// even after it's chmod'd read-only.
func TestRestoreJobPreservesTerminalFieldsAndReadOnly(t *testing.T) {
	ctx := ctxT(t)
	path := filepath.Join(t.TempDir(), "arc.db")

	arc, err := OpenArchiveWriter(ctx, path)
	if err != nil {
		t.Fatalf("OpenArchiveWriter: %v", err)
	}

	job := mkJob("j-1", map[string]string{"script": "echo hi", "procs": "2"})
	job.Status = jobs.SUCCESS
	job.ReturnCode = 7
	job.Details = append(job.Details, jobs.JobDefDetail{Key: "extra", Value: "v"})
	if err := arc.RestoreJob(ctx, job); err != nil {
		t.Fatalf("RestoreJob: %v", err)
	}
	// Idempotent — restoring again must not error (INSERT OR IGNORE).
	if err := arc.RestoreJob(ctx, job); err != nil {
		t.Fatalf("RestoreJob (idempotent): %v", err)
	}
	_ = arc.Close()

	// Enforce read-only like the service does, then read it back.
	if err := os.Chmod(path, 0o440); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	ro, err := OpenReadOnly(ctx, path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer ro.Close()

	got, err := ro.GetJob(ctx, "j-1")
	if err != nil {
		t.Fatalf("GetJob from read-only archive: %v", err)
	}
	if got.Status != jobs.SUCCESS {
		t.Errorf("status = %v, want SUCCESS", got.Status)
	}
	if got.ReturnCode != 7 {
		t.Errorf("return_code = %d, want 7", got.ReturnCode)
	}
	if got.GetDetail("extra", "") != "v" {
		t.Errorf("detail extra = %q, want v", got.GetDetail("extra", ""))
	}
}

// RestoreJob tolerates a dependency edge whose parent isn't in the archive
// (foreign keys are OFF on the archive writer) — an archive is a historical
// dump, not a referentially-complete queue.
func TestRestoreJobToleratesDanglingDep(t *testing.T) {
	ctx := ctxT(t)
	path := filepath.Join(t.TempDir(), "arc.db")
	arc, err := OpenArchiveWriter(ctx, path)
	if err != nil {
		t.Fatalf("OpenArchiveWriter: %v", err)
	}
	defer arc.Close()

	// child references a parent that is never archived.
	child := mkJob("child", map[string]string{"script": "x"}, "missing-parent")
	child.Status = jobs.SUCCESS
	if err := arc.RestoreJob(ctx, child); err != nil {
		t.Fatalf("RestoreJob with dangling dep: %v", err)
	}
	got, err := arc.GetJob(ctx, "child")
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if len(got.AfterOk) != 1 || got.AfterOk[0] != "missing-parent" {
		t.Errorf("afterok = %v, want [missing-parent]", got.AfterOk)
	}
}

// Vacuum runs without error on a live store.
func TestVacuum(t *testing.T) {
	st := newTestStore(t)
	ctx := ctxT(t)
	if err := st.InsertJob(ctx, mkJob("v-1", map[string]string{"script": "x"})); err != nil {
		t.Fatalf("InsertJob: %v", err)
	}
	if err := st.Vacuum(ctx); err != nil {
		t.Fatalf("Vacuum: %v", err)
	}
	if _, err := st.GetJob(ctx, "v-1"); err != nil {
		t.Fatalf("GetJob after vacuum: %v", err)
	}
}

// OpenReadOnly on a missing file is an error, not a silent empty DB.
func TestOpenReadOnlyMissingFile(t *testing.T) {
	ctx := ctxT(t)
	if _, err := OpenReadOnly(ctx, filepath.Join(t.TempDir(), "nope.db")); err == nil {
		t.Fatal("OpenReadOnly on missing file: want error, got nil")
	}
}
