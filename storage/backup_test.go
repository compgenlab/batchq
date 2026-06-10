package storage

import (
	"context"
	"path/filepath"
	"testing"
)

// Backup writes a consistent, queryable snapshot: every job in the source DB
// is present in the copy and the copy passes integrity_check.
func TestBackupSnapshotIsConsistent(t *testing.T) {
	src := newTestStore(t)
	ctx := ctxT(t)

	for _, id := range []string{"b-1", "b-2", "b-3"} {
		if err := src.InsertJob(ctx, mkJob(id, map[string]string{"script": "echo " + id})); err != nil {
			t.Fatalf("InsertJob %s: %v", id, err)
		}
	}

	dest := filepath.Join(t.TempDir(), "snap.db")
	if err := src.Backup(ctx, dest); err != nil {
		t.Fatalf("Backup: %v", err)
	}

	// Open the snapshot as a fresh store and confirm all jobs are present.
	snap, err := Open(context.Background(), dest, Options{})
	if err != nil {
		t.Fatalf("open snapshot: %v", err)
	}
	defer snap.Close()

	jobs, err := snap.ListJobs(ctx, true, false)
	if err != nil {
		t.Fatalf("ListJobs on snapshot: %v", err)
	}
	if len(jobs) != 3 {
		t.Fatalf("snapshot has %d jobs, want 3", len(jobs))
	}

	// The copy must pass SQLite's own integrity check.
	var result string
	if err := snap.(*sqliteStorage).readDB.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&result); err != nil {
		t.Fatalf("integrity_check: %v", err)
	}
	if result != "ok" {
		t.Fatalf("integrity_check = %q, want \"ok\"", result)
	}
}

// VACUUM INTO refuses a destination that already exists, so the storage-layer
// Backup surfaces an error rather than silently overwriting.
func TestBackupRefusesExistingDestination(t *testing.T) {
	src := newTestStore(t)
	ctx := ctxT(t)

	dest := filepath.Join(t.TempDir(), "snap.db")
	if err := src.Backup(ctx, dest); err != nil {
		t.Fatalf("first Backup: %v", err)
	}
	if err := src.Backup(ctx, dest); err == nil {
		t.Fatal("second Backup to an existing path should error, got nil")
	}
}
