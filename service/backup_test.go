package service

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// With no destination, Backup writes a snapshot under $BATCHQ_HOME/backups/
// and returns the absolute path it chose.
func TestBackupDefaultDestination(t *testing.T) {
	home := t.TempDir()
	t.Setenv("BATCHQ_HOME", home)

	svc := newService(t)
	ctx := ctxT(t)

	path, err := svc.Backup(ctx, "")
	if err != nil {
		t.Fatalf("Backup: %v", err)
	}
	wantDir := filepath.Join(home, "backups")
	if filepath.Dir(path) != wantDir {
		t.Fatalf("default backup dir = %q, want %q", filepath.Dir(path), wantDir)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("snapshot not written: %v", err)
	}
}

// An explicit destination is honored and returned absolute; a pre-existing
// destination is rejected as a bad request (not a raw SQLite error).
func TestBackupExplicitAndExistingDestination(t *testing.T) {
	svc := newService(t)
	ctx := ctxT(t)

	dest := filepath.Join(t.TempDir(), "snap.db")
	got, err := svc.Backup(ctx, dest)
	if err != nil {
		t.Fatalf("Backup: %v", err)
	}
	if got != dest {
		t.Fatalf("returned path = %q, want %q", got, dest)
	}

	_, err = svc.Backup(ctx, dest)
	if err == nil {
		t.Fatal("backup to existing destination should error, got nil")
	}
	if !errors.Is(err, ErrBadRequest) {
		t.Fatalf("error = %v, want ErrBadRequest", err)
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("error %q should mention the destination already exists", err)
	}
}
