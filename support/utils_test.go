package support

import (
	"os"
	"path/filepath"
	"testing"
)

// TestExpandPathAbsEmpty guards the regression where ExpandPathAbs("")
// panicked on path[len(path)-1]. An empty path must return empty, no error.
func TestExpandPathAbsEmpty(t *testing.T) {
	got, err := ExpandPathAbs("")
	if err != nil {
		t.Fatalf("ExpandPathAbs(\"\"): unexpected error %v", err)
	}
	if got != "" {
		t.Fatalf("ExpandPathAbs(\"\") = %q, want \"\"", got)
	}
}

// TestFileExists covers the PR #30 contract: FileExists is true only when the
// path is statable, and a non-"not found" stat error (e.g. permission denied)
// reports false rather than masquerading as existence.
func TestFileExists(t *testing.T) {
	dir := t.TempDir()

	present := filepath.Join(dir, "present")
	if err := os.WriteFile(present, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	if !FileExists(present) {
		t.Errorf("FileExists(%q) = false, want true", present)
	}

	absent := filepath.Join(dir, "absent")
	if FileExists(absent) {
		t.Errorf("FileExists(%q) = true, want false", absent)
	}

	// Permission-denied case: a file inside a 0000 directory can't be statted.
	// Root bypasses directory permissions, so skip there.
	if AmIRoot() {
		t.Skip("running as root; directory permissions are bypassed")
	}
	locked := filepath.Join(dir, "locked")
	if err := os.Mkdir(locked, 0755); err != nil {
		t.Fatal(err)
	}
	hidden := filepath.Join(locked, "file")
	if err := os.WriteFile(hidden, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(locked, 0000); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(locked, 0755) // restore so t.TempDir cleanup can remove it
	if FileExists(hidden) {
		t.Errorf("FileExists(%q) = true on permission error, want false", hidden)
	}
}
