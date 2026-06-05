package cmd

import (
	"os"
	"testing"
)

// TestIsDirectoryEmpty guards the PR #30 panic fix: isDirectory("") must return
// an error instead of indexing path[len(path)-1] on an empty string.
func TestIsDirectoryEmpty(t *testing.T) {
	ok, err := isDirectory("")
	if err == nil {
		t.Fatal("isDirectory(\"\"): expected error, got nil")
	}
	if ok {
		t.Fatal("isDirectory(\"\"): expected false")
	}
}

func TestIsDirectory(t *testing.T) {
	dir := t.TempDir()
	ok, err := isDirectory(dir)
	if err != nil || !ok {
		t.Fatalf("isDirectory(%q) = (%v, %v), want (true, nil)", dir, ok, err)
	}

	// A non-existent path ending in a separator is assumed to be a directory.
	ok, err = isDirectory(dir + string(os.PathSeparator) + "missing" + string(os.PathSeparator))
	if err != nil || !ok {
		t.Fatalf("isDirectory(trailing-sep) = (%v, %v), want (true, nil)", ok, err)
	}
}
