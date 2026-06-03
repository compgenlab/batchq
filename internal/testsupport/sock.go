// Package testsupport provides small helpers used only by *_test.go files
// across this module. It lives under internal/ so it stays unreachable to
// downstream importers.
package testsupport

import (
	"os"
	"testing"
)

// ShortSockDir returns a fresh directory whose path is short enough that
// unix sockets created inside it fit macOS's 104-character sun_path limit.
// The directory is removed at test end.
//
// t.TempDir() on macOS expands to /var/folders/<hash>/T/<TestName>_<rand>/NNN/
// which is already ~90 characters before any filename is appended; a long
// test name reliably pushes the resulting "<dir>/foo.sock" past the limit
// and causes bind()/connect() to fail with "invalid argument" on darwin.
// Rooting under /tmp instead keeps the prefix small (4 chars vs ~50) so
// no test name we'd reasonably write blows the budget. /tmp exists on
// every supported platform (linux/darwin), so there's no portability
// regression vs t.TempDir() for the (already unix-socket-only) use cases.
func ShortSockDir(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("/tmp", "bq-")
	if err != nil {
		t.Fatalf("ShortSockDir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(d) })
	return d
}
