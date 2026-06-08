package server

// election_test.go covers the SQLITE_BUSY fix: the single-instance election
// (Elect) must run before any DB is opened, and on shutdown the DB must be
// released (OnShutdown) before the socket (election token) is unlinked.

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/mbreese/batchq/internal/testsupport"
)

// TestConcurrentElectSingleWinner proves that N processes racing to start a
// server against the same socket produce exactly one election winner, and every
// loser gets ErrAlreadyRunning — never some other error that would let it fall
// through and open the DB (the SQLITE_BUSY root cause). Elect touches no
// storage, mirroring the new cmd/server.go ordering where losers exit before
// storage.Open.
func TestConcurrentElectSingleWinner(t *testing.T) {
	dir := testsupport.ShortSockDir(t)
	sock := filepath.Join(dir, "race.sock")
	listen := "unix://" + sock

	const n = 8
	var wg sync.WaitGroup
	results := make([]error, n)
	listeners := make([]net.Listener, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ln, _, err := Elect(listen, 0)
			results[i] = err
			listeners[i] = ln
		}(i)
	}
	close(start) // release all goroutines as simultaneously as possible
	wg.Wait()

	wins := 0
	for i, err := range results {
		switch {
		case err == nil:
			wins++
			_ = listeners[i].Close()
		case errors.Is(err, ErrAlreadyRunning):
			// expected loser
		default:
			t.Fatalf("goroutine %d: unexpected error %v (a non-ErrAlreadyRunning error would let a loser proceed to open the DB)", i, err)
		}
	}
	if wins != 1 {
		t.Fatalf("expected exactly 1 election winner, got %d", wins)
	}
	_ = os.Remove(sock)
}
