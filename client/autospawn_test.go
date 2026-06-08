package client

// autospawn_test.go exercises the contract logic in DialAndConnect:
//   - happy path when a server is already running
//   - spawn path when nothing is on the socket
//   - stale-socket cleanup before spawn
//   - autospawn disabled → surface the connect error
//
// The fork-exec mechanics of spawnServer (Setsid, /dev/null stdio,
// child reaping) are covered indirectly by `test.sh` end-to-end runs;
// here we plug a custom SpawnFunc that stands up an in-process server.

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mbreese/batchq/internal/testsupport"
	"github.com/mbreese/batchq/server"
	"github.com/mbreese/batchq/service"
	"github.com/mbreese/batchq/storage"
)

// spawnInProcess returns a SpawnFunc that starts a real batchq server
// listening on the requested socket. Cleanup is wired into t so the
// goroutine doesn't leak.
func spawnInProcess(t *testing.T) (func(string) error, *atomic.Int32) {
	t.Helper()
	var spawnCount atomic.Int32
	return func(socketPath string) error {
		spawnCount.Add(1)
		dbPath := filepath.Join(filepath.Dir(socketPath), "spawned.db")
		st, err := storage.Open(context.Background(), dbPath, storage.Options{})
		if err != nil {
			return err
		}
		svc := service.New(st)
		srv, err := server.New(svc, server.Options{Listen: "unix://" + socketPath})
		if err != nil {
			_ = st.Close()
			return err
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			_ = srv.Serve(ctx)
			_ = st.Close()
			close(done)
		}()
		t.Cleanup(func() {
			cancel()
			<-done
		})
		return nil
	}, &spawnCount
}

func TestAutospawnHappyPathServerAlreadyUp(t *testing.T) {
	dir := testsupport.ShortSockDir(t)
	sock := filepath.Join(dir, "ready.sock")
	dbPath := filepath.Join(dir, "ready.db")

	st, err := storage.Open(context.Background(), dbPath, storage.Options{})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	svc := service.New(st)
	srv, err := server.New(svc, server.Options{Listen: "unix://" + sock})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = srv.Serve(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	// Wait briefly for bind.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.Dial("unix", sock); err == nil {
			_ = c.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	spawn, spawnCount := spawnInProcess(t)
	c, err := DialAndConnect(context.Background(),
		Options{URL: "unix://" + sock, Timeout: 2 * time.Second},
		AutospawnConfig{Enabled: true, SpawnFunc: spawn, PollTimeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("DialAndConnect: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if spawnCount.Load() != 0 {
		t.Fatalf("spawned %d times, want 0 (server was already up)", spawnCount.Load())
	}
}

func TestAutospawnSpawnsWhenNoServer(t *testing.T) {
	dir := testsupport.ShortSockDir(t)
	sock := filepath.Join(dir, "spawned.sock")

	spawn, spawnCount := spawnInProcess(t)
	c, err := DialAndConnect(context.Background(),
		Options{URL: "unix://" + sock, Timeout: 2 * time.Second},
		AutospawnConfig{Enabled: true, SpawnFunc: spawn, PollTimeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("DialAndConnect: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if spawnCount.Load() != 1 {
		t.Fatalf("spawned %d times, want 1", spawnCount.Load())
	}
	if err := c.Health(context.Background()); err != nil {
		t.Fatalf("Health after spawn: %v", err)
	}
}

// TestAutospawnPrefersConnectingToSlowServer guards the SQLITE_BUSY fix: when a
// live server is briefly unreachable at the first probe (here it binds a moment
// later), DialAndConnect must retry and CONNECT to it rather than spawn a
// duplicate (which would open the same DB and cause SQLITE_BUSY). spawnCount
// must stay 0.
func TestAutospawnPrefersConnectingToSlowServer(t *testing.T) {
	dir := testsupport.ShortSockDir(t)
	sock := filepath.Join(dir, "slow.sock")
	dbPath := filepath.Join(dir, "slow.db")

	st, err := storage.Open(context.Background(), dbPath, storage.Options{})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv, err := server.New(service.New(st), server.Options{Listen: "unix://" + sock})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	// Bring the server up shortly AFTER DialAndConnect's first probe fails, but
	// well within its connect-retry window (~3×300ms).
	go func() {
		time.Sleep(400 * time.Millisecond)
		_ = srv.Serve(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	spawn, spawnCount := spawnInProcess(t)
	c, err := DialAndConnect(context.Background(),
		Options{URL: "unix://" + sock, Timeout: 2 * time.Second},
		AutospawnConfig{Enabled: true, SpawnFunc: spawn, PollTimeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("DialAndConnect: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if spawnCount.Load() != 0 {
		t.Fatalf("spawned %d times, want 0 (should have connected to the slow-to-start server, not duplicated it)", spawnCount.Load())
	}
}

func TestAutospawnRemovesStaleSocket(t *testing.T) {
	dir := testsupport.ShortSockDir(t)
	sock := filepath.Join(dir, "stale.sock")

	// Create a regular file at the socket path. A net.Dial against this
	// will fail with "connection refused" (or similar), so autospawn
	// should treat it as stale and remove it before spawning.
	if err := os.WriteFile(sock, []byte("stale"), 0o600); err != nil {
		t.Fatalf("write stale: %v", err)
	}

	spawn, spawnCount := spawnInProcess(t)
	c, err := DialAndConnect(context.Background(),
		Options{URL: "unix://" + sock, Timeout: 2 * time.Second},
		AutospawnConfig{Enabled: true, SpawnFunc: spawn, PollTimeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("DialAndConnect: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if spawnCount.Load() != 1 {
		t.Fatalf("spawned %d times, want 1", spawnCount.Load())
	}
}

func TestAutospawnDisabledReturnsConnectError(t *testing.T) {
	dir := testsupport.ShortSockDir(t)
	sock := filepath.Join(dir, "nobody.sock")

	_, err := DialAndConnect(context.Background(),
		Options{URL: "unix://" + sock, Timeout: 200 * time.Millisecond},
		AutospawnConfig{Enabled: false})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !isConnectFailure(err) {
		t.Fatalf("expected connect failure, got: %v", err)
	}
}

func TestAutospawnContextCancelStopsPolling(t *testing.T) {
	dir := testsupport.ShortSockDir(t)
	sock := filepath.Join(dir, "never.sock")

	// SpawnFunc that "succeeds" but never starts a server, so the
	// caller will poll until ctx is canceled.
	spawn := func(string) error { return nil }
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err := DialAndConnect(ctx,
		Options{URL: "unix://" + sock, Timeout: 100 * time.Millisecond},
		AutospawnConfig{
			Enabled:      true,
			SpawnFunc:    spawn,
			PollTimeout:  5 * time.Second,
			PollInterval: 20 * time.Millisecond,
		})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got: %v", err)
	}
}
