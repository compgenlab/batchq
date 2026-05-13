package server

// lifecycle_test.go covers the Phase 8 additions to the server:
//   - flock-based single-instance election (LockPath, ErrLockHeld)
//   - idle-timeout self-shutdown (IdleTimeout, IdleCheckInterval)

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/mbreese/batchq/service"
	"github.com/mbreese/batchq/storage"
)

func newSvcForLifecycle(t *testing.T) *service.Service {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "lifecycle.db")
	st, err := storage.Open(context.Background(), dbPath, storage.Options{})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return service.New(st)
}

func waitForSocket(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c, err := net.Dial("unix", path); err == nil {
			_ = c.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("socket %s never became reachable", path)
}

func TestLockFileBlocksSecondInstance(t *testing.T) {
	dir := t.TempDir()
	lock := filepath.Join(dir, "server.lock")
	sock1 := filepath.Join(dir, "s1.sock")
	sock2 := filepath.Join(dir, "s2.sock")

	svc := newSvcForLifecycle(t)

	first, err := New(svc, Options{Listen: "unix://" + sock1, LockPath: lock})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() { done1 <- first.Serve(ctx1) }()
	waitForSocket(t, sock1, 2*time.Second)
	defer func() {
		cancel1()
		<-done1
	}()

	// Second instance with the same lock file must refuse to start.
	second, err := New(svc, Options{Listen: "unix://" + sock2, LockPath: lock})
	if err != nil {
		t.Fatalf("New second: %v", err)
	}
	err = second.Serve(context.Background())
	if err != ErrLockHeld {
		t.Fatalf("Serve second: got %v, want ErrLockHeld", err)
	}
}

func TestLockReleasedAfterShutdown(t *testing.T) {
	dir := t.TempDir()
	lock := filepath.Join(dir, "server.lock")
	sock1 := filepath.Join(dir, "s1.sock")
	sock2 := filepath.Join(dir, "s2.sock")

	svc := newSvcForLifecycle(t)

	// Start, then stop, the first instance.
	first, err := New(svc, Options{Listen: "unix://" + sock1, LockPath: lock})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() { done1 <- first.Serve(ctx1) }()
	waitForSocket(t, sock1, 2*time.Second)
	cancel1()
	if err := <-done1; err != nil {
		t.Fatalf("first Serve: %v", err)
	}

	// Now the second instance must be able to take the lock cleanly.
	second, err := New(svc, Options{Listen: "unix://" + sock2, LockPath: lock})
	if err != nil {
		t.Fatalf("New second: %v", err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() { done2 <- second.Serve(ctx2) }()
	waitForSocket(t, sock2, 2*time.Second)
	cancel2()
	if err := <-done2; err != nil {
		t.Fatalf("second Serve: %v", err)
	}
}

func TestIdleShutdownFires(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "idle.sock")
	svc := newSvcForLifecycle(t)

	fired := make(chan struct{}, 1)
	srv, err := New(svc, Options{
		Listen:            "unix://" + sock,
		IdleTimeout:       150 * time.Millisecond,
		IdleCheckInterval: 30 * time.Millisecond,
		OnIdleShutdown:    func() { fired <- struct{}{} },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(context.Background()) }()
	waitForSocket(t, sock, 2*time.Second)

	select {
	case <-fired:
	case <-time.After(3 * time.Second):
		t.Fatal("idle shutdown never fired")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not return after idle shutdown")
	}
}

func TestIdleShutdownResetsOnRequest(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "active.sock")
	svc := newSvcForLifecycle(t)

	srv, err := New(svc, Options{
		Listen:            "unix://" + sock,
		IdleTimeout:       300 * time.Millisecond,
		IdleCheckInterval: 30 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(context.Background()) }()
	waitForSocket(t, sock, 2*time.Second)

	// Make a request every 80ms for 600ms. The server must not shut down
	// during that window even though IdleTimeout is 300ms.
	deadline := time.Now().Add(600 * time.Millisecond)
	for time.Now().Before(deadline) {
		c, err := net.Dial("unix", sock)
		if err != nil {
			t.Fatalf("dial during activity loop: %v", err)
		}
		// Send a minimal HTTP request so the activity middleware records it.
		if _, err := c.Write([]byte("GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n")); err != nil {
			t.Fatalf("write: %v", err)
		}
		buf := make([]byte, 256)
		_, _ = c.Read(buf)
		_ = c.Close()
		time.Sleep(80 * time.Millisecond)
	}

	// Now stop poking and confirm idle shutdown fires shortly after.
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after activity stopped")
	}
}
