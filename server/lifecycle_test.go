package server

// lifecycle_test.go covers:
//   - single-instance election via the unix socket (ErrAlreadyRunning)
//   - stale-socket recovery on bind
//   - idle-timeout self-shutdown (IdleTimeout, IdleCheckInterval)

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mbreese/batchq/internal/testsupport"
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

// A second server pointed at the same socket as a live first server
// must refuse to start. The socket itself is the election token: the
// second bind() returns EADDRINUSE, the probe finds the live server,
// and we surface ErrAlreadyRunning.
func TestSecondInstanceRejected(t *testing.T) {
	dir := testsupport.ShortSockDir(t)
	sock := filepath.Join(dir, "first.sock")

	svc := newSvcForLifecycle(t)

	first, err := New(svc, Options{Listen: "unix://" + sock})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() { done1 <- first.Serve(ctx1) }()
	waitForSocket(t, sock, 2*time.Second)
	defer func() {
		cancel1()
		<-done1
	}()

	second, err := New(svc, Options{Listen: "unix://" + sock})
	if err != nil {
		t.Fatalf("New second: %v", err)
	}
	if err := second.Serve(context.Background()); err != ErrAlreadyRunning {
		t.Fatalf("Serve second: got %v, want ErrAlreadyRunning", err)
	}
}

// After a clean shutdown the socket file is unlinked, so a fresh
// server on the same path must come up without complaint.
func TestSocketReclaimedAfterShutdown(t *testing.T) {
	dir := testsupport.ShortSockDir(t)
	sock := filepath.Join(dir, "reclaim.sock")

	svc := newSvcForLifecycle(t)

	first, err := New(svc, Options{Listen: "unix://" + sock})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() { done1 <- first.Serve(ctx1) }()
	waitForSocket(t, sock, 2*time.Second)
	cancel1()
	if err := <-done1; err != nil {
		t.Fatalf("first Serve: %v", err)
	}

	second, err := New(svc, Options{Listen: "unix://" + sock})
	if err != nil {
		t.Fatalf("New second: %v", err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() { done2 <- second.Serve(ctx2) }()
	waitForSocket(t, sock, 2*time.Second)
	cancel2()
	if err := <-done2; err != nil {
		t.Fatalf("second Serve: %v", err)
	}
}

// A socket file left behind by a crashed process (nothing listening on
// it) must be transparently recovered: bind() fails with EADDRINUSE,
// the probe fails too, we unlink and retry.
func TestStaleSocketRecovered(t *testing.T) {
	dir := testsupport.ShortSockDir(t)
	sock := filepath.Join(dir, "stale.sock")

	// Fabricate a stale socket: bind one, close the listener, leave the
	// inode on disk. Closing the net.Listener with a unix socket
	// normally unlinks the path, so we re-create the file as an empty
	// regular file to simulate the messier crash case where the path
	// outlives the process.
	if f, err := os.Create(sock); err != nil {
		t.Fatalf("create stale: %v", err)
	} else {
		_ = f.Close()
	}

	svc := newSvcForLifecycle(t)
	srv, err := New(svc, Options{Listen: "unix://" + sock})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	waitForSocket(t, sock, 2*time.Second)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Serve: %v", err)
	}
}

func TestIdleShutdownFires(t *testing.T) {
	dir := testsupport.ShortSockDir(t)
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
	dir := testsupport.ShortSockDir(t)
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
