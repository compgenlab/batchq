package server

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/mbreese/batchq/api"
	"github.com/mbreese/batchq/client"
	"github.com/mbreese/batchq/service"
	"github.com/mbreese/batchq/storage"
)

// startTCPServer brings up a real server on an OS-assigned TCP port and
// returns its bound address.
func startTCPServer(t *testing.T) string {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "batchq.db")
	st, err := storage.Open(context.Background(), dbPath, storage.Options{})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	srv, err := New(service.New(st), Options{Listen: "tcp://127.0.0.1:0"})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("Serve did not exit after cancel")
		}
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if addr := srv.Addr(); addr != "" {
			return addr
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("server did not bind a TCP address")
	return ""
}

// TestTCPEndToEnd exercises the full path: server binds a TCP port, a REST
// client dials it over tcp://, and a real request round-trips.
func TestTCPEndToEnd(t *testing.T) {
	addr := startTCPServer(t)

	c, err := client.DialWithOptions(client.Options{URL: "tcp://" + addr, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("client.Dial tcp: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	ctx := context.Background()
	if err := c.Health(ctx); err != nil {
		t.Fatalf("health over tcp: %v", err)
	}
	dto, err := c.SubmitJob(ctx, &api.SubmitJobRequest{
		Details: map[string]string{"script": "echo hi"},
	})
	if err != nil {
		t.Fatalf("submit over tcp: %v", err)
	}
	if dto == nil || dto.JobID == "" {
		t.Fatalf("submit returned no job id: %+v", dto)
	}
}

// TestListenTCPAddrInUse confirms a port already in use surfaces as
// ErrAlreadyRunning rather than a generic bind error.
func TestListenTCPAddrInUse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("hold port: %v", err)
	}
	defer ln.Close()

	srv := &Server{opts: Options{Listen: "tcp://" + ln.Addr().String()}}
	if _, err := srv.listen(); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("listen on in-use port: got %v, want ErrAlreadyRunning", err)
	}
}
