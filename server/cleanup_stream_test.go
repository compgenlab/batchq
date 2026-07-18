package server

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/compgenlab/batchq/api"
	"github.com/compgenlab/batchq/client"
	"github.com/compgenlab/batchq/service"
	"github.com/compgenlab/batchq/storage"
)

// A streaming cleanup must NOT be cut off by the server's WriteTimeout — the
// handler clears the write deadline for the stream. With a 1ms WriteTimeout the
// op always outlasts it, so without the fix the client would get "unexpected
// EOF"; with it, the stream completes cleanly.
func TestCleanupStreamSurvivesWriteTimeout(t *testing.T) {
	ctx := context.Background()
	st, err := storage.Open(ctx, filepath.Join(t.TempDir(), "batchq.db"), storage.Options{})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	svc := service.New(st)

	// Seed a handful of terminal (SUCCESS) jobs.
	const n = 5
	for i := 0; i < n; i++ {
		dto, err := svc.SubmitJob(ctx, &api.SubmitJobRequest{Details: map[string]string{"script": "echo hi"}})
		if err != nil {
			t.Fatalf("SubmitJob: %v", err)
		}
		if _, err := svc.ClaimNextJob(ctx, "r", "simple", "", storage.Limits{}); err != nil {
			t.Fatalf("ClaimNextJob: %v", err)
		}
		if err := svc.EndJob(ctx, "r", dto.JobID, 0, ""); err != nil {
			t.Fatalf("EndJob: %v", err)
		}
	}

	// Server with an aggressively short WriteTimeout — any real work outlasts it.
	srv, err := New(svc, Options{Listen: "tcp://127.0.0.1:0", WriteTimeout: time.Millisecond})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	srvCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(srvCtx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("Serve did not exit")
		}
	})

	var addr string
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if addr = srv.Addr(); addr != "" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if addr == "" {
		t.Fatal("server did not bind")
	}

	c, err := client.DialWithOptions(client.Options{URL: "tcp://" + addr, NoRequestTimeout: true})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	resp, err := c.Cleanup(ctx, api.CleanupRequest{Statuses: []string{"SUCCESS"}, Vacuum: true}, nil)
	if err != nil {
		t.Fatalf("Cleanup errored (WriteTimeout not cleared for the stream?): %v", err)
	}
	if resp.Removed != n {
		t.Fatalf("removed = %d, want %d", resp.Removed, n)
	}
	if !resp.Vacuumed {
		t.Fatalf("vacuum did not run")
	}
}
