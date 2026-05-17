package server_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mbreese/batchq/server"
	"github.com/mbreese/batchq/service"
	"github.com/mbreese/batchq/storage"
)

func TestOwnershipMonitor_HealthyServerStaysUp(t *testing.T) {
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "batchq.sock")
	dbPath := filepath.Join(dir, "batchq.db")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, err := storage.Open(ctx, dbPath, storage.Options{})
	if err != nil { t.Fatal(err) }
	defer store.Close()

	srv, err := server.New(service.New(store), server.Options{
		Listen:                 "unix://" + sockPath,
		OwnershipCheckInterval: 500 * time.Millisecond,
	})
	if err != nil { t.Fatal(err) }

	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	time.Sleep(200 * time.Millisecond)
	if _, err := os.Stat(sockPath); err != nil { t.Fatal(err) }

	// Run for 2 seconds — monitor should tick ~4 times without false positives.
	time.Sleep(2 * time.Second)
	if _, err := os.Stat(sockPath); err != nil {
		t.Fatalf("server died unexpectedly: %v", err)
	}
	fmt.Println("healthy server survived 2s of ownership checks")
	cancel()
	<-done
}

// TestOwnershipMonitor_DoesNotPreventIdleShutdown guards against a
// regression where the monitor's own self-dial bumps lastActivity via
// the withActivity middleware, preventing an idle server from ever
// shutting down. The fix is HeaderInternalOwner; this test would fail
// without it.
func TestOwnershipMonitor_DoesNotPreventIdleShutdown(t *testing.T) {
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "batchq.sock")
	dbPath := filepath.Join(dir, "batchq.db")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, err := storage.Open(ctx, dbPath, storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	srv, err := server.New(service.New(store), server.Options{
		Listen:                 "unix://" + sockPath,
		IdleTimeout:            500 * time.Millisecond,
		IdleCheckInterval:      100 * time.Millisecond,
		OwnershipCheckInterval: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned: %v", err)
		}
		fmt.Println("server idled out despite monitor pings")
	case <-time.After(3 * time.Second):
		t.Fatal("idle shutdown never fired (ownership pings must be bumping activity)")
	}
}

func TestOwnershipMonitor_OrphanShutsDown(t *testing.T) {
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "batchq.sock")
	dbPath := filepath.Join(dir, "batchq.db")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, err := storage.Open(ctx, dbPath, storage.Options{})
	if err != nil { t.Fatal(err) }
	defer store.Close()

	srv, err := server.New(service.New(store), server.Options{
		Listen:                 "unix://" + sockPath,
		OwnershipCheckInterval: 200 * time.Millisecond,
	})
	if err != nil { t.Fatal(err) }
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	time.Sleep(200 * time.Millisecond)

	// Detach our socket.
	if err := os.Remove(sockPath); err != nil { t.Fatal(err) }
	fmt.Println("unlinked socket, waiting for monitor to detect orphan")

	select {
	case err := <-done:
		if err != nil { t.Fatalf("Serve returned: %v", err) }
		fmt.Println("server shut down as expected")
	case <-time.After(3 * time.Second):
		t.Fatal("server did not shut down after socket unlinked within 3s")
	}
}
