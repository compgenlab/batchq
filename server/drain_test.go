package server

// drain_test.go covers the draining admission gate: while a server is shutting
// down it must reject new requests with 503 + HeaderDraining (so the client can
// reconnect and retry) WITHOUT leaving inFlight incremented or touching the DB.

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mbreese/batchq/api"
)

func TestDrainingGateRejectsWithRetryableSignal(t *testing.T) {
	svc := newSvcForLifecycle(t)
	srv, err := New(svc, Options{Listen: "unix:///unused.sock"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Simulate "shutdown committed".
	srv.draining.Store(true)

	h := srv.routes()
	req := httptest.NewRequest(http.MethodGet, api.RouteHealth, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 while draining", rec.Code)
	}
	if got := rec.Header().Get(api.HeaderDraining); got != "1" {
		t.Fatalf("%s = %q, want \"1\"", api.HeaderDraining, got)
	}
	if n := srv.inFlight.Load(); n != 0 {
		t.Fatalf("inFlight = %d after a rejected request, want 0 (gate must not leak)", n)
	}
}

// The ownership-monitor self-dial (HeaderInternalOwner) must bypass the gate —
// it carries no real work and must not be 503'd even while draining.
func TestDrainingGateSkipsOwnerSelfDial(t *testing.T) {
	svc := newSvcForLifecycle(t)
	srv, err := New(svc, Options{Listen: "unix:///unused.sock"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	srv.draining.Store(true)

	h := srv.routes()
	req := httptest.NewRequest(http.MethodGet, api.RouteHealth, nil)
	req.Header.Set(api.HeaderInternalOwner, srv.instanceID)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code == http.StatusServiceUnavailable {
		t.Fatal("owner self-dial was 503'd by the draining gate; it must bypass")
	}
}
