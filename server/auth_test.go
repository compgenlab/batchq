package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/mbreese/batchq/api"
	"github.com/mbreese/batchq/service"
	"github.com/mbreese/batchq/storage"
)

// newAuthTestServer mirrors newTestServer but sets a shared auth token so
// the withAuth middleware is active.
func newAuthTestServer(t *testing.T, token string) *httptest.Server {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "batchq.db")
	st, err := storage.Open(context.Background(), dbPath, storage.Options{})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	s, err := New(service.New(st), Options{Listen: "unix:///dev/null", AuthToken: token})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	ts := httptest.NewServer(s.routes())
	t.Cleanup(ts.Close)
	return ts
}

// submitWithAuth POSTs a job, optionally with an Authorization header, and
// returns the status code.
func submitWithAuth(t *testing.T, ts *httptest.Server, authHeader string) int {
	t.Helper()
	body, _ := json.Marshal(api.SubmitJobRequest{Details: map[string]string{"script": "echo hi"}})
	req, err := http.NewRequest("POST", ts.URL+api.Prefix+api.RouteJobs, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new req: %v", err)
	}
	if authHeader != "" {
		req.Header.Set(api.HeaderAuthorization, authHeader)
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

func TestAuthRejectsMissingToken(t *testing.T) {
	ts := newAuthTestServer(t, "s3cret")
	if code := submitWithAuth(t, ts, ""); code != http.StatusUnauthorized {
		t.Fatalf("missing token: status %d, want 401", code)
	}
}

func TestAuthRejectsWrongToken(t *testing.T) {
	ts := newAuthTestServer(t, "s3cret")
	if code := submitWithAuth(t, ts, "Bearer nope"); code != http.StatusUnauthorized {
		t.Fatalf("wrong token: status %d, want 401", code)
	}
}

func TestAuthAcceptsCorrectToken(t *testing.T) {
	ts := newAuthTestServer(t, "s3cret")
	// Scheme match is case-insensitive.
	if code := submitWithAuth(t, ts, "bearer s3cret"); code != http.StatusCreated {
		t.Fatalf("correct token: status %d, want 201", code)
	}
}

func TestAuthHealthExempt(t *testing.T) {
	ts := newAuthTestServer(t, "s3cret")
	resp, err := ts.Client().Get(ts.URL + api.RouteHealth)
	if err != nil {
		t.Fatalf("get health: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health with auth enabled: status %d, want 200", resp.StatusCode)
	}
}

func TestAuthDisabledAllowsNoToken(t *testing.T) {
	ts := newAuthTestServer(t, "") // no token configured -> auth off
	if code := submitWithAuth(t, ts, ""); code != http.StatusCreated {
		t.Fatalf("auth disabled: status %d, want 201", code)
	}
}
