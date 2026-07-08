package client

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// NewReverseProxy forwards requests to the client's backend, preserving the
// path and injecting the client's bearer token so the backend authenticates it.
func TestNewReverseProxyForwardsWithToken(t *testing.T) {
	var gotPath, gotAuth string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		_, _ = io.WriteString(w, "ok")
	}))
	defer backend.Close()

	// backend.URL is http://127.0.0.1:PORT — the "http" scheme uses the tcp path.
	c, err := DialWithOptions(Options{URL: backend.URL, Token: "sk-xyz"})
	if err != nil {
		t.Fatalf("DialWithOptions: %v", err)
	}
	proxy := c.NewReverseProxy()

	req := httptest.NewRequest(http.MethodGet, "http://gateway/api/v1/queue", nil)
	rec := httptest.NewRecorder()
	proxy.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("proxied status = %d, want 200", rec.Code)
	}
	if gotPath != "/api/v1/queue" {
		t.Fatalf("backend saw path %q, want /api/v1/queue", gotPath)
	}
	if gotAuth != "Bearer sk-xyz" {
		t.Fatalf("backend saw Authorization %q, want Bearer sk-xyz", gotAuth)
	}
}

// Without a token, the proxy injects no Authorization header (the backend's own
// auth, if any, is the gate).
func TestNewReverseProxyNoTokenNoHeader(t *testing.T) {
	var gotAuth string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
	}))
	defer backend.Close()

	c, err := DialWithOptions(Options{URL: backend.URL})
	if err != nil {
		t.Fatalf("DialWithOptions: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "http://gateway/api/v1/queue", nil)
	c.NewReverseProxy().ServeHTTP(httptest.NewRecorder(), req)

	if gotAuth != "" {
		t.Fatalf("backend saw Authorization %q, want none", gotAuth)
	}
}
