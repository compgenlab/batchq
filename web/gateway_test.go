package web

// gateway_test.go exercises the combined UI + API gateway mode (`batchq web
// --api`): HTTP Basic on the browser UI at /, and bearer-token auth on the
// reverse-proxied REST API under /api/.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/compgenlab/batchq/api"
)

func gatewayClient(t *testing.T) *http.Client {
	t.Helper()
	c, _ := startBackendForWeb(t)
	// Put one job in the backend so /api/v1/queue returns something real.
	if _, err := c.SubmitJob(context.Background(), &api.SubmitJobRequest{
		Name:    "gwjob",
		Details: map[string]string{"script": "#!/bin/sh\nexit 0\n", "uid": "1000", "gid": "1000"},
	}); err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	return startWebForTestWithOptions(t, c, Options{
		APIEnabled: true,
		APIToken:   "sk-123",
		Username:   "admin",
		Password:   "secret",
	})
}

// do issues a GET with optional header setup and returns status + body.
func do(t *testing.T, hc *http.Client, path string, setup func(*http.Request)) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://web"+path, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if setup != nil {
		setup(req)
	}
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func TestGatewayBrowserAuth(t *testing.T) {
	hc := gatewayClient(t)

	// No credentials -> 401 + Basic challenge.
	req, _ := http.NewRequest(http.MethodGet, "http://web/", nil)
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET / without creds = %d, want 401", resp.StatusCode)
	}
	if got := resp.Header.Get("WWW-Authenticate"); got == "" || got[:5] != "Basic" {
		t.Fatalf("WWW-Authenticate = %q, want Basic challenge", got)
	}

	// Correct credentials -> 200 HTML.
	status, body := do(t, hc, "/", func(r *http.Request) { r.SetBasicAuth("admin", "secret") })
	if status != http.StatusOK {
		t.Fatalf("GET / with admin:secret = %d, want 200", status)
	}
	if body == "" {
		t.Fatal("GET / returned empty body")
	}

	// Wrong password -> 401.
	if status, _ := do(t, hc, "/", func(r *http.Request) { r.SetBasicAuth("admin", "nope") }); status != http.StatusUnauthorized {
		t.Fatalf("GET / with wrong password = %d, want 401", status)
	}
	// Wrong username -> 401.
	if status, _ := do(t, hc, "/", func(r *http.Request) { r.SetBasicAuth("root", "secret") }); status != http.StatusUnauthorized {
		t.Fatalf("GET / with wrong username = %d, want 401", status)
	}
}

func TestGatewayAPIAuth(t *testing.T) {
	hc := gatewayClient(t)
	const apiPath = "/api/v1/queue"

	// No bearer -> 401 + Bearer challenge.
	status, _ := do(t, hc, apiPath, nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("GET %s without token = %d, want 401", apiPath, status)
	}

	// Browser Basic creds don't authenticate the API.
	if status, _ := do(t, hc, apiPath, func(r *http.Request) { r.SetBasicAuth("admin", "secret") }); status != http.StatusUnauthorized {
		t.Fatalf("GET %s with Basic = %d, want 401", apiPath, status)
	}

	// Wrong bearer -> 401.
	if status, _ := do(t, hc, apiPath, func(r *http.Request) { r.Header.Set("Authorization", "Bearer wrong") }); status != http.StatusUnauthorized {
		t.Fatalf("GET %s with wrong token = %d, want 401", apiPath, status)
	}

	// Correct bearer -> 200 and proxied JSON from the backend.
	status, body := do(t, hc, apiPath, func(r *http.Request) { r.Header.Set("Authorization", "Bearer sk-123") })
	if status != http.StatusOK {
		t.Fatalf("GET %s with token = %d, want 200 (body: %s)", apiPath, status, body)
	}
	var payload struct {
		Jobs []map[string]any `json:"jobs"`
	}
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatalf("proxied response not JSON: %v (body: %s)", err, body)
	}
	if len(payload.Jobs) == 0 {
		t.Fatalf("proxied queue empty, want the submitted job (body: %s)", body)
	}
}

// With no password/token configured and --api off, the gateway behaves like the
// classic web UI: / is open and /api/ is not mounted.
func TestGatewayDisabledIsBackwardCompatible(t *testing.T) {
	c, _ := startBackendForWeb(t)
	hc := startWebForTestWithOptions(t, c, Options{})

	if status, _ := do(t, hc, "/", nil); status != http.StatusOK {
		t.Fatalf("GET / (no auth configured) = %d, want 200", status)
	}
	// /api/ is not mounted -> the HTML catch-all handles it as not found.
	if status, _ := do(t, hc, "/api/v1/queue", nil); status == http.StatusOK {
		t.Fatalf("GET /api/v1/queue with --api off = %d, want non-200 (not mounted)", status)
	}
}
