package client

// retry_test.go exercises do()'s reconnect-and-retry across an idle-server
// handoff: a draining 503 (or a connect failure) must be retried after
// reconnecting, turning the rare cull-window collision into a transparent
// success instead of a hard error.

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mbreese/batchq/api"
)

type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func resp(status int, header http.Header, body string) *http.Response {
	if header == nil {
		header = http.Header{}
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body))}
}

// A submit that hits a draining server (503 + HeaderDraining) must be retried
// after a reconnect and ultimately succeed — without the caller seeing an error.
func TestDoRetriesAfterDrainingHandoff(t *testing.T) {
	orig := handoffBackoffs
	handoffBackoffs = []time.Duration{10 * time.Millisecond, 10 * time.Millisecond, 10 * time.Millisecond}
	t.Cleanup(func() { handoffBackoffs = orig })

	c, err := DialWithOptions(Options{URL: "unix:///ignored.sock", Timeout: time.Second})
	if err != nil {
		t.Fatalf("DialWithOptions: %v", err)
	}

	var calls atomic.Int32
	c.httpC = &http.Client{Transport: rtFunc(func(r *http.Request) (*http.Response, error) {
		switch calls.Add(1) {
		case 1:
			// First submit: server is draining.
			h := http.Header{}
			h.Set(api.HeaderDraining, "1")
			return resp(http.StatusServiceUnavailable, h, "shutting down"), nil
		default:
			// ensureUp's health probe, then the resend — both succeed.
			return resp(http.StatusOK, nil, "{}"), nil
		}
	})}
	c.auto = AutospawnConfig{Enabled: true, SpawnFunc: func(string) error { return nil }}

	if err := c.do(context.Background(), http.MethodPost, api.Prefix+api.RouteJobs, map[string]string{"x": "y"}, nil); err != nil {
		t.Fatalf("do should have recovered via retry, got: %v", err)
	}
	if n := calls.Load(); n < 3 {
		t.Fatalf("expected >=3 round-trips (draining 503, health probe, resend), got %d", n)
	}
}

// Without auto.Enabled (e.g. a remote client) a draining 503 is surfaced as-is,
// not retried — there's nothing to respawn.
func TestDoDoesNotRetryWhenAutospawnDisabled(t *testing.T) {
	c, err := DialWithOptions(Options{URL: "https://example.invalid", Timeout: time.Second})
	if err != nil {
		t.Fatalf("DialWithOptions: %v", err)
	}
	var calls atomic.Int32
	c.httpC = &http.Client{Transport: rtFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		h := http.Header{}
		h.Set(api.HeaderDraining, "1")
		return resp(http.StatusServiceUnavailable, h, "shutting down"), nil
	})}
	// c.auto stays zero (disabled).

	err = c.do(context.Background(), http.MethodPost, api.Prefix+api.RouteJobs, nil, nil)
	if err == nil {
		t.Fatal("expected the draining 503 to surface as an error with autospawn disabled")
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("expected exactly 1 round-trip (no retry), got %d", n)
	}
}
