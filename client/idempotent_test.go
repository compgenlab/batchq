package client

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/compgenlab/batchq/api"
)

func TestRetryableIdempotent(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"request timeout", context.DeadlineExceeded, true},
		{"5xx server error", &HTTPError{StatusCode: http.StatusInternalServerError}, true},
		{"503 draining handoff", &HTTPError{StatusCode: http.StatusServiceUnavailable, Draining: true}, true},
		{"4xx bad request", &HTTPError{StatusCode: http.StatusBadRequest}, false},
		{"404 not found", &HTTPError{StatusCode: http.StatusNotFound}, false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := retryableIdempotent(tc.err); got != tc.want {
				t.Fatalf("retryableIdempotent(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// A plain 4xx must NOT be retried even by the idempotent predicate (it's a real
// client error, not a transient), while retryableHandoff stays strict.
func TestRetryableHandoffVsIdempotent(t *testing.T) {
	timeout := context.DeadlineExceeded
	if retryableHandoff(timeout) {
		t.Fatal("retryableHandoff must NOT retry a timeout (keyless submit would duplicate)")
	}
	if !retryableIdempotent(timeout) {
		t.Fatal("retryableIdempotent must retry a timeout")
	}
	badReq := &HTTPError{StatusCode: http.StatusBadRequest}
	if retryableIdempotent(badReq) {
		t.Fatal("retryableIdempotent must not retry a 4xx")
	}
	if !errors.Is(context.DeadlineExceeded, context.DeadlineExceeded) {
		t.Fatal("sanity")
	}
}

// A submit carrying a client-assigned id retries a transient (a 5xx / timeout)
// and converges — because the id makes it safe (rtFunc + tiny backoffs from
// retry_test.go).
func TestSubmitJobRetriesTransientWhenIdempotent(t *testing.T) {
	orig := handoffBackoffs
	handoffBackoffs = []time.Duration{5 * time.Millisecond, 5 * time.Millisecond, 5 * time.Millisecond}
	t.Cleanup(func() { handoffBackoffs = orig })

	c, err := DialWithOptions(Options{URL: "unix:///ignored.sock", Timeout: time.Second})
	if err != nil {
		t.Fatalf("DialWithOptions: %v", err)
	}
	var calls atomic.Int32
	c.httpC = &http.Client{Transport: rtFunc(func(r *http.Request) (*http.Response, error) {
		switch calls.Add(1) {
		case 1:
			// First submit hits a transient (e.g. SQLITE_BUSY behind a Lustre stall).
			return resp(http.StatusInternalServerError, nil, `{"error":"database is locked"}`), nil
		default:
			// ensureUp health probe, then the resend — the job comes back.
			return resp(http.StatusOK, nil, `{"job":{"job_id":"aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee","status":"QUEUED"}}`), nil
		}
	})}
	c.auto = AutospawnConfig{Enabled: true, SpawnFunc: func(string) error { return nil }}

	dto, err := c.SubmitJob(context.Background(), &api.SubmitJobRequest{
		JobID: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee", Details: map[string]string{"script": "x"},
	})
	if err != nil {
		t.Fatalf("idempotent submit should have retried through the transient, got: %v", err)
	}
	if dto.JobID != "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee" {
		t.Fatalf("job id = %q", dto.JobID)
	}
	if n := calls.Load(); n < 3 {
		t.Fatalf("expected >=3 round-trips (500, health probe, resend), got %d", n)
	}
}

// The SAME transient (a 5xx) on a keyless submit must NOT be retried — retrying
// would risk a duplicate since the server assigns a fresh id each time.
func TestSubmitJobDoesNotRetryTransientWithoutID(t *testing.T) {
	c, err := DialWithOptions(Options{URL: "unix:///ignored.sock", Timeout: time.Second})
	if err != nil {
		t.Fatalf("DialWithOptions: %v", err)
	}
	var calls atomic.Int32
	c.httpC = &http.Client{Transport: rtFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		return resp(http.StatusInternalServerError, nil, `{"error":"database is locked"}`), nil
	})}
	c.auto = AutospawnConfig{Enabled: true, SpawnFunc: func(string) error { return nil }}

	_, err = c.SubmitJob(context.Background(), &api.SubmitJobRequest{Details: map[string]string{"script": "x"}})
	if err == nil {
		t.Fatal("keyless submit should surface the 5xx, not retry it")
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("expected exactly 1 round-trip (no retry), got %d", n)
	}
}
