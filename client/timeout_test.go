package client

import (
	"testing"
	"time"
)

// NoRequestTimeout must drop the per-request http.Client timeout to 0 (so only
// the caller's context bounds a request) while keeping opts.Timeout at a
// concrete value for the autospawn health probe. Regression guard for the bug
// where a "long-running" client was still 30s-capped.
func TestNoRequestTimeoutDisablesHTTPCap(t *testing.T) {
	c, err := DialWithOptions(Options{URL: "unix:///ignored.sock", NoRequestTimeout: true})
	if err != nil {
		t.Fatalf("DialWithOptions: %v", err)
	}
	if c.httpC.Timeout != 0 {
		t.Fatalf("httpC.Timeout = %v, want 0 (no per-request cap)", c.httpC.Timeout)
	}
	if c.opts.Timeout != 30*time.Second {
		t.Fatalf("opts.Timeout = %v, want 30s (probe timeout stays concrete)", c.opts.Timeout)
	}
}

// The default (no options) keeps the historical 30s per-request cap.
func TestDefaultHTTPCap(t *testing.T) {
	c, err := DialWithOptions(Options{URL: "unix:///ignored.sock"})
	if err != nil {
		t.Fatalf("DialWithOptions: %v", err)
	}
	if c.httpC.Timeout != 30*time.Second {
		t.Fatalf("httpC.Timeout = %v, want 30s", c.httpC.Timeout)
	}
}
