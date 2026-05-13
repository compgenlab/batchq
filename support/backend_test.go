package support

import (
	"strings"
	"testing"
)

func TestParseBackendSqlite3(t *testing.T) {
	b, err := ParseBackend("sqlite3:///home/me/db.sqlite")
	if err != nil {
		t.Fatalf("ParseBackend: %v", err)
	}
	if b.Scheme != BackendSqlite3 {
		t.Fatalf("scheme: got %q want sqlite3", b.Scheme)
	}
	if !b.IsLocal() {
		t.Fatal("sqlite3 should be local")
	}
	path, err := b.SqlitePath()
	if err != nil {
		t.Fatalf("SqlitePath: %v", err)
	}
	if path != "/home/me/db.sqlite" {
		t.Fatalf("path: got %q", path)
	}
}

func TestParseBackendSqlite3RejectsHost(t *testing.T) {
	b, err := ParseBackend("sqlite3://somehost/path")
	if err != nil {
		t.Fatalf("ParseBackend: %v", err)
	}
	if _, err := b.SqlitePath(); err == nil {
		t.Fatal("expected error on sqlite3 URL with host")
	}
}

func TestParseBackendBatchqRemoteHTTPS(t *testing.T) {
	b, err := ParseBackend("batchq-remote://example.com/api/v1")
	if err != nil {
		t.Fatalf("ParseBackend: %v", err)
	}
	if b.IsLocal() {
		t.Fatal("batchq-remote should not be local")
	}
	got, err := b.RemoteHTTPURL()
	if err != nil {
		t.Fatalf("RemoteHTTPURL: %v", err)
	}
	if got != "https://example.com/api/v1" {
		t.Fatalf("URL: got %q", got)
	}
}

func TestParseBackendBatchqRemoteInsecure(t *testing.T) {
	b, err := ParseBackend("batchq-remote://10.0.0.5:8080/api/v1?insecure=true")
	if err != nil {
		t.Fatalf("ParseBackend: %v", err)
	}
	got, err := b.RemoteHTTPURL()
	if err != nil {
		t.Fatalf("RemoteHTTPURL: %v", err)
	}
	if got != "http://10.0.0.5:8080/api/v1" {
		t.Fatalf("URL: got %q", got)
	}
}

func TestParseBackendUnsupportedScheme(t *testing.T) {
	_, err := ParseBackend("redis://x/y")
	if err == nil {
		t.Fatal("expected error on redis scheme")
	}
	if !strings.Contains(err.Error(), "unsupported scheme") {
		t.Fatalf("error message: %v", err)
	}
}

func TestParseBackendEmpty(t *testing.T) {
	if _, err := ParseBackend(""); err == nil {
		t.Fatal("expected error on empty URL")
	}
}

func TestParseBackendInvalidInsecure(t *testing.T) {
	b, err := ParseBackend("batchq-remote://example.com/?insecure=maybe")
	if err != nil {
		t.Fatalf("ParseBackend: %v", err)
	}
	if _, err := b.RemoteHTTPURL(); err == nil {
		t.Fatal("expected error on insecure=maybe")
	}
}
