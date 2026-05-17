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

func TestParseBackendRejectsRemoteScheme(t *testing.T) {
	if _, err := ParseBackend("batchq-remote://example.com/api/v1"); err == nil {
		t.Fatal("expected error: remote URLs are no longer backends")
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

func TestParseRemoteHTTPS(t *testing.T) {
	got, err := ParseRemote("https://example.com/api/v1")
	if err != nil {
		t.Fatalf("ParseRemote: %v", err)
	}
	if got != "https://example.com/api/v1" {
		t.Fatalf("URL: got %q", got)
	}
}

func TestParseRemoteWithPort(t *testing.T) {
	got, err := ParseRemote("https://10.0.0.5:8443/api/v1")
	if err != nil {
		t.Fatalf("ParseRemote: %v", err)
	}
	if got != "https://10.0.0.5:8443/api/v1" {
		t.Fatalf("URL: got %q", got)
	}
}

func TestParseRemoteSubpath(t *testing.T) {
	got, err := ParseRemote("https://server/subpath")
	if err != nil {
		t.Fatalf("ParseRemote: %v", err)
	}
	if got != "https://server/subpath" {
		t.Fatalf("URL: got %q", got)
	}
}

func TestParseRemoteRejectsHTTP(t *testing.T) {
	_, err := ParseRemote("http://example.com")
	if err == nil {
		t.Fatal("expected error on plain http")
	}
	if !strings.Contains(err.Error(), "https") {
		t.Fatalf("error message: %v", err)
	}
}

func TestParseRemoteRejectsEmpty(t *testing.T) {
	if _, err := ParseRemote(""); err == nil {
		t.Fatal("expected error on empty URL")
	}
}

func TestParseRemoteRejectsMissingHost(t *testing.T) {
	if _, err := ParseRemote("https://"); err == nil {
		t.Fatal("expected error on missing host")
	}
}
