package support

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// SaveConfig then LoadConfig must round-trip the values, and the written file
// must be minimal (omit empty/zero leaves) and 0600 (it holds secrets).
func TestSaveConfigRoundTripAndMinimal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config")

	c := &Config{}
	c.Batchq.Token = "sk-secret"
	c.Web.Password = "hunter2"
	c.Server.IdleTimeout = Duration(90_000_000_000) // 90s — a non-zero Duration must round-trip as a string

	if err := SaveConfig(path, c); err != nil {
		t.Fatalf("SaveConfig: %v", err)
	}

	// Mode 0600.
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("mode = %o, want 600", perm)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	text := string(raw)
	// Minimal: empty leaves are omitted.
	if strings.Contains(text, `runner = ""`) || strings.Contains(text, `remote = ""`) {
		t.Fatalf("written config not minimal:\n%s", text)
	}
	// Duration wrote as a string, not a nanosecond integer.
	if !strings.Contains(text, `idle_timeout = "1m30s"`) {
		t.Fatalf("idle_timeout not round-tripped as duration string:\n%s", text)
	}

	got, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if got.Batchq.Token != "sk-secret" {
		t.Fatalf("token = %q, want sk-secret", got.Batchq.Token)
	}
	if got.Web.Password != "hunter2" {
		t.Fatalf("password = %q, want hunter2", got.Web.Password)
	}
	if got.Server.IdleTimeout.AsDuration().String() != "1m30s" {
		t.Fatalf("idle_timeout = %v, want 1m30s", got.Server.IdleTimeout.AsDuration())
	}
}

// SaveConfig must preserve pre-existing settings when adding a generated value
// (the web-secrets persistence flow: load raw, set a field, save).
func TestSaveConfigPreservesExisting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config")
	if err := os.WriteFile(path, []byte("[server]\n  db = \"sqlite3:///data/q.db\"\n"), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}

	raw, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	raw.Web.Password = "generated"
	if err := SaveConfig(path, raw); err != nil {
		t.Fatalf("SaveConfig: %v", err)
	}

	got, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if got.Server.DB != "sqlite3:///data/q.db" {
		t.Fatalf("existing db lost: %q", got.Server.DB)
	}
	if got.Web.Password != "generated" {
		t.Fatalf("password = %q, want generated", got.Web.Password)
	}
}
