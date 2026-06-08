package support

// debuglog.go is the lifecycle debug log behind the --log flag / [batchq] log
// config key. It exists to diagnose "too many servers" / SQLITE_BUSY problems:
// clients and (auto)spawned servers append timestamped, PID-tagged lines to one
// shared file, so a human can see exactly when each server PID started, served
// requests, and shut down — and whether two ever overlapped.

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// debugTimeFormat includes milliseconds and the zone offset so interleaved
// lines from different processes order correctly by wall clock.
const debugTimeFormat = "2006-01-02T15:04:05.000Z07:00"

// DebugLogger writes one line per event to an underlying writer, prefixed with
// a timestamp, the process PID, and a role ("client" / "server"). It is
// safe for concurrent use and a nil *DebugLogger is a no-op, so call sites can
// hold a possibly-nil logger without guarding every call.
type DebugLogger struct {
	mu   sync.Mutex
	w    io.Writer
	role string
	pid  int
}

// NewDebugLogger returns a logger that writes to w tagged with role. Returns
// nil if w is nil, so "logging disabled" flows through as a nil *DebugLogger.
func NewDebugLogger(w io.Writer, role string) *DebugLogger {
	if w == nil {
		return nil
	}
	return &DebugLogger{w: w, role: role, pid: os.Getpid()}
}

// OpenDebugLog opens (creating, append-mode) the debug log file at path and
// returns a *DebugLogger tagged with role. An empty path disables logging
// (returns a nil logger and nil error). Append mode + whole-line writes make
// concurrent appends from the client and the server it spawned safe on local
// filesystems.
func OpenDebugLog(path, role string) (*DebugLogger, error) {
	if path == "" {
		return nil, nil
	}
	if expanded, err := ExpandPathAbs(path); err == nil {
		path = expanded
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		_ = os.MkdirAll(dir, 0o755)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open debug log %s: %w", path, err)
	}
	return NewDebugLogger(f, role), nil
}

// Logf writes one event line: "<ts> pid=<pid> <role> <message>". Safe on a nil
// receiver (no-op) and safe for concurrent callers.
func (l *DebugLogger) Logf(format string, args ...any) {
	if l == nil || l.w == nil {
		return
	}
	msg := fmt.Sprintf(format, args...)
	line := fmt.Sprintf("%s pid=%d %-6s %s\n", time.Now().Format(debugTimeFormat), l.pid, l.role, msg)
	l.mu.Lock()
	_, _ = io.WriteString(l.w, line)
	l.mu.Unlock()
}
