package support

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestDebugLoggerFormatAndNilSafety(t *testing.T) {
	// Nil logger must be a no-op, not a panic.
	var nilLog *DebugLogger
	nilLog.Logf("should not panic %d", 1)

	var buf bytes.Buffer
	l := NewDebugLogger(&buf, "server")
	l.Logf("hello %s", "world")

	line := buf.String()
	if !strings.Contains(line, "pid="+strconv.Itoa(os.Getpid())) {
		t.Errorf("line missing this process PID: %q", line)
	}
	if !strings.Contains(line, "server") {
		t.Errorf("line missing role: %q", line)
	}
	if !strings.Contains(line, "hello world") {
		t.Errorf("line missing message: %q", line)
	}
	if !strings.HasSuffix(line, "\n") {
		t.Errorf("line not newline-terminated: %q", line)
	}
}

func TestNewDebugLoggerNilWriter(t *testing.T) {
	if l := NewDebugLogger(nil, "client"); l != nil {
		t.Fatal("NewDebugLogger(nil, ...) should return a nil logger")
	}
}

func TestOpenDebugLogEmptyPathDisables(t *testing.T) {
	l, err := OpenDebugLog("", "client")
	if err != nil {
		t.Fatalf("OpenDebugLog(\"\"): %v", err)
	}
	if l != nil {
		t.Fatal("empty path should disable logging (nil logger)")
	}
}

func TestOpenDebugLogAppendsConcurrently(t *testing.T) {
	path := filepath.Join(t.TempDir(), "debug.log")
	l, err := OpenDebugLog(path, "client")
	if err != nil {
		t.Fatalf("OpenDebugLog: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			l.Logf("event %d", n)
		}(i)
	}
	wg.Wait()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	lines := strings.Count(string(data), "\n")
	if lines != 50 {
		t.Fatalf("got %d log lines, want 50 (concurrent writes must not interleave/drop)", lines)
	}
}
