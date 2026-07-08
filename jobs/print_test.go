package jobs

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
)

// captureStdout runs fn with os.Stdout redirected to a pipe and returns what
// it wrote. JobDef.Print writes directly to stdout.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w
	defer func() { os.Stdout = orig }()

	fn()
	_ = w.Close()

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("copy: %v", err)
	}
	return buf.String()
}

// A job that is an array task surfaces its array id (and task index) in the
// details header, so `batchq details <task-uuid>` shows array membership.
func TestPrintShowsArrayForTask(t *testing.T) {
	// AddDetail is a no-op once JobId is set, so populate details first.
	job := &JobDef{Name: "t"}
	job.AddDetail("script", "echo hi")
	job.AddDetail("array_id", "arr-uuid")
	job.AddDetail("array_index", "3")
	job.JobId = "task-uuid"

	out := captureStdout(t, job.Print)

	if !strings.Contains(out, "array    : arr-uuid (task 3)") {
		t.Fatalf("Print output missing array line, got:\n%s", out)
	}
}

// A plain (non-array) job has no array line.
func TestPrintOmitsArrayForPlainJob(t *testing.T) {
	job := &JobDef{Name: "p"}
	job.AddDetail("script", "echo hi")
	job.JobId = "plain-uuid"

	out := captureStdout(t, job.Print)

	if strings.Contains(out, "array    :") {
		t.Fatalf("Print output has an array line for a plain job, got:\n%s", out)
	}
}
