package jobs

import (
	"bytes"
	"io"
	"os"
	"sort"
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

// The [job details] block prints keys in a stable alphabetical order,
// regardless of the order details were added.
func TestPrintSortsDetailKeys(t *testing.T) {
	job := &JobDef{Name: "s"}
	// Add out of alphabetical order; "script" is extracted and printed last.
	job.AddDetail("script", "echo hi")
	job.AddDetail("walltime", "60")
	job.AddDetail("gid", "1000")
	job.AddDetail("procs", "4")
	job.AddDetail("mem", "2000")
	job.JobId = "uuid"

	out := captureStdout(t, job.Print)

	// Collect the detail keys in the order they were printed.
	var keys []string
	inBlock := false
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "---[job details]---") {
			inBlock = true
			continue
		}
		if strings.HasPrefix(line, "---[job script]") || strings.HasPrefix(line, "---[job running") {
			break
		}
		if inBlock {
			if k, _, ok := strings.Cut(line, " :"); ok {
				keys = append(keys, strings.TrimSpace(k))
			}
		}
	}

	if !sort.StringsAreSorted(keys) {
		t.Fatalf("detail keys not sorted: %v", keys)
	}
	// Sanity: the keys we added (minus script) are all present.
	want := []string{"gid", "mem", "procs", "walltime"}
	for _, w := range want {
		found := false
		for _, k := range keys {
			if k == w {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("key %q missing from details: %v", w, keys)
		}
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
