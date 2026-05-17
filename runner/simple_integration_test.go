package runner

// simple_integration_test.go exercises the simple runner end-to-end against
// a real server over a unix socket. It catches contract breaks at the
// runner/REST seam (atomic-claim wiring, pid running-detail update, EndJob
// finalization) that unit tests on the runner alone would miss.

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/mbreese/batchq/api"
	"github.com/mbreese/batchq/client"
	"github.com/mbreese/batchq/jobs"
	"github.com/mbreese/batchq/server"
	"github.com/mbreese/batchq/service"
	"github.com/mbreese/batchq/storage"
)

// jobDetails returns the standard details for a runner test job. When the
// test runs as root (e.g. inside the Go Docker container), the runner
// refuses to start jobs without explicit uid/gid; supply the current ones.
func jobDetails(script string) map[string]string {
	return map[string]string{
		"script": script,
		"uid":    strconv.Itoa(os.Getuid()),
		"gid":    strconv.Itoa(os.Getgid()),
	}
}

func startServerForRunner(t *testing.T) (*client.Client, string) {
	t.Helper()
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "runner.sock")
	dbPath := filepath.Join(dir, "runner.db")

	st, err := storage.Open(context.Background(), dbPath, storage.Options{})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	svc := service.New(st)
	srv, err := server.New(svc, server.Options{Listen: "unix://" + sockPath})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = srv.Serve(ctx)
		close(done)
	}()

	var c *client.Client
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := net.Dial("unix", sockPath); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	c, err = client.DialWithOptions(client.Options{URL: "unix://" + sockPath, Timeout: 5 * time.Second})
	if err != nil {
		cancel()
		<-done
		t.Fatalf("client.Dial: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		<-done
		_ = c.Close()
	})
	return c, dir
}

func TestSimpleRunnerEndToEnd(t *testing.T) {
	c, workdir := startServerForRunner(t)

	// Use a unique spool dir for this test so concurrent test runs don't
	// trip over each other.
	t.Setenv("BATCHQ_HOME", workdir)

	scriptOut := filepath.Join(workdir, "hello.out")
	script := "#!/bin/sh\necho hello-world > " + scriptOut + "\nexit 0\n"

	ctx := context.Background()
	dto, err := c.SubmitJob(ctx, &api.SubmitJobRequest{
		Name:    "hello",
		Details: jobDetails(script),
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	r := NewSimpleRunner(c).SetMaxJobCount(1)
	if !r.Start() {
		t.Fatalf("runner did not run a job")
	}

	// The runner's exit goroutine reports EndJob; allow a brief settle.
	var final *api.JobDTO
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		final, err = c.GetJob(ctx, dto.JobID)
		if err == nil && final != nil && (final.Status == jobs.SUCCESS.String() || final.Status == jobs.FAILED.String()) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if final.Status != jobs.SUCCESS.String() {
		t.Fatalf("status: %s want SUCCESS", final.Status)
	}
	if final.ReturnCode != 0 {
		t.Fatalf("return_code: %d want 0", final.ReturnCode)
	}
	if got, err := os.ReadFile(scriptOut); err != nil {
		t.Fatalf("read script output: %v", err)
	} else if string(got) != "hello-world\n" {
		t.Fatalf("script output: %q", got)
	}
}

func TestSimpleRunnerFailedJob(t *testing.T) {
	c, workdir := startServerForRunner(t)
	t.Setenv("BATCHQ_HOME", workdir)

	ctx := context.Background()
	dto, err := c.SubmitJob(ctx, &api.SubmitJobRequest{
		Name:    "boom",
		Details: jobDetails("#!/bin/sh\nexit 7\n"),
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	r := NewSimpleRunner(c).SetMaxJobCount(1)
	if !r.Start() {
		t.Fatalf("runner did not run a job")
	}

	var final *api.JobDTO
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		final, err = c.GetJob(ctx, dto.JobID)
		if err == nil && final != nil && (final.Status == jobs.SUCCESS.String() || final.Status == jobs.FAILED.String()) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if final.Status != jobs.FAILED.String() {
		t.Fatalf("status: %s want FAILED", final.Status)
	}
	if final.ReturnCode != 7 {
		t.Fatalf("return_code: %d want 7", final.ReturnCode)
	}
}

func TestSimpleRunnerNoJobsExitsImmediately(t *testing.T) {
	c, workdir := startServerForRunner(t)
	t.Setenv("BATCHQ_HOME", workdir)

	r := NewSimpleRunner(c)
	if r.Start() {
		t.Fatalf("runner reported it ran a job when queue was empty")
	}
}
