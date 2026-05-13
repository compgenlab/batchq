package cmd

// submit_compat_test.go is the regression suite that keeps the
// `batchq submit` CLI contract byte-for-byte stable across v1 → v2.
// Downstream pipelines parse this output — see [[feedback_submit_contract]].

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/mbreese/batchq/client"
	"github.com/mbreese/batchq/server"
	"github.com/mbreese/batchq/service"
	"github.com/mbreese/batchq/storage"
	"github.com/mbreese/batchq/support"
)

// startCompatServer spins up a real server over a unix socket and returns a
// client and a teardown. It also points the package-level `clientURL` at
// the socket so dialClient() will reach this server.
func startCompatServer(t *testing.T) *client.Client {
	t.Helper()
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "compat.sock")
	dbPath := filepath.Join(dir, "compat.db")

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

	// Wait for the server to start.
	var c *client.Client
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c, err = client.DialWithOptions(client.Options{URL: "unix://" + sockPath, Timeout: 2 * time.Second})
		if err == nil {
			if pingErr := c.Health(ctx); pingErr == nil {
				break
			}
			_ = c.Close()
		}
		time.Sleep(20 * time.Millisecond)
	}
	if c == nil {
		cancel()
		<-done
		t.Fatal("server did not start in time")
	}
	t.Cleanup(func() {
		cancel()
		<-done
		_ = c.Close()
	})

	// Config must be non-nil because submitCmd queries it for defaults,
	// and dialClient() reads Config.Server.Listen to compute the URL.
	if Config == nil {
		Config = &support.Config{}
	}
	prevListen := Config.Server.Listen
	prevBackend := clientBackend
	prevNoSpawn := clientNoAutospawn
	Config.Server.Listen = "unix://" + sockPath
	// Disable autospawn so a slow probe doesn't try to fork batchq.
	clientNoAutospawn = true
	t.Cleanup(func() {
		Config.Server.Listen = prevListen
		clientBackend = prevBackend
		clientNoAutospawn = prevNoSpawn
	})

	return c
}

// capture redirects os.Stdout for the duration of fn and returns what was
// written. submit prints the job ID through fmt.Printf so we have to grab
// the real file descriptor.
func capture(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w

	doneRead := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		doneRead <- string(b)
	}()

	fn()
	_ = w.Close()
	os.Stdout = old
	return <-doneRead
}

// resetSubmitFlags zeroes the package-level submit-flag globals so each
// test case starts clean. cobra normally does this between command runs
// when flags are bound to fresh variables — but ours are package-level.
//
// It also resets pflag's `argsLenAtDash` to -1 via reflection. pflag's
// Parse() only resets it inside Init(), not between subsequent Parse()
// calls, so once a test uses `--` every later Execute() in the same
// process sees a stale dash position. The submit CLI keys file-vs-inline
// detection on ArgsLenAtDash, so a stale value silently routes a
// script-file submission down the inline-command path. (pflag@v1.0.6).
func resetSubmitFlags() {
	jobName = ""
	jobDeps = ""
	jobProcs = -1
	jobMemStr = ""
	jobTimeStr = ""
	jobWd = ""
	jobStdout = ""
	jobStderr = ""
	jobEnv = false
	jobHold = false
	verbose = false
	slurmMode = false

	fs := reflect.ValueOf(submitCmd.Flags()).Elem()
	if f := fs.FieldByName("argsLenAtDash"); f.IsValid() {
		*(*int)(unsafe.Pointer(f.UnsafeAddr())) = -1
	}
}

func runSubmit(t *testing.T, args ...string) string {
	t.Helper()
	resetSubmitFlags()
	rootCmd.SetArgs(append([]string{"submit"}, args...))
	out := capture(t, func() {
		if err := rootCmd.Execute(); err != nil {
			t.Fatalf("submit: %v", err)
		}
	})
	return strings.TrimRight(out, "\n")
}

func TestSubmitInlineCommand(t *testing.T) {
	c := startCompatServer(t)

	out := runSubmit(t, "--", "echo", "hello")
	if len(out) < 32 || !strings.Contains(out, "-") {
		t.Fatalf("submit stdout should be a UUID: %q", out)
	}

	dto, err := c.GetJob(context.Background(), out)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if !strings.Contains(dto.Details["script"], "echo hello") {
		t.Fatalf("script missing echo hello: %q", dto.Details["script"])
	}
	if dto.Status != "QUEUED" {
		t.Fatalf("status: %s", dto.Status)
	}
}

func TestSubmitScriptFileWithResourceFlags(t *testing.T) {
	c := startCompatServer(t)

	dir := t.TempDir()
	script := filepath.Join(dir, "job.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho hi\n"), 0o755); err != nil {
		t.Fatalf("write script: %v", err)
	}

	out := runSubmit(t,
		"--name", "myjob",
		"-p", "4",
		"-m", "2G",
		"-t", "1:00:00",
		"--wd", dir,
		script,
	)
	dto, err := c.GetJob(context.Background(), out)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if dto.Name != "myjob" {
		t.Fatalf("name: %q", dto.Name)
	}
	if dto.Details["procs"] != "4" {
		t.Fatalf("procs: %q", dto.Details["procs"])
	}
	// 2G → 2000 MB (decimal, per jobs.ParseMemoryString).
	if dto.Details["mem"] != "2000" {
		t.Fatalf("mem: %q", dto.Details["mem"])
	}
	// 1:00:00 → 3600 seconds (per jobs.ParseWalltimeString).
	if dto.Details["walltime"] != "3600" {
		t.Fatalf("walltime: %q", dto.Details["walltime"])
	}
	if !strings.HasSuffix(dto.Details["wd"], dir) {
		t.Fatalf("wd: %q (want suffix %s)", dto.Details["wd"], dir)
	}
}

func TestSubmitBatchqHeaders(t *testing.T) {
	c := startCompatServer(t)

	dir := t.TempDir()
	script := filepath.Join(dir, "with_headers.sh")
	body := strings.Join([]string{
		"#!/bin/sh",
		"#BATCHQ -name hdrjob",
		"#BATCHQ -procs 8",
		"#BATCHQ -mem 4G",
		"#BATCHQ -walltime 30:00",
		"echo hi",
	}, "\n") + "\n"
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatalf("write: %v", err)
	}

	out := runSubmit(t, script)
	dto, err := c.GetJob(context.Background(), out)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if dto.Name != "hdrjob" {
		t.Fatalf("name: %q", dto.Name)
	}
	if dto.Details["procs"] != "8" {
		t.Fatalf("procs: %q", dto.Details["procs"])
	}
	if dto.Details["mem"] != "4000" {
		t.Fatalf("mem: %q", dto.Details["mem"])
	}
	// 30:00 → 30*60 = 1800 seconds.
	if dto.Details["walltime"] != "1800" {
		t.Fatalf("walltime: %q", dto.Details["walltime"])
	}
}

func TestSubmitSlurmHeaders(t *testing.T) {
	c := startCompatServer(t)

	dir := t.TempDir()
	script := filepath.Join(dir, "sbatch.sh")
	body := strings.Join([]string{
		"#!/bin/sh",
		"#SBATCH -J slurmjob",
		"#SBATCH -c 16",
		"#SBATCH --mem=8G",
		"#SBATCH -t 02:00:00",
		"#SBATCH -o out-%j.log",
		"#SBATCH -e err-%j.log",
		"echo go",
	}, "\n") + "\n"
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatalf("write: %v", err)
	}

	out := runSubmit(t, "--slurm", script)
	dto, err := c.GetJob(context.Background(), out)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if dto.Name != "slurmjob" {
		t.Fatalf("name: %q", dto.Name)
	}
	if dto.Details["procs"] != "16" {
		t.Fatalf("procs: %q", dto.Details["procs"])
	}
	if dto.Details["mem"] != "8000" {
		t.Fatalf("mem: %q", dto.Details["mem"])
	}
	// 02:00:00 → 7200 seconds.
	if dto.Details["walltime"] != "7200" {
		t.Fatalf("walltime: %q", dto.Details["walltime"])
	}
	// %j must be rewritten to %JOBID by the submit parser — see the
	// [[feedback_submit_contract]] invariant. Storage then substitutes
	// %JOBID with the actual job ID at insert time, so the stored value
	// should contain the UUID where %j once was.
	if !strings.Contains(dto.Details["stdout"], dto.JobID) {
		t.Fatalf("stdout did not get %%j → %%JOBID → jobID: %q (want job %s)", dto.Details["stdout"], dto.JobID)
	}
	if !strings.Contains(dto.Details["stderr"], dto.JobID) {
		t.Fatalf("stderr did not get %%j → %%JOBID → jobID: %q (want job %s)", dto.Details["stderr"], dto.JobID)
	}
}

func TestSubmitHoldFlag(t *testing.T) {
	c := startCompatServer(t)

	out := runSubmit(t, "--hold", "--", "echo", "x")
	dto, err := c.GetJob(context.Background(), out)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if dto.Status != "USERHOLD" {
		t.Fatalf("status: %s, want USERHOLD", dto.Status)
	}
}

func TestSubmitStdoutIsJobIDOnly(t *testing.T) {
	// The submit contract: stdout is a single UUID + newline. Nothing else.
	startCompatServer(t)

	resetSubmitFlags()
	rootCmd.SetArgs([]string{"submit", "--", "echo", "x"})
	raw := capture(t, func() {
		if err := rootCmd.Execute(); err != nil {
			t.Fatalf("submit: %v", err)
		}
	})
	// Must end with exactly one newline.
	if !strings.HasSuffix(raw, "\n") {
		t.Fatalf("expected trailing newline, got %q", raw)
	}
	body := strings.TrimRight(raw, "\n")
	if strings.ContainsAny(body, " \t\n") {
		t.Fatalf("stdout must be just a UUID, got %q", raw)
	}
	if strings.Count(body, "-") < 4 {
		t.Fatalf("expected UUID with hyphens, got %q", body)
	}
}
