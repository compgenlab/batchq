package client

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mbreese/batchq/api"
	"github.com/mbreese/batchq/server"
	"github.com/mbreese/batchq/service"
	"github.com/mbreese/batchq/storage"
)

// Conformance suite: every test below dials a real Server over a unix
// socket, so the wire path (Client → HTTP → handler → service → storage)
// is exercised end-to-end.

func newUnixPair(t *testing.T) (*Client, *server.Server, context.CancelFunc) {
	t.Helper()
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "batchq.sock")
	dbPath := filepath.Join(dir, "batchq.db")

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

	// Wait for socket to appear.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c, err := DialWithOptions(Options{URL: "unix://" + sockPath, Timeout: 2 * time.Second})
		if err == nil {
			if pingErr := c.Health(ctx); pingErr == nil {
				t.Cleanup(func() {
					cancel()
					<-done
					_ = c.Close()
				})
				return c, srv, cancel
			}
			_ = c.Close()
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done
	t.Fatal("server did not start in time")
	return nil, nil, nil
}

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestClientHealth(t *testing.T) {
	c, _, _ := newUnixPair(t)
	if err := c.Health(ctxT(t)); err != nil {
		t.Fatalf("Health: %v", err)
	}
}

func TestClientSubmitGetCancel(t *testing.T) {
	c, _, _ := newUnixPair(t)
	ctx := ctxT(t)

	dto, err := c.SubmitJob(ctx, &api.SubmitJobRequest{
		Name:    "smoke",
		Details: map[string]string{"script": "echo hi"},
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	if dto.JobID == "" {
		t.Fatal("empty JobID")
	}
	if dto.Status != "QUEUED" {
		t.Fatalf("status: %s", dto.Status)
	}

	got, err := c.GetJob(ctx, dto.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.JobID != dto.JobID {
		t.Fatalf("JobID roundtrip: got %s, want %s", got.JobID, dto.JobID)
	}

	if err := c.CancelJob(ctx, dto.JobID, "test"); err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	got, _ = c.GetJob(ctx, dto.JobID)
	if got.Status != "CANCELED" {
		t.Fatalf("status after cancel: %s", got.Status)
	}
}

func TestClientGetNotFoundIsTyped(t *testing.T) {
	c, _, _ := newUnixPair(t)
	_, err := c.GetJob(ctxT(t), "does-not-exist")
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("not ErrNotFound: %v", err)
	}
}

func TestClientListJobsByStatus(t *testing.T) {
	c, _, _ := newUnixPair(t)
	ctx := ctxT(t)

	for i := 0; i < 3; i++ {
		_, err := c.SubmitJob(ctx, &api.SubmitJobRequest{
			Details: map[string]string{"script": "x"},
		})
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}
	got, err := c.ListJobs(ctx, ListJobsOptions{Statuses: []string{"QUEUED"}})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("len: %d, want 3", len(got))
	}
}

func TestClientHoldReleasePriority(t *testing.T) {
	c, _, _ := newUnixPair(t)
	ctx := ctxT(t)

	dto, _ := c.SubmitJob(ctx, &api.SubmitJobRequest{Details: map[string]string{"script": "x"}})

	if err := c.HoldJob(ctx, dto.JobID); err != nil {
		t.Fatalf("HoldJob: %v", err)
	}
	got, _ := c.GetJob(ctx, dto.JobID)
	if got.Status != "USERHOLD" {
		t.Fatalf("status: %s", got.Status)
	}
	if err := c.ReleaseJob(ctx, dto.JobID); err != nil {
		t.Fatalf("ReleaseJob: %v", err)
	}
	if err := c.AdjustJobPriority(ctx, dto.JobID, 3); err != nil {
		t.Fatalf("AdjustJobPriority: %v", err)
	}
	got, _ = c.GetJob(ctx, dto.JobID)
	if got.Priority != 3 {
		t.Fatalf("priority: %d", got.Priority)
	}
}

func TestClientRunnerLifecycle(t *testing.T) {
	c, _, _ := newUnixPair(t)
	ctx := ctxT(t)

	dto, _ := c.SubmitJob(ctx, &api.SubmitJobRequest{Details: map[string]string{"script": "x"}})

	resp, err := c.ClaimNextJob(ctx, "r-1", "simple", 0, 0, 0)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if resp.Job == nil || resp.Job.JobID != dto.JobID {
		t.Fatalf("claim: %+v", resp)
	}
	if resp.Job.Status != "RUNNING" {
		t.Fatalf("status: %s", resp.Job.Status)
	}

	if err := c.UpdateRunningDetails(ctx, "r-1", dto.JobID, map[string]string{"pid": "12345"}); err != nil {
		t.Fatalf("UpdateRunningDetails: %v", err)
	}
	if err := c.EndJob(ctx, "r-1", dto.JobID, 0, ""); err != nil {
		t.Fatalf("EndJob: %v", err)
	}
	got, _ := c.GetJob(ctx, dto.JobID)
	if got.Status != "SUCCESS" {
		t.Fatalf("status: %s", got.Status)
	}
}

func TestClientProxyLifecycle(t *testing.T) {
	c, _, _ := newUnixPair(t)
	ctx := ctxT(t)

	dto, _ := c.SubmitJob(ctx, &api.SubmitJobRequest{Details: map[string]string{"script": "x"}})

	if _, err := c.ClaimNextJob(ctx, "r-2", "slurm", 0, 0, 0); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := c.MarkJobProxied(ctx, "r-2", dto.JobID, map[string]string{"slurm_job_id": "1234"}); err != nil {
		t.Fatalf("MarkJobProxied: %v", err)
	}
	now := time.Now().UTC()
	if err := c.EndProxiedJob(ctx, "r-2", dto.JobID, "SUCCESS", now.Add(-time.Minute), now, 0, ""); err != nil {
		t.Fatalf("EndProxiedJob: %v", err)
	}
	got, _ := c.GetJob(ctx, dto.JobID)
	if got.Status != "SUCCESS" {
		t.Fatalf("status: %s", got.Status)
	}
}

// TestClientConcurrentClaim is the wire-level regression test for the
// FetchNext/StartJob race — N goroutines compete to claim N jobs through
// the REST API; only one claim per job must succeed.
func TestClientConcurrentClaim(t *testing.T) {
	c, _, _ := newUnixPair(t)
	ctx := ctxT(t)

	const N = 5
	for i := 0; i < N; i++ {
		_, err := c.SubmitJob(ctx, &api.SubmitJobRequest{
			Details: map[string]string{"script": "x"},
		})
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	wins := make(chan string, N*2)
	for i := 0; i < N*2; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			resp, err := c.ClaimNextJob(ctx, runnerID(i), "simple", 0, 0, 0)
			if err != nil {
				t.Errorf("Claim: %v", err)
				return
			}
			if resp.Job != nil {
				wins <- resp.Job.JobID
			}
		}()
	}
	wg.Wait()
	close(wins)

	seen := map[string]bool{}
	count := 0
	for id := range wins {
		if seen[id] {
			t.Fatalf("job %s claimed twice", id)
		}
		seen[id] = true
		count++
	}
	if count != N {
		t.Fatalf("claimed %d, want %d", count, N)
	}
}

func TestClientQueueAndCounts(t *testing.T) {
	c, _, _ := newUnixPair(t)
	ctx := ctxT(t)

	_, _ = c.SubmitJob(ctx, &api.SubmitJobRequest{Details: map[string]string{"script": "x"}})

	counts, err := c.GetJobStatusCounts(ctx, false)
	if err != nil {
		t.Fatalf("counts: %v", err)
	}
	if counts["QUEUED"] != 1 {
		t.Fatalf("queued: %d", counts["QUEUED"])
	}
	queue, err := c.GetQueueJobs(ctx, false, false)
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	if len(queue) != 1 {
		t.Fatalf("queue len: %d", len(queue))
	}
}

func TestClientDependentsAndDeps(t *testing.T) {
	c, _, _ := newUnixPair(t)
	ctx := ctxT(t)

	parent, _ := c.SubmitJob(ctx, &api.SubmitJobRequest{Details: map[string]string{"script": "x"}})
	child, _ := c.SubmitJob(ctx, &api.SubmitJobRequest{
		AfterOk: []string{parent.JobID},
		Details: map[string]string{"script": "x"},
	})

	deps, err := c.GetJobDependents(ctx, parent.JobID)
	if err != nil {
		t.Fatalf("GetJobDependents: %v", err)
	}
	if len(deps) != 1 || deps[0] != child.JobID {
		t.Fatalf("deps: %+v", deps)
	}
}

func runnerID(i int) string {
	return "r-" + intToStr(i)
}

func intToStr(i int) string {
	if i == 0 {
		return "0"
	}
	var out []byte
	for i > 0 {
		out = append([]byte{byte('0' + i%10)}, out...)
		i /= 10
	}
	return string(out)
}

// TestHTTPSBaseHonorsPathPrefix verifies that a https:// URL with a path
// component is preserved as a mount-point prefix on every request — i.e.
// `[batchq] remote = "https://example.com/proxy/path"` sends
// `/proxy/path/api/v1/jobs` rather than `/api/v1/jobs`. This is the
// reverse-proxy-subpath case.
func TestHTTPSBaseHonorsPathPrefix(t *testing.T) {
	cases := []struct {
		url  string
		want string
	}{
		{"https://example.com", "https://example.com"},
		{"https://example.com/", "https://example.com"},
		{"https://example.com/path", "https://example.com/path"},
		{"https://example.com/path/", "https://example.com/path"},
		{"https://example.com/a/b/c", "https://example.com/a/b/c"},
		{"https://example.com:8443/svc", "https://example.com:8443/svc"},
	}
	for _, tc := range cases {
		c, err := DialWithOptions(Options{URL: tc.url})
		if err != nil {
			t.Errorf("DialWithOptions(%q): %v", tc.url, err)
			continue
		}
		if c.base != tc.want {
			t.Errorf("URL %q: base = %q, want %q", tc.url, c.base, tc.want)
		}
		c.Close()
	}
}

func TestClientRejectsUnsupportedSchemes(t *testing.T) {
	for _, u := range []string{"http://example.com", "tcp://example.com:8080", "ftp://x"} {
		if _, err := DialWithOptions(Options{URL: u}); err == nil {
			t.Errorf("DialWithOptions(%q): expected error", u)
		}
	}
}
