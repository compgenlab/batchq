package web

// server_integration_test.go exercises the web UI end-to-end against a
// real batchq server over a unix socket. It catches contract breaks at
// the web/REST seam (DTO→JobDef conversion, template rendering of
// server-returned data) that pure unit tests on the web package would
// miss.

import (
	"context"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mbreese/batchq/api"
	"github.com/mbreese/batchq/client"
	"github.com/mbreese/batchq/server"
	"github.com/mbreese/batchq/service"
	"github.com/mbreese/batchq/storage"
)

func startBackendForWeb(t *testing.T) (*client.Client, string) {
	t.Helper()
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "backend.sock")
	dbPath := filepath.Join(dir, "backend.db")

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

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := net.Dial("unix", sockPath); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	c, err := client.DialWithOptions(client.Options{URL: "unix://" + sockPath, Timeout: 5 * time.Second})
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

// startWebForTest spins up the web UI on a unix socket and returns a
// hand-rolled http.Client that dials that socket. Web's StartServer is
// blocking and installs signal handlers, so we drive the internals
// directly instead — the test exercises the same handlers either way.
func startWebForTest(t *testing.T, c *client.Client) *http.Client {
	t.Helper()
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "web.sock")

	templates, err := loadWebTemplates()
	if err != nil {
		t.Fatalf("loadWebTemplates: %v", err)
	}
	wsrv := &webServer{client: c, templates: templates, verbose: false}

	mux := http.NewServeMux()
	mux.HandleFunc("/jobs/", wsrv.handleJob)
	mux.HandleFunc("/jobs", wsrv.handleQueue)
	mux.HandleFunc("/search", wsrv.handleSearch)
	mux.HandleFunc("/", wsrv.handleQueue)

	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	httpSrv := &http.Server{Handler: mux}
	go func() { _ = httpSrv.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(ctx)
	})

	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", sockPath)
			},
		},
	}
}

func get(t *testing.T, httpc *http.Client, path string) string {
	t.Helper()
	resp, err := httpc.Get("http://web" + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET %s: status %d: %s", path, resp.StatusCode, body)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(body)
}

func TestWebQueueListsSubmittedJob(t *testing.T) {
	c, _ := startBackendForWeb(t)
	ctx := context.Background()

	dto, err := c.SubmitJob(ctx, &api.SubmitJobRequest{
		Name: "webjob",
		Details: map[string]string{
			"script": "#!/bin/sh\nexit 0\n",
			"uid":    "1000",
			"gid":    "1000",
		},
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	httpc := startWebForTest(t, c)
	body := get(t, httpc, "/jobs?status=QUEUED")

	if !strings.Contains(body, dto.JobID) {
		t.Fatalf("queue page missing job ID %s", dto.JobID)
	}
	if !strings.Contains(body, "webjob") {
		t.Fatalf("queue page missing job name")
	}
	if !strings.Contains(body, "QUEUED") {
		t.Fatalf("queue page missing status badge")
	}
}

func TestWebJobDetailRenders(t *testing.T) {
	c, _ := startBackendForWeb(t)
	ctx := context.Background()

	dto, err := c.SubmitJob(ctx, &api.SubmitJobRequest{
		Name: "detailjob",
		Details: map[string]string{
			"script":   "#!/bin/sh\necho detail\nexit 0\n",
			"procs":    "4",
			"mem":      "2000",
			"walltime": "3600",
			"uid":      strconv.Itoa(1000),
			"gid":      strconv.Itoa(1000),
		},
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	httpc := startWebForTest(t, c)
	body := get(t, httpc, "/jobs/"+dto.JobID)

	for _, want := range []string{dto.JobID, "detailjob", "echo detail", "procs", "walltime"} {
		if !strings.Contains(body, want) {
			t.Fatalf("job detail page missing %q", want)
		}
	}
}

func TestWebSearchFindsJob(t *testing.T) {
	c, _ := startBackendForWeb(t)
	ctx := context.Background()

	dto, err := c.SubmitJob(ctx, &api.SubmitJobRequest{
		Name: "needle",
		Details: map[string]string{
			"script": "#!/bin/sh\nexit 0\n",
			"uid":    "1000",
			"gid":    "1000",
		},
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}

	httpc := startWebForTest(t, c)

	hit := get(t, httpc, "/search?q=needle")
	if !strings.Contains(hit, dto.JobID) {
		t.Fatalf("search page missing job ID %s", dto.JobID)
	}

	miss := get(t, httpc, "/search?q=no-such-token-xyz")
	if strings.Contains(miss, dto.JobID) {
		t.Fatalf("search page matched unrelated job for unrelated query")
	}
	if !strings.Contains(miss, "No matching jobs") {
		t.Fatalf("search page missing empty-state message")
	}
}

func TestWebJobNotFound404(t *testing.T) {
	c, _ := startBackendForWeb(t)
	httpc := startWebForTest(t, c)

	resp, err := httpc.Get("http://web/jobs/00000000-0000-0000-0000-000000000000")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status: %d, want 404", resp.StatusCode)
	}
}
