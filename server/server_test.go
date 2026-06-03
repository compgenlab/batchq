package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mbreese/batchq/api"
	"github.com/mbreese/batchq/internal/testsupport"
	"github.com/mbreese/batchq/service"
	"github.com/mbreese/batchq/storage"
)

// newTestServer wires storage+service+server and returns an httptest.Server
// fronting the routes. Use it for handler-level tests that don't care
// whether the transport is a unix socket or TCP.
func newTestServer(t *testing.T) (*httptest.Server, *Server) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "batchq.db")
	st, err := storage.Open(context.Background(), dbPath, storage.Options{})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	svc := service.New(st)
	s, err := New(svc, Options{Listen: "unix:///dev/null"}) // Listen unused
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	ts := httptest.NewServer(s.routes())
	t.Cleanup(ts.Close)
	return ts, s
}

// httpDo is a thin convenience wrapper.
func httpDo(t *testing.T, ts *httptest.Server, method, path string, body any) (*http.Response, []byte) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, ts.URL+path, rdr)
	if err != nil {
		t.Fatalf("new req: %v", err)
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp, out
}

// --- Health -----------------------------------------------------------

func TestHealth(t *testing.T) {
	ts, _ := newTestServer(t)
	resp, body := httpDo(t, ts, "GET", "/healthz", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d, body=%s", resp.StatusCode, body)
	}
	if resp.Header.Get(api.HeaderVersion) != api.Version {
		t.Fatalf("missing API version header: %s", resp.Header.Get(api.HeaderVersion))
	}
}

// --- Submit / Get / List ----------------------------------------------

func TestSubmitJobOverHTTP(t *testing.T) {
	ts, _ := newTestServer(t)
	resp, body := httpDo(t, ts, "POST", api.Prefix+api.RouteJobs, api.SubmitJobRequest{
		Name:    "hello",
		Details: map[string]string{"script": "echo hi"},
	})
	if resp.StatusCode != 201 {
		t.Fatalf("status: %d, body=%s", resp.StatusCode, body)
	}
	var sresp api.SubmitJobResponse
	if err := json.Unmarshal(body, &sresp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if sresp.Job == nil || sresp.Job.JobID == "" {
		t.Fatalf("job: %+v", sresp.Job)
	}
	if sresp.Job.Status != "QUEUED" {
		t.Fatalf("status: %s", sresp.Job.Status)
	}
}

func TestSubmitRejectsBadJSON(t *testing.T) {
	ts, _ := newTestServer(t)
	req, err := http.NewRequest("POST", ts.URL+api.Prefix+api.RouteJobs,
		bytes.NewReader([]byte("not json")))
	if err != nil {
		t.Fatalf("new req: %v", err)
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
}

func TestSubmitRejectsUnknownField(t *testing.T) {
	ts, _ := newTestServer(t)
	resp, _ := httpDo(t, ts, "POST", api.Prefix+api.RouteJobs, map[string]any{
		"name":     "x",
		"details":  map[string]string{"script": "x"},
		"bogus":    "field",
	})
	if resp.StatusCode != 400 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
}

func TestGetJobNotFound(t *testing.T) {
	ts, _ := newTestServer(t)
	resp, _ := httpDo(t, ts, "GET", api.Prefix+"/jobs/missing", nil)
	if resp.StatusCode != 404 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
}

func TestListJobs(t *testing.T) {
	ts, _ := newTestServer(t)
	for i := 0; i < 3; i++ {
		_, _ = httpDo(t, ts, "POST", api.Prefix+api.RouteJobs, api.SubmitJobRequest{
			Details: map[string]string{"script": "x"},
		})
	}
	resp, body := httpDo(t, ts, "GET", api.Prefix+api.RouteJobs+"?all=true", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d, body=%s", resp.StatusCode, body)
	}
	var lresp api.ListJobsResponse
	if err := json.Unmarshal(body, &lresp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(lresp.Jobs) != 3 {
		t.Fatalf("got %d, want 3", len(lresp.Jobs))
	}
}

func TestListJobsByStatus(t *testing.T) {
	ts, _ := newTestServer(t)
	for i := 0; i < 2; i++ {
		_, _ = httpDo(t, ts, "POST", api.Prefix+api.RouteJobs, api.SubmitJobRequest{
			Details: map[string]string{"script": "x"},
		})
	}
	resp, body := httpDo(t, ts, "GET", api.Prefix+api.RouteJobs+"?status=QUEUED", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d, body=%s", resp.StatusCode, body)
	}
	var lresp api.ListJobsResponse
	_ = json.Unmarshal(body, &lresp)
	if len(lresp.Jobs) != 2 {
		t.Fatalf("got %d, want 2", len(lresp.Jobs))
	}
}

// --- Queue / counts ---------------------------------------------------

func TestQueueAndCounts(t *testing.T) {
	ts, _ := newTestServer(t)
	_, _ = httpDo(t, ts, "POST", api.Prefix+api.RouteJobs, api.SubmitJobRequest{
		Details: map[string]string{"script": "x"},
	})

	resp, body := httpDo(t, ts, "GET", api.Prefix+api.RouteQueueCounts, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	var counts api.StatusCountsResponse
	_ = json.Unmarshal(body, &counts)
	if counts.Counts["QUEUED"] != 1 {
		t.Fatalf("queued count: %d", counts.Counts["QUEUED"])
	}

	resp, body = httpDo(t, ts, "GET", api.Prefix+api.RouteQueue, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("queue status: %d", resp.StatusCode)
	}
	var q api.ListJobsResponse
	_ = json.Unmarshal(body, &q)
	if len(q.Jobs) != 1 {
		t.Fatalf("queue len: %d", len(q.Jobs))
	}
}

// --- Hold / release / priority / cancel ------------------------------

func TestHoldReleasePriorityCancel(t *testing.T) {
	ts, _ := newTestServer(t)
	resp, body := httpDo(t, ts, "POST", api.Prefix+api.RouteJobs, api.SubmitJobRequest{
		Details: map[string]string{"script": "x"},
	})
	if resp.StatusCode != 201 {
		t.Fatalf("submit: %d body=%s", resp.StatusCode, body)
	}
	var sresp api.SubmitJobResponse
	_ = json.Unmarshal(body, &sresp)
	jobID := sresp.Job.JobID

	// Hold
	resp, _ = httpDo(t, ts, "POST", api.Prefix+"/jobs/"+jobID+"/hold", nil)
	if resp.StatusCode != 204 {
		t.Fatalf("hold: %d", resp.StatusCode)
	}
	resp, body = httpDo(t, ts, "GET", api.Prefix+"/jobs/"+jobID, nil)
	var jresp api.JobResponse
	_ = json.Unmarshal(body, &jresp)
	if jresp.Job.Status != "USERHOLD" {
		t.Fatalf("status after hold: %s", jresp.Job.Status)
	}

	// Release
	resp, _ = httpDo(t, ts, "POST", api.Prefix+"/jobs/"+jobID+"/release", nil)
	if resp.StatusCode != 204 {
		t.Fatalf("release: %d", resp.StatusCode)
	}

	// Priority
	resp, _ = httpDo(t, ts, "POST", api.Prefix+"/jobs/"+jobID+"/priority", api.PriorityRequest{Delta: 5})
	if resp.StatusCode != 204 {
		t.Fatalf("priority: %d", resp.StatusCode)
	}

	// Cancel
	resp, _ = httpDo(t, ts, "DELETE", api.Prefix+"/jobs/"+jobID, api.CancelJobRequest{Reason: "test"})
	if resp.StatusCode != 204 {
		t.Fatalf("cancel: %d", resp.StatusCode)
	}
	resp, body = httpDo(t, ts, "GET", api.Prefix+"/jobs/"+jobID, nil)
	_ = json.Unmarshal(body, &jresp)
	if jresp.Job.Status != "CANCELED" {
		t.Fatalf("status after cancel: %s", jresp.Job.Status)
	}
}

// --- Full runner round-trip -----------------------------------------

func TestRunnerClaimEndRoundTrip(t *testing.T) {
	ts, _ := newTestServer(t)
	_, body := httpDo(t, ts, "POST", api.Prefix+api.RouteJobs, api.SubmitJobRequest{
		Details: map[string]string{"script": "x"},
	})
	var sresp api.SubmitJobResponse
	_ = json.Unmarshal(body, &sresp)
	jobID := sresp.Job.JobID

	// Claim
	resp, body := httpDo(t, ts, "POST",
		api.Prefix+"/runners/r-1/claim",
		api.ClaimJobRequest{Kind: "simple"})
	if resp.StatusCode != 200 {
		t.Fatalf("claim status: %d body=%s", resp.StatusCode, body)
	}
	var cresp api.ClaimJobResponse
	_ = json.Unmarshal(body, &cresp)
	if cresp.Job == nil || cresp.Job.JobID != jobID {
		t.Fatalf("claim: %+v", cresp)
	}
	if cresp.Job.Status != "RUNNING" {
		t.Fatalf("status: %s", cresp.Job.Status)
	}

	// Update running details
	resp, _ = httpDo(t, ts, "PATCH",
		api.Prefix+"/runners/r-1/jobs/"+jobID+"/running",
		api.RunningDetailsRequest{Details: map[string]string{"pid": "9999"}})
	if resp.StatusCode != 204 {
		t.Fatalf("update running: %d", resp.StatusCode)
	}

	// End with success
	resp, _ = httpDo(t, ts, "POST",
		api.Prefix+"/runners/r-1/jobs/"+jobID+"/end",
		api.EndJobRequest{ReturnCode: 0})
	if resp.StatusCode != 204 {
		t.Fatalf("end: %d", resp.StatusCode)
	}

	// Verify final state
	resp, body = httpDo(t, ts, "GET", api.Prefix+"/jobs/"+jobID, nil)
	var jresp api.JobResponse
	_ = json.Unmarshal(body, &jresp)
	if jresp.Job.Status != "SUCCESS" {
		t.Fatalf("final status: %s", jresp.Job.Status)
	}
	if jresp.Job.RunningDetails["pid"] != "9999" {
		t.Fatalf("pid not persisted: %+v", jresp.Job.RunningDetails)
	}
}

func TestRunnerProxyRoundTrip(t *testing.T) {
	ts, _ := newTestServer(t)
	_, body := httpDo(t, ts, "POST", api.Prefix+api.RouteJobs, api.SubmitJobRequest{
		Details: map[string]string{"script": "x"},
	})
	var sresp api.SubmitJobResponse
	_ = json.Unmarshal(body, &sresp)
	jobID := sresp.Job.JobID

	// Claim
	_, _ = httpDo(t, ts, "POST", api.Prefix+"/runners/r-2/claim",
		api.ClaimJobRequest{Kind: "slurm"})

	// Mark proxied
	resp, _ := httpDo(t, ts, "POST",
		api.Prefix+"/runners/r-2/jobs/"+jobID+"/proxy",
		api.ProxyJobRequest{RunningDetails: map[string]string{"slurm_job_id": "1234"}})
	if resp.StatusCode != 204 {
		t.Fatalf("proxy: %d", resp.StatusCode)
	}

	resp, body = httpDo(t, ts, "GET", api.Prefix+"/jobs/"+jobID, nil)
	var jresp api.JobResponse
	_ = json.Unmarshal(body, &jresp)
	if jresp.Job.Status != "PROXYQUEUED" {
		t.Fatalf("status after proxy: %s", jresp.Job.Status)
	}

	// End proxied
	now := time.Now().UTC()
	resp, _ = httpDo(t, ts, "POST",
		api.Prefix+"/runners/r-2/jobs/"+jobID+"/proxy-end",
		api.EndProxyRequest{
			Status:     "SUCCESS",
			StartTime:  &now,
			EndTime:    &now,
			ReturnCode: 0,
		})
	if resp.StatusCode != 204 {
		t.Fatalf("proxy-end: %d", resp.StatusCode)
	}
	resp, body = httpDo(t, ts, "GET", api.Prefix+"/jobs/"+jobID, nil)
	_ = json.Unmarshal(body, &jresp)
	if jresp.Job.Status != "SUCCESS" {
		t.Fatalf("final status: %s", jresp.Job.Status)
	}
}

// --- Unix-socket end-to-end ------------------------------------------

func TestServeUnixSocket(t *testing.T) {
	dir := testsupport.ShortSockDir(t)
	sockPath := filepath.Join(dir, "batchq.sock")
	dbPath := filepath.Join(dir, "batchq.db")

	st, err := storage.Open(context.Background(), dbPath, storage.Options{})
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	svc := service.New(st)
	srv, err := New(svc, Options{Listen: "unix://" + sockPath})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ctx) }()

	// Wait briefly for the socket file to appear.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := net.Dial("unix", sockPath); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", sockPath)
			},
		},
	}

	resp, err := client.Get("http://batchq/healthz")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}

	cancel()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("serve err: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not exit after context cancel")
	}

	if _, err := os.Stat(sockPath); err == nil {
		t.Errorf("socket not cleaned up: %s", sockPath)
	}
}
