// Package client is the Go REST client for the batchq server. Every other
// in-repo component that talks to the server — CLI commands, runners, the
// web UI — uses Client. The wire format is defined in package api.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/mbreese/batchq/api"
)

// Typed errors. Callers can do `errors.Is(err, ErrNotFound)` etc.
var (
	ErrNotFound     = errors.New("client: not found")
	ErrInvalidState = errors.New("client: invalid state")
	ErrBadRequest   = errors.New("client: bad request")
	ErrUnauthorized = errors.New("client: unauthorized")
	ErrServerError  = errors.New("client: server error")
)

// HTTPError wraps a non-2xx response. It implements Is for the typed
// sentinels above so `errors.Is(err, ErrNotFound)` works.
type HTTPError struct {
	StatusCode int
	Body       string
	APIError   *api.ErrorResponse
}

func (e *HTTPError) Error() string {
	if e.APIError != nil && e.APIError.Error != "" {
		return fmt.Sprintf("batchq: %d: %s", e.StatusCode, e.APIError.Error)
	}
	return fmt.Sprintf("batchq: %d: %s", e.StatusCode, strings.TrimSpace(e.Body))
}

func (e *HTTPError) Is(target error) bool {
	switch target {
	case ErrNotFound:
		return e.StatusCode == http.StatusNotFound
	case ErrInvalidState:
		return e.StatusCode == http.StatusConflict
	case ErrBadRequest:
		return e.StatusCode == http.StatusBadRequest
	case ErrUnauthorized:
		return e.StatusCode == http.StatusUnauthorized
	case ErrServerError:
		return e.StatusCode >= 500
	}
	return false
}

// Options configures a Client at construction time.
type Options struct {
	// URL of the server. One of:
	//   unix:///path/to/sock  — local server over a unix domain socket
	//   tcp://host:port       — server over a plain-HTTP TCP port (e.g. a
	//                            containerized server reached over a
	//                            trusted/Docker network)
	//   https://host:port     — remote HTTPS REST API (typically a
	//                            reverse proxy terminating TLS in front
	//                            of a batchq server)
	URL string

	// Token is the bearer token sent in the Authorization header. It is
	// attached on every transport when set — over https for a remote
	// server, and over the unix socket too so a local client/runner can
	// authenticate to a server configured with a shared `[server] token`.
	// Empty token means no header is sent.
	Token string

	// Timeout for individual requests. Default 30s. Pass 0 to mean "no
	// per-request timeout" — the caller's context governs.
	Timeout time.Duration

	// UserAgent is the User-Agent header value. Default "batchq-client/2".
	UserAgent string
}

// Client is a REST client for the batchq server. It is safe for concurrent
// use.
type Client struct {
	opts   Options
	httpC  *http.Client
	base   string // URL prefix to prepend to every request (e.g. "http://batchq")
	socket string // unix socket path, if applicable; empty for https
}

// Dial parses the URL and returns a Client. No connection is made until
// the first request — Dial is a constructor, not a network op.
func Dial(u string) (*Client, error) {
	return DialWithOptions(Options{URL: u})
}

// DialWithOptions is Dial with full Options control.
func DialWithOptions(opts Options) (*Client, error) {
	if opts.URL == "" {
		return nil, errors.New("client: empty URL")
	}
	if opts.Timeout == 0 {
		opts.Timeout = 30 * time.Second
	}
	if opts.UserAgent == "" {
		opts.UserAgent = "batchq-client/2"
	}

	parsed, err := url.Parse(opts.URL)
	if err != nil {
		return nil, fmt.Errorf("client: parse URL: %w", err)
	}

	c := &Client{opts: opts}
	switch parsed.Scheme {
	case "unix":
		c.socket = parsed.Path
		if c.socket == "" {
			return nil, errors.New("client: unix:// URL has empty path")
		}
		c.base = "http://batchq" // bogus host; the dialer ignores it
		c.httpC = &http.Client{
			Timeout: opts.Timeout,
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "unix", c.socket)
				},
			},
		}
	case "tcp", "http":
		// Plain HTTP over a TCP port — a containerized server on a trusted
		// network. The default transport dials host:port directly. A
		// mount-point subpath is honored the same way as https.
		base := "http://" + parsed.Host
		if p := strings.TrimRight(parsed.Path, "/"); p != "" {
			base += p
		}
		c.base = base
		c.httpC = &http.Client{Timeout: opts.Timeout}
	case "https":
		// Include the URL path as a mount-point prefix so deployments
		// behind a reverse proxy at /some/subpath work. Trailing slashes
		// are stripped — request paths always begin with `/`.
		base := "https://" + parsed.Host
		if p := strings.TrimRight(parsed.Path, "/"); p != "" {
			base += p
		}
		c.base = base
		c.httpC = &http.Client{Timeout: opts.Timeout}
	default:
		return nil, fmt.Errorf("client: unsupported scheme %q (want unix://, tcp://, or https://)", parsed.Scheme)
	}
	return c, nil
}

// Close releases idle connections. The Client is no longer usable
// afterwards.
func (c *Client) Close() error {
	if t, ok := c.httpC.Transport.(*http.Transport); ok {
		t.CloseIdleConnections()
	}
	return nil
}

// SocketPath returns the unix socket path, or "" if not using unix.
func (c *Client) SocketPath() string { return c.socket }

// --- core HTTP plumbing ------------------------------------------------

// do performs an HTTP request. Body is JSON-marshaled if non-nil. out, if
// non-nil, is JSON-unmarshaled from the response. Non-2xx responses
// produce a *HTTPError.
func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("client: marshal body: %w", err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rdr)
	if err != nil {
		return fmt.Errorf("client: new request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.opts.Token != "" {
		req.Header.Set(api.HeaderAuthorization, "Bearer "+c.opts.Token)
	}
	req.Header.Set("User-Agent", c.opts.UserAgent)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpC.Do(req)
	if err != nil {
		return fmt.Errorf("client: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("client: read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		httpErr := &HTTPError{StatusCode: resp.StatusCode, Body: string(respBody)}
		if len(respBody) > 0 {
			var apiErr api.ErrorResponse
			if json.Unmarshal(respBody, &apiErr) == nil && apiErr.Error != "" {
				httpErr.APIError = &apiErr
			}
		}
		return httpErr
	}

	if out != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, out); err != nil {
			return fmt.Errorf("client: unmarshal response: %w", err)
		}
	}
	return nil
}

// --- Jobs --------------------------------------------------------------

func (c *Client) Health(ctx context.Context) error {
	return c.do(ctx, http.MethodGet, "/healthz", nil, nil)
}

// Shutdown asks the server to drain in-flight requests and exit. Callers
// that want "ensure no server is running" should treat a connect failure
// (no such file or connection refused) as a successful no-op — there was
// nothing to shut down.
func (c *Client) Shutdown(ctx context.Context) error {
	return c.do(ctx, http.MethodPost, api.Prefix+api.RouteShutdown, nil, nil)
}

func (c *Client) SubmitJob(ctx context.Context, req *api.SubmitJobRequest) (*api.JobDTO, error) {
	var resp api.SubmitJobResponse
	if err := c.do(ctx, http.MethodPost, api.Prefix+api.RouteJobs, req, &resp); err != nil {
		return nil, err
	}
	return resp.Job, nil
}

// SubmitArray submits a job array (one template + task indices) and returns the
// generated array id plus the persisted task jobs.
func (c *Client) SubmitArray(ctx context.Context, req *api.SubmitArrayRequest) (*api.SubmitArrayResponse, error) {
	var resp api.SubmitArrayResponse
	if err := c.do(ctx, http.MethodPost, api.Prefix+api.RouteJobsArray, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) GetJob(ctx context.Context, jobID string) (*api.JobDTO, error) {
	var resp api.JobResponse
	if err := c.do(ctx, http.MethodGet, api.Prefix+"/jobs/"+jobID, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Job, nil
}

// ListJobsOptions controls GET /jobs.
type ListJobsOptions struct {
	ShowAll      bool
	SortByStatus bool
	Statuses     []string // wire-format status names
	Query        string
	Limit        int
	Offset       int

	// Optional filters.
	RunID   string // jobs whose "run_id" detail equals this
	ArrayID string // tasks whose "array_id" detail equals this
	Output  string // jobs that list this path in output_files
	Input   string // jobs that list this path in input_files
}

func (c *Client) ListJobs(ctx context.Context, opts ListJobsOptions) ([]*api.JobDTO, error) {
	q := url.Values{}
	if opts.ShowAll {
		q.Set("all", "true")
	}
	if opts.SortByStatus {
		q.Set("sort_by_status", "true")
	}
	if opts.Query != "" {
		q.Set("q", opts.Query)
	}
	if len(opts.Statuses) > 0 {
		q.Set("status", strings.Join(opts.Statuses, ","))
	}
	if opts.Limit > 0 {
		q.Set("limit", strconv.Itoa(opts.Limit))
	}
	if opts.Offset > 0 {
		q.Set("offset", strconv.Itoa(opts.Offset))
	}
	if opts.RunID != "" {
		q.Set("run_id", opts.RunID)
	}
	if opts.ArrayID != "" {
		q.Set("array_id", opts.ArrayID)
	}
	if opts.Output != "" {
		q.Set("output", opts.Output)
	}
	if opts.Input != "" {
		q.Set("input", opts.Input)
	}
	path := api.Prefix + api.RouteJobs
	if encoded := q.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var resp api.ListJobsResponse
	if err := c.do(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Jobs, nil
}

func (c *Client) CancelJob(ctx context.Context, jobID, reason string) error {
	return c.do(ctx, http.MethodDelete, api.Prefix+"/jobs/"+jobID,
		api.CancelJobRequest{Reason: reason}, nil)
}

func (c *Client) GetJobDependents(ctx context.Context, jobID string) ([]string, error) {
	var resp api.JobDependentsResponse
	if err := c.do(ctx, http.MethodGet, api.Prefix+"/jobs/"+jobID+"/dependents", nil, &resp); err != nil {
		return nil, err
	}
	return resp.JobIDs, nil
}

func (c *Client) HoldJob(ctx context.Context, jobID string) error {
	return c.do(ctx, http.MethodPost, api.Prefix+"/jobs/"+jobID+"/hold", nil, nil)
}

func (c *Client) ReleaseJob(ctx context.Context, jobID string) error {
	return c.do(ctx, http.MethodPost, api.Prefix+"/jobs/"+jobID+"/release", nil, nil)
}

func (c *Client) AdjustJobPriority(ctx context.Context, jobID string, delta int) error {
	return c.do(ctx, http.MethodPost, api.Prefix+"/jobs/"+jobID+"/priority",
		api.PriorityRequest{Delta: delta}, nil)
}

// CancelArray/HoldArray/ReleaseArray apply the operation to every task of an
// array, returning the number of affected tasks.
func (c *Client) CancelArray(ctx context.Context, arrayID, reason string) (int, error) {
	var resp api.ArrayActionResponse
	err := c.do(ctx, http.MethodPost, api.Prefix+"/arrays/"+arrayID+"/cancel",
		api.CancelJobRequest{Reason: reason}, &resp)
	return resp.Count, err
}

func (c *Client) HoldArray(ctx context.Context, arrayID string) (int, error) {
	var resp api.ArrayActionResponse
	err := c.do(ctx, http.MethodPost, api.Prefix+"/arrays/"+arrayID+"/hold", nil, &resp)
	return resp.Count, err
}

func (c *Client) ReleaseArray(ctx context.Context, arrayID string) (int, error) {
	var resp api.ArrayActionResponse
	err := c.do(ctx, http.MethodPost, api.Prefix+"/arrays/"+arrayID+"/release", nil, &resp)
	return resp.Count, err
}

// CleanupJob removes a terminal job from storage. The server returns
// ErrInvalidState if the job is not in a terminal state.
func (c *Client) CleanupJob(ctx context.Context, jobID string) error {
	return c.do(ctx, http.MethodPost, api.Prefix+"/jobs/"+jobID+"/cleanup", nil, nil)
}

// --- Queue -------------------------------------------------------------

func (c *Client) GetQueueJobs(ctx context.Context, showAll, sortByStatus bool) ([]*api.JobDTO, error) {
	q := url.Values{}
	if showAll {
		q.Set("all", "true")
	}
	if sortByStatus {
		q.Set("sort_by_status", "true")
	}
	path := api.Prefix + api.RouteQueue
	if encoded := q.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var resp api.ListJobsResponse
	if err := c.do(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Jobs, nil
}

func (c *Client) GetJobStatusCounts(ctx context.Context, showAll bool) (map[string]int, error) {
	path := api.Prefix + api.RouteQueueCounts
	if showAll {
		path += "?all=true"
	}
	var resp api.StatusCountsResponse
	if err := c.do(ctx, http.MethodGet, path, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Counts, nil
}

// --- Runner endpoints --------------------------------------------------

// ClaimNextJob is the atomic claim primitive used by runners. The returned
// response has Job=nil if nothing was claimed; MoreEligible then signals a lost
// claim race (an immediate retry may land a job) and Blocked signals that
// queued jobs remain which don't fit this runner's limits/resources.
func (c *Client) ClaimNextJob(ctx context.Context, runnerID, kind, host string, maxProcs, maxMemMB, maxWalltimeSec int, resources map[string]string) (*api.ClaimJobResponse, error) {
	req := api.ClaimJobRequest{
		Kind:           kind,
		Host:           host,
		MaxProcs:       maxProcs,
		MaxMemoryMB:    maxMemMB,
		MaxWalltimeSec: maxWalltimeSec,
		Resources:      resources,
	}
	var resp api.ClaimJobResponse
	if err := c.do(ctx, http.MethodPost,
		api.Prefix+"/runners/"+runnerID+"/claim", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ClaimNextArrayBatch claims the next plain job or array batch for a
// batch-capable runner. maxTasks (<=0 = unbounded) caps how many tasks of one
// array are claimed together. See api.ClaimArrayResponse for the shape.
func (c *Client) ClaimNextArrayBatch(ctx context.Context, runnerID, kind, host string, maxProcs, maxMemMB, maxWalltimeSec int, resources map[string]string, maxTasks int) (*api.ClaimArrayResponse, error) {
	req := api.ClaimArrayRequest{
		Kind:           kind,
		Host:           host,
		MaxProcs:       maxProcs,
		MaxMemoryMB:    maxMemMB,
		MaxWalltimeSec: maxWalltimeSec,
		Resources:      resources,
		MaxTasks:       maxTasks,
	}
	var resp api.ClaimArrayResponse
	if err := c.do(ctx, http.MethodPost,
		api.Prefix+"/runners/"+runnerID+"/claim-array", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) MarkJobProxied(ctx context.Context, runnerID, jobID string, details map[string]string) error {
	return c.do(ctx, http.MethodPost,
		api.Prefix+"/runners/"+runnerID+"/jobs/"+jobID+"/proxy",
		api.ProxyJobRequest{RunningDetails: details}, nil)
}

func (c *Client) UpdateRunningDetails(ctx context.Context, runnerID, jobID string, details map[string]string) error {
	return c.do(ctx, http.MethodPatch,
		api.Prefix+"/runners/"+runnerID+"/jobs/"+jobID+"/running",
		api.RunningDetailsRequest{Details: details}, nil)
}

// EndJob reports the terminal state of a locally-run job. A non-empty notes
// is persisted to jobs.notes — runners pass a short reason when failing a
// job (e.g. "missing UID from job details").
func (c *Client) EndJob(ctx context.Context, runnerID, jobID string, returnCode int, notes string) error {
	return c.do(ctx, http.MethodPost,
		api.Prefix+"/runners/"+runnerID+"/jobs/"+jobID+"/end",
		api.EndJobRequest{ReturnCode: returnCode, Notes: notes}, nil)
}

// EndProxiedJob reports the terminal state of a SLURM-proxied job. status
// must be SUCCESS, FAILED, or CANCELED. A non-empty notes is persisted to
// jobs.notes (typically the SLURM-reported state name for non-success).
func (c *Client) EndProxiedJob(ctx context.Context, runnerID, jobID, status string, startTime, endTime time.Time, returnCode int, notes string) error {
	req := api.EndProxyRequest{Status: status, ReturnCode: returnCode, Notes: notes}
	if !startTime.IsZero() {
		t := startTime.UTC()
		req.StartTime = &t
	}
	if !endTime.IsZero() {
		t := endTime.UTC()
		req.EndTime = &t
	}
	return c.do(ctx, http.MethodPost,
		api.Prefix+"/runners/"+runnerID+"/jobs/"+jobID+"/proxy-end",
		req, nil)
}
