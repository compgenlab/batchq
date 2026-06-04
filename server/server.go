// Package server runs the batchq HTTP REST API. It owns the storage
// handle and listens on a unix socket. Clients (CLI, runner, web UI)
// drive it via the API contract in package api. Network exposure is the
// reverse proxy's job — batchq itself never binds a TCP port directly.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/mbreese/batchq/api"
	"github.com/mbreese/batchq/service"
	"github.com/mbreese/batchq/support"
)

// ErrAlreadyRunning is returned by Serve when another batchq server is
// already listening on Options.Listen. Callers can check this to
// distinguish "another instance is running" from real listener failures.
//
// Detection is done at bind time: if the socket file exists and a
// connect probe succeeds, we treat the existing server as authoritative
// and refuse to start. Only when the probe fails (ECONNREFUSED, stale
// socket from a crashed process) do we unlink and retry the bind.
var ErrAlreadyRunning = errors.New("server: another instance is already listening")

// Options configures a Server.
type Options struct {
	// Listen is the listener URL. Only "unix:///path/to/sock" is supported
	// today; native HTTPS (https://) is reserved for a future phase.
	// Required.
	Listen string

	// SocketMode is the unix-socket file mode. Defaults to 0600 (owner-only).
	SocketMode os.FileMode

	// ReadTimeout / WriteTimeout bound individual HTTP requests. Defaults
	// to 2 minutes each, matching the v1 web server.
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	// IdleTimeout, if > 0, makes the server shut itself down once no
	// request has been received in this long AND no requests are in
	// flight. Zero (default) disables idle shutdown.
	IdleTimeout time.Duration

	// IdleCheckInterval is how often the idle monitor wakes up. Defaults
	// to min(IdleTimeout/4, 30s). Only matters when IdleTimeout > 0.
	IdleCheckInterval time.Duration

	// OnIdleShutdown is called (in a goroutine) just before the idle
	// monitor triggers shutdown. Useful for tests and operator logging.
	OnIdleShutdown func()

	// OwnershipCheckInterval is how often the server dials its own
	// unix socket to confirm that the bound path still leads to this
	// process. Mismatch (different instance_id) or any dial failure
	// triggers Shutdown — catches the case where another server has
	// taken over the path. Defaults to 30s. Zero disables.
	OwnershipCheckInterval time.Duration

	// AuthToken, if non-empty, requires every API request (except the
	// health check) to carry `Authorization: Bearer <AuthToken>`. Empty
	// (the default) disables in-band auth — the unix socket's filesystem
	// permissions are then the only access control.
	AuthToken string
}

// Server is a long-lived HTTP server over the batchq REST API.
type Server struct {
	svc  *service.Service
	opts Options

	// instanceID is a per-process UUID exposed via GET /healthz. The
	// ownership monitor self-dials and compares this value; mismatch
	// means our socket path now leads to a different process.
	instanceID string

	httpSrv *http.Server

	mu         sync.Mutex
	listener   net.Listener
	socketPath string // set if listening on a unix socket; cleaned up on Shutdown

	// lastActivityNanos is touched on every request (in or out) so the
	// idle monitor can decide when to shut down. Initialized to "now" at
	// Serve start so a freshly-spawned server gets a full grace period
	// before any client connects.
	lastActivityNanos atomic.Int64
	inFlight          atomic.Int64
}

// New wires a Service into an HTTP handler tree but does not start
// listening. Call Serve to bind the socket and accept requests.
func New(svc *service.Service, opts Options) (*Server, error) {
	if svc == nil {
		return nil, errors.New("server: nil service")
	}
	if opts.Listen == "" {
		return nil, errors.New("server: empty Listen")
	}
	if opts.SocketMode == 0 {
		opts.SocketMode = 0o600
	}
	if opts.ReadTimeout == 0 {
		opts.ReadTimeout = 2 * time.Minute
	}
	if opts.WriteTimeout == 0 {
		opts.WriteTimeout = 2 * time.Minute
	}

	if opts.OwnershipCheckInterval == 0 {
		opts.OwnershipCheckInterval = 30 * time.Second
	}

	s := &Server{
		svc:        svc,
		opts:       opts,
		instanceID: support.NewUUID(),
	}
	mux := s.routes()
	s.httpSrv = &http.Server{
		Handler:      mux,
		ReadTimeout:  opts.ReadTimeout,
		WriteTimeout: opts.WriteTimeout,
		ConnContext:  connContextWithPeerCreds,
	}
	s.lastActivityNanos.Store(time.Now().UnixNano())
	return s, nil
}

// Serve binds the listener and blocks until Shutdown is called, the
// idle monitor fires, or the listener fails. The returned error is nil
// for a clean Shutdown, non-nil otherwise. Returns ErrAlreadyRunning if
// another batchq server is already listening on Options.Listen.
func (s *Server) Serve(ctx context.Context) error {
	ln, err := s.listen()
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	// Shut down gracefully when the caller's context cancels.
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.httpSrv.Shutdown(shutdownCtx)
	}()

	// Idle monitor: only spun up if a timeout is configured.
	idleCtx, idleCancel := context.WithCancel(context.Background())
	defer idleCancel()
	if s.opts.IdleTimeout > 0 {
		go s.runIdleMonitor(idleCtx)
	}

	// Ownership monitor: self-dials the bound unix socket periodically
	// and shuts down if the path leads somewhere else.
	ownerCtx, ownerCancel := context.WithCancel(context.Background())
	defer ownerCancel()
	if s.opts.OwnershipCheckInterval > 0 && s.SocketPath() != "" {
		go s.runOwnershipMonitor(ownerCtx)
	}

	serveErr := s.httpSrv.Serve(ln)
	s.cleanupSocket()
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return serveErr
	}
	return nil
}

// Addr returns the bound address, useful in tests where Listen was given
// as "unix://" with an empty path (we picked a temp path).
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// SocketPath returns the unix socket path if we are listening on one, else "".
func (s *Server) SocketPath() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.socketPath
}

// listen parses Options.Listen and binds the right kind of listener.
//
// Only unix:// is supported today. Direct TCP exposure of the REST API
// (with or without TLS) is intentionally not implemented: operators who
// need network access put a reverse proxy (nginx, Caddy, ...) in front
// of a unix socket. When native TCP+TLS support lands later it will use
// the https:// scheme here.
func (s *Server) listen() (net.Listener, error) {
	u, err := url.Parse(s.opts.Listen)
	if err != nil {
		return nil, fmt.Errorf("server: parse listen URL: %w", err)
	}
	switch u.Scheme {
	case "unix":
		return s.listenUnix(u.Path)
	default:
		return nil, fmt.Errorf("server: unsupported listen scheme %q (only unix:// is supported)", u.Scheme)
	}
}

// listenUnix binds the unix socket at path, using the socket itself as
// the single-instance election mechanism. The flow:
//
//   1. Try bind().
//   2. On EADDRINUSE, probe the path with a short Dial. If the probe
//      succeeds, another batchq is alive → return ErrAlreadyRunning.
//   3. If the probe fails (ECONNREFUSED, ENOENT race), the file is a
//      stale leftover from a crashed process — unlink it and retry bind
//      exactly once.
//
// We never unconditionally unlink before binding: that's the move that
// would let a racing start clobber a live socket. The narrow window
// where two simultaneous recoveries could both decide a socket is
// stale is acceptable — the loser's second bind still fails with
// EADDRINUSE and the process exits cleanly.
func (s *Server) listenUnix(path string) (net.Listener, error) {
	if path == "" {
		return nil, errors.New("server: unix:// URL has empty path")
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("server: mkdir socket dir: %w", err)
		}
	}

	ln, err := net.Listen("unix", path)
	if err != nil {
		if !isAddrInUse(err) {
			return nil, fmt.Errorf("server: listen unix: %w", err)
		}
		if socketIsLive(path) {
			return nil, ErrAlreadyRunning
		}
		// Stale socket from a crashed instance — best-effort unlink,
		// then retry exactly once. A second EADDRINUSE means we lost a
		// recovery race; surface that as ErrAlreadyRunning since the
		// winner is now serving on the same path.
		_ = os.Remove(path)
		ln, err = net.Listen("unix", path)
		if err != nil {
			if isAddrInUse(err) {
				return nil, ErrAlreadyRunning
			}
			return nil, fmt.Errorf("server: listen unix: %w", err)
		}
	}
	if err := os.Chmod(path, s.opts.SocketMode); err != nil {
		ln.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("server: chmod socket: %w", err)
	}
	s.mu.Lock()
	s.socketPath = path
	s.mu.Unlock()
	return ln, nil
}

// isAddrInUse reports whether err is EADDRINUSE (wrapped however the
// net package presents it).
func isAddrInUse(err error) bool {
	return errors.Is(err, syscall.EADDRINUSE)
}

// socketIsLive probes path with a short connect. Success means a
// process is accepting connections — definitionally alive. Any error
// (ECONNREFUSED on a stale socket file, ENOENT on a vanished path) is
// treated as "not live".
func socketIsLive(path string) bool {
	conn, err := net.DialTimeout("unix", path, 200*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}


func (s *Server) cleanupSocket() {
	s.mu.Lock()
	path := s.socketPath
	s.socketPath = ""
	s.mu.Unlock()
	if path != "" {
		_ = os.Remove(path)
	}
}

// Shutdown stops accepting new connections, waits for in-flight requests
// to finish (up to the given context's deadline), and unlinks the socket.
func (s *Server) Shutdown(ctx context.Context) error {
	err := s.httpSrv.Shutdown(ctx)
	s.cleanupSocket()
	return err
}

// routes builds the http.ServeMux for the API. It is split out so tests
// can mount it on their own listener without the unix-socket setup.
func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()

	// Health probes live at the root (not under /api/vN) so a reverse
	// proxy can health-check without knowing the API version.
	mux.HandleFunc("GET "+api.RouteHealth, s.handleHealth)

	p := api.Prefix

	mux.HandleFunc("POST "+p+api.RouteJobs, s.handleSubmitJob)
	mux.HandleFunc("POST "+p+api.RouteJobsArray, s.handleSubmitArray)
	mux.HandleFunc("POST "+p+api.RouteArrayCancel, s.handleCancelArray)
	mux.HandleFunc("POST "+p+api.RouteArrayHold, s.handleHoldArray)
	mux.HandleFunc("POST "+p+api.RouteArrayRelease, s.handleReleaseArray)
	mux.HandleFunc("GET "+p+api.RouteJobs, s.handleListJobs)
	mux.HandleFunc("GET "+p+api.RouteJobsByID, s.handleGetJob)
	mux.HandleFunc("DELETE "+p+api.RouteJobsByID, s.handleCancelJob)
	mux.HandleFunc("GET "+p+api.RouteJobDependents, s.handleJobDependents)
	mux.HandleFunc("POST "+p+api.RouteJobHold, s.handleHoldJob)
	mux.HandleFunc("POST "+p+api.RouteJobRelease, s.handleReleaseJob)
	mux.HandleFunc("POST "+p+api.RouteJobPriority, s.handlePriority)
	mux.HandleFunc("POST "+p+api.RouteJobCleanup, s.handleCleanupJob)

	mux.HandleFunc("GET "+p+api.RouteQueue, s.handleQueue)
	mux.HandleFunc("GET "+p+api.RouteQueueCounts, s.handleQueueCounts)

	mux.HandleFunc("POST "+p+api.RouteRunnerClaim, s.handleClaim)
	mux.HandleFunc("POST "+p+api.RouteRunnerClaimArray, s.handleClaimArray)
	mux.HandleFunc("POST "+p+api.RouteRunnerJobProxy, s.handleMarkProxied)
	mux.HandleFunc("PATCH "+p+api.RouteRunnerJobRunning, s.handleUpdateRunning)
	mux.HandleFunc("POST "+p+api.RouteRunnerJobEnd, s.handleEndJob)
	mux.HandleFunc("POST "+p+api.RouteRunnerJobProxyEnd, s.handleEndProxied)

	mux.HandleFunc("POST "+p+api.RouteShutdown, s.handleShutdown)

	// withAuth sits outside withActivity so unauthenticated requests
	// neither reset the idle timer nor count as in-flight.
	return s.withVersionHeader(s.withAuth(s.withActivity(mux)))
}

// withVersionHeader stamps every response with the API version header so
// clients can detect breaking changes.
func (s *Server) withVersionHeader(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(api.HeaderVersion, api.Version)
		next.ServeHTTP(w, r)
	})
}

// withActivity tracks last-activity and in-flight counts so the idle
// monitor can decide when to shut down. Health checks from real clients
// count — they reset the idle timer of a freshly-spawned server while
// clients dial. Requests carrying HeaderInternalOwner are the server's
// own ownership-monitor self-dials; they skip activity tracking so the
// monitor doesn't keep an otherwise-idle server alive forever.
func (s *Server) withActivity(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(api.HeaderInternalOwner) != "" {
			next.ServeHTTP(w, r)
			return
		}
		s.inFlight.Add(1)
		s.lastActivityNanos.Store(time.Now().UnixNano())
		defer func() {
			s.lastActivityNanos.Store(time.Now().UnixNano())
			s.inFlight.Add(-1)
		}()
		next.ServeHTTP(w, r)
	})
}

// runIdleMonitor ticks at IdleCheckInterval. When no requests are
// in-flight and the elapsed time since the last activity exceeds
// IdleTimeout, it triggers httpSrv.Shutdown — which unblocks Serve and
// drives the normal cleanup path.
func (s *Server) runIdleMonitor(ctx context.Context) {
	interval := s.opts.IdleCheckInterval
	if interval <= 0 {
		interval = s.opts.IdleTimeout / 4
		if interval > 30*time.Second {
			interval = 30 * time.Second
		}
		if interval < 100*time.Millisecond {
			interval = 100 * time.Millisecond
		}
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if s.inFlight.Load() > 0 {
				continue
			}
			last := time.Unix(0, s.lastActivityNanos.Load())
			if time.Since(last) < s.opts.IdleTimeout {
				continue
			}
			if s.opts.OnIdleShutdown != nil {
				go s.opts.OnIdleShutdown()
			}
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := s.httpSrv.Shutdown(shutdownCtx); err != nil {
				log.Printf("server: idle shutdown: %v", err)
			}
			cancel()
			return
		}
	}
}

// runOwnershipMonitor periodically dials the server's own unix socket
// and compares the InstanceID in the response to its own. A mismatch
// (someone else now answers at our path) or any failure (the path is
// gone) triggers Shutdown. The self-dial carries HeaderInternalOwner
// so withActivity skips it — these pings must not keep an idle server
// alive.
//
// Only runs when Options.OwnershipCheckInterval > 0 and we're bound to
// a unix socket. https:// listeners (future) don't have a meaningful
// "path → process" binding to monitor.
func (s *Server) runOwnershipMonitor(ctx context.Context) {
	sockPath := s.SocketPath()
	if sockPath == "" {
		return
	}
	// DisableKeepAlives is critical: a cached unix connection points
	// at the inode we got at dial time, not at whatever the path now
	// resolves to. If the socket file is unlinked (or replaced by a
	// new server), the cached connection would still hit our own
	// listener, and ownership would always confirm — defeating the
	// whole point of this monitor. Forcing a fresh dial each tick
	// makes the path-resolution actually exercise.
	httpClient := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			DialContext: func(dialCtx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(dialCtx, "unix", sockPath)
			},
			DisableKeepAlives: true,
		},
	}
	defer httpClient.CloseIdleConnections()

	ticker := time.NewTicker(s.opts.OwnershipCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !s.confirmOwnership(ctx, httpClient) {
				// If a shutdown is already in flight (idle monitor or
				// caller-context cancel), the cleanupSocket path may
				// have unlinked the socket already — this "failure"
				// is just the tail end of a normal shutdown, not an
				// orphan. Shutdown is idempotent so logging the line
				// is harmless but noisy in that case.
				log.Printf("server: ownership check failed (path %s no longer leads to instance %s); shutting down", sockPath, s.instanceID)
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := s.httpSrv.Shutdown(shutdownCtx); err != nil {
					log.Printf("server: ownership shutdown: %v", err)
				}
				cancel()
				return
			}
		}
	}
}

// confirmOwnership returns true iff dialing our own socket path
// reaches a server that reports our InstanceID. Any non-200, any
// network/JSON error, or a mismatched ID returns false.
func (s *Server) confirmOwnership(ctx context.Context, httpClient *http.Client) bool {
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, "http://batchq/healthz", nil)
	if err != nil {
		return false
	}
	req.Header.Set(api.HeaderInternalOwner, s.instanceID)
	resp, err := httpClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	var hr api.HealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&hr); err != nil {
		return false
	}
	return hr.InstanceID == s.instanceID
}
