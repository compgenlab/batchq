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

	// OnShutdown, if set, is called exactly once during shutdown to release
	// the storage handle (typically Storage.Close). It is sequenced against
	// the socket so the unix socket — the single-instance lock — is unlinked
	// only AFTER the database is closed: a freshly-autospawned server cannot
	// bind the socket (and therefore cannot open the DB) until the dying
	// server has dropped it. This closes the idle-handoff window that
	// otherwise lets two servers touch the DB at once and report SQLITE_BUSY.
	OnShutdown func() error
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

	// draining is set true once shutdown commits. While draining, the
	// activity middleware rejects new requests with 503 + HeaderDraining
	// (they never touch the closing DB) so the client can reconnect and
	// retry. Paired with inFlight via a store-then-load / add-then-load
	// handshake so a request and a committing shutdown can't both proceed.
	draining atomic.Bool

	// onShutdown releases storage; see Options.OnShutdown. shutdownMu guards
	// shutdownStarted so the three shutdown triggers (idle, context-cancel,
	// ownership) and the admin route run the ordered teardown at most once.
	onShutdown      func() error
	shutdownMu      sync.Mutex
	shutdownStarted bool
}

// errCullAborted is returned by shutdown when an idle cull is abandoned
// because a request arrived between the idle check and the drain commit.
// The idle monitor treats it as "keep serving", not a failure.
var errCullAborted = errors.New("server: idle cull aborted (request in flight)")

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
		opts.OwnershipCheckInterval = 3 * time.Second
	}

	s := &Server{
		svc:        svc,
		opts:       opts,
		instanceID: support.NewUUID(),
		onShutdown: opts.OnShutdown,
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
	ln, socketPath, err := Elect(s.opts.Listen, s.opts.SocketMode)
	if err != nil {
		return err
	}
	return s.ServeListener(ctx, ln, socketPath)
}

// ServeListener serves on a listener already acquired via Elect, skipping the
// single-instance election. socketPath (empty for tcp://) is recorded so the
// ownership monitor and socket cleanup work. It blocks until shutdown, like
// Serve. Splitting the election out lets cmd/server.go win the election BEFORE
// opening the database, so a losing server never touches the DB file.
func (s *Server) ServeListener(ctx context.Context, ln net.Listener, socketPath string) error {
	s.mu.Lock()
	s.listener = ln
	s.socketPath = socketPath
	s.mu.Unlock()

	// Shut down gracefully when the caller's context cancels.
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.shutdown(shutdownCtx, "context-cancel", false)
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

// Elect acquires the single-instance election token (the listener) for the
// given Listen URL WITHOUT constructing a Server or opening any storage. It
// returns ErrAlreadyRunning if another batchq server already owns the address.
// socketPath is the bound unix socket path (empty for tcp://).
//
// Doing the election before opening the database is the whole point: a server
// that loses the election must never touch the DB file, otherwise concurrently
// autospawned servers contend on it and the winner sees SQLITE_BUSY.
//
// Two schemes are supported:
//   - unix:///path/to/sock — the default; access is gated by the socket's
//     filesystem permissions and kernel peer credentials.
//   - tcp://host:port — a plain-HTTP TCP port, for containerized /
//     orchestrated deployments (Docker, k8s) where a host-path socket is
//     awkward. batchq still never terminates TLS — front a TCP port with a
//     reverse proxy for network exposure. TCP carries no peer credentials,
//     so a TCP-bound server should set [server] token (see cmd/server.go,
//     which warns when it isn't).
func Elect(listen string, socketMode os.FileMode) (ln net.Listener, socketPath string, err error) {
	u, err := url.Parse(listen)
	if err != nil {
		return nil, "", fmt.Errorf("server: parse listen URL: %w", err)
	}
	switch u.Scheme {
	case "unix":
		if socketMode == 0 {
			socketMode = 0o600
		}
		ln, err = electUnix(u.Path, socketMode)
		if err != nil {
			return nil, "", err
		}
		return ln, u.Path, nil
	case "tcp":
		ln, err = electTCP(u.Host)
		if err != nil {
			return nil, "", err
		}
		return ln, "", nil
	default:
		return nil, "", fmt.Errorf("server: unsupported listen scheme %q (want unix:// or tcp://)", u.Scheme)
	}
}

// listen is a thin compatibility shim over Elect, kept so existing tests that
// call srv.listen() directly still compile. Production code uses Elect.
func (s *Server) listen() (net.Listener, error) {
	ln, _, err := Elect(s.opts.Listen, s.opts.SocketMode)
	return ln, err
}

// electTCP binds a plain-HTTP TCP listener at host:port. Unlike the unix
// socket there is no election token or stale-file recovery: a port already
// in use means another server owns it, so EADDRINUSE maps straight to
// ErrAlreadyRunning.
func electTCP(hostport string) (net.Listener, error) {
	if hostport == "" {
		return nil, errors.New("server: tcp:// URL has empty host:port")
	}
	ln, err := net.Listen("tcp", hostport)
	if err != nil {
		if isAddrInUse(err) {
			return nil, ErrAlreadyRunning
		}
		return nil, fmt.Errorf("server: listen tcp: %w", err)
	}
	return ln, nil
}

// electUnix binds the unix socket at path, using the socket itself as
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
func electUnix(path string, socketMode os.FileMode) (net.Listener, error) {
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
	if err := os.Chmod(path, socketMode); err != nil {
		ln.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("server: chmod socket: %w", err)
	}
	// Keep the socket file across listener.Close(): Go's UnixListener unlinks
	// it by default, which would drop the lock at the START of shutdown —
	// before the DB is closed. We make cleanupSocket the sole unlink point so
	// the socket (the lock) is released only AFTER OnShutdown closes the DB.
	// A lingering socket from a crash is handled by the stale-socket recovery
	// above.
	if ul, ok := ln.(*net.UnixListener); ok {
		ul.SetUnlinkOnClose(false)
	}
	return ln, nil
}

// isAddrInUse reports whether err is EADDRINUSE (wrapped however the
// net package presents it).
func isAddrInUse(err error) bool {
	return errors.Is(err, syscall.EADDRINUSE)
}

// socketIsLive probes path with a short connect, retried a few times. Success
// on ANY attempt means a process is accepting connections — definitionally
// alive. We only conclude "not live" (a stale file from a crashed process)
// after every attempt fails, so a live-but-momentarily-unreachable server (a
// loaded node, a briefly-full accept backlog) is never mistaken for dead and
// have its socket unlinked. The common case — a socket that is actually live —
// returns true on the first try with no added latency.
func socketIsLive(path string) bool {
	return probeSocketLive(path, 3, 500*time.Millisecond, 100*time.Millisecond)
}

// probeSocketLive dials path up to attempts times (timeout each, gap between
// tries), returning true as soon as one connect succeeds.
func probeSocketLive(path string, attempts int, timeout, gap time.Duration) bool {
	for i := 0; i < attempts; i++ {
		conn, err := net.DialTimeout("unix", path, timeout)
		if err == nil {
			_ = conn.Close()
			return true
		}
		if i < attempts-1 {
			time.Sleep(gap)
		}
	}
	return false
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
// to finish (up to the given context's deadline), closes the database, and
// unlinks the socket — in that order.
func (s *Server) Shutdown(ctx context.Context) error {
	err := s.shutdown(ctx, "request", false)
	s.cleanupSocket()
	return err
}

// shutdown runs the ordered teardown, guaranteeing the unix socket (the
// single-instance lock) is released only AFTER the database is closed:
//
//   - idle (nothing in flight): close the DB while the listener is still
//     bound, then stop accepting. No other process can take the socket — and
//     thus open the DB — until cleanupSocket unlinks it, which happens after
//     Serve returns. Hard guarantee, zero writer overlap.
//   - busy (a forced shutdown with requests in flight): drain first so
//     in-flight handlers can finish their DB work, then close the DB. The
//     listener closes during the drain, but the socket FILE lingers
//     (SetUnlinkOnClose(false)) until cleanupSocket runs after the DB is
//     closed, and a racing opener still hits EADDRINUSE + the ~1s liveness
//     probe — so this path stays safe too.
//
// abortIfBusy (idle monitor only) abandons the cull if a request slipped in
// between the idle check and the draining commit, returning errCullAborted.
// shutdownStarted makes the committed teardown run at most once.
func (s *Server) shutdown(ctx context.Context, reason string, abortIfBusy bool) error {
	s.shutdownMu.Lock()
	defer s.shutdownMu.Unlock()
	if s.shutdownStarted {
		return nil
	}
	// Set draining BEFORE reading inFlight. The activity middleware does the
	// mirror (inFlight++ then read draining), so a request and this commit
	// can never both decide to proceed against the DB.
	s.draining.Store(true)
	busy := s.inFlight.Load() > 0
	if busy && abortIfBusy {
		s.draining.Store(false)
		return errCullAborted
	}
	s.shutdownStarted = true

	var err error
	if busy {
		// In-flight handlers still need the DB: drain, then close.
		err = s.httpSrv.Shutdown(ctx)
		s.closeStorage(reason)
	} else {
		// Nothing in flight: close the DB while still bound, then stop
		// accepting. New arrivals get 503 + HeaderDraining via the gate.
		s.closeStorage(reason)
		err = s.httpSrv.Shutdown(ctx)
	}
	return err
}

// closeStorage invokes the OnShutdown hook (Storage.Close) at most once via
// shutdownStarted. Logged, not fatal — we are tearing down regardless.
func (s *Server) closeStorage(reason string) {
	if s.onShutdown == nil {
		return
	}
	if err := s.onShutdown(); err != nil {
		log.Printf("server: close storage on %s shutdown: %v", reason, err)
	}
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
		// Add to inFlight BEFORE reading draining (shutdown does the mirror:
		// store draining, then read inFlight). One side always observes the
		// other, so a request never reaches the DB after a committed cull
		// closed it. A draining server rejects with 503 + HeaderDraining; the
		// request did no work, so the client safely reconnects and retries.
		s.inFlight.Add(1)
		if s.draining.Load() {
			s.inFlight.Add(-1)
			w.Header().Set(api.HeaderDraining, "1")
			http.Error(w, "batchq: server is shutting down, retry", http.StatusServiceUnavailable)
			return
		}
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
			err := s.shutdown(shutdownCtx, "idle", true)
			cancel()
			if errors.Is(err, errCullAborted) {
				// A request arrived between the idle check and the drain
				// commit — keep serving and re-evaluate next tick.
				continue
			}
			if err != nil {
				log.Printf("server: idle shutdown: %v", err)
			}
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
	// Require several CONSECUTIVE failures before stepping down. A single
	// failed self-dial under load (slow accept, transient hiccup) must not
	// shut down a healthy server — that would itself create churn. A genuine
	// takeover (a different instance now answers our path) fails every check,
	// so it still evicts quickly: failThreshold × OwnershipCheckInterval.
	const failThreshold = 3
	consecutiveFails := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if s.confirmOwnership(ctx, httpClient) {
				consecutiveFails = 0
				continue
			}
			consecutiveFails++
			if consecutiveFails < failThreshold {
				continue
			}
			// If a shutdown is already in flight (idle monitor or
			// caller-context cancel), the cleanupSocket path may
			// have unlinked the socket already — this "failure"
			// is just the tail end of a normal shutdown, not an
			// orphan. Shutdown is idempotent so logging the line
			// is harmless but noisy in that case.
			log.Printf("server: ownership check failed %d× (path %s no longer leads to instance %s); shutting down", consecutiveFails, sockPath, s.instanceID)
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := s.shutdown(shutdownCtx, "ownership", false); err != nil {
				log.Printf("server: ownership shutdown: %v", err)
			}
			cancel()
			return
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
