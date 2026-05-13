// Package server runs the batchq REST API. It owns the storage handle and
// listens on a unix socket or TCP address. Clients (CLI, runner, web UI)
// drive it via the API contract in package api.
package server

import (
	"context"
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
)

// ErrLockHeld is returned by Serve when another batchq server already
// holds the lock file. Callers can check this to distinguish "another
// instance is running" from real listener failures.
var ErrLockHeld = errors.New("server: lock held by another instance")

// Options configures a Server.
type Options struct {
	// Listen is the listener URL: "unix:///path/to/sock" or
	// "tcp://host:port". Required.
	Listen string

	// SocketMode is the unix-socket file mode. Defaults to 0600 (owner-only).
	SocketMode os.FileMode

	// ReadTimeout / WriteTimeout bound individual HTTP requests. Defaults
	// to 2 minutes each, matching the v1 web server.
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	// LockPath, if non-empty, is the path to a file used as an exclusive
	// flock election. Serve returns ErrLockHeld if another process holds
	// the lock. The file persists across runs; only the flock itself is
	// the lifetime signal.
	LockPath string

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
}

// Server is a long-lived HTTP server over the batchq REST API.
type Server struct {
	svc  *service.Service
	opts Options

	httpSrv *http.Server

	mu         sync.Mutex
	listener   net.Listener
	socketPath string // set if listening on a unix socket; cleaned up on Shutdown
	lockFile   *os.File

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

	s := &Server{svc: svc, opts: opts}
	mux := s.routes()
	s.httpSrv = &http.Server{
		Handler:      mux,
		ReadTimeout:  opts.ReadTimeout,
		WriteTimeout: opts.WriteTimeout,
	}
	s.lastActivityNanos.Store(time.Now().UnixNano())
	return s, nil
}

// Serve binds the listener and blocks until Shutdown is called, the
// idle monitor fires, or the listener fails. The returned error is nil
// for a clean Shutdown, non-nil otherwise. If another process already
// holds Options.LockPath, returns ErrLockHeld without touching the
// socket.
func (s *Server) Serve(ctx context.Context) error {
	if err := s.acquireLock(); err != nil {
		return err
	}

	ln, err := s.listen()
	if err != nil {
		s.releaseLock()
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

	serveErr := s.httpSrv.Serve(ln)
	s.cleanupSocket()
	s.releaseLock()
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
func (s *Server) listen() (net.Listener, error) {
	u, err := url.Parse(s.opts.Listen)
	if err != nil {
		return nil, fmt.Errorf("server: parse listen URL: %w", err)
	}
	switch u.Scheme {
	case "unix":
		return s.listenUnix(u.Path)
	case "tcp":
		return s.listenTCP(u.Host)
	default:
		return nil, fmt.Errorf("server: unsupported scheme %q", u.Scheme)
	}
}

func (s *Server) listenUnix(path string) (net.Listener, error) {
	if path == "" {
		return nil, errors.New("server: unix:// URL has empty path")
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("server: mkdir socket dir: %w", err)
		}
	}
	// Best-effort unlink any stale socket file from a previous crash. A
	// running server holding the path will be re-detected via flock in
	// Phase 8; for now this is sufficient.
	_ = os.Remove(path)

	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("server: listen unix: %w", err)
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

func (s *Server) listenTCP(addr string) (net.Listener, error) {
	if addr == "" {
		return nil, errors.New("server: tcp:// URL missing host:port")
	}
	// TCP listening currently has no auth; this is gated by config policy
	// at the cmd layer (which will refuse to start a TCP listener without a
	// master key once token auth lands). The Server itself accepts it so
	// tests can exercise the wire path.
	return net.Listen("tcp", addr)
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
	mux.HandleFunc("POST "+p+api.RouteRunnerJobProxy, s.handleMarkProxied)
	mux.HandleFunc("PATCH "+p+api.RouteRunnerJobRunning, s.handleUpdateRunning)
	mux.HandleFunc("POST "+p+api.RouteRunnerJobEnd, s.handleEndJob)
	mux.HandleFunc("POST "+p+api.RouteRunnerJobProxyEnd, s.handleEndProxied)

	return s.withVersionHeader(s.withActivity(mux))
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
// monitor can decide when to shut down. Health checks count too — they
// reset the idle timer of a freshly-spawned server while clients dial.
func (s *Server) withActivity(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.inFlight.Add(1)
		s.lastActivityNanos.Store(time.Now().UnixNano())
		defer func() {
			s.lastActivityNanos.Store(time.Now().UnixNano())
			s.inFlight.Add(-1)
		}()
		next.ServeHTTP(w, r)
	})
}

// acquireLock takes an exclusive flock on Options.LockPath. The lock is
// held by keeping the *os.File open; the kernel releases it when the
// fd is closed (either explicitly via releaseLock, or implicitly when
// the process exits).
func (s *Server) acquireLock() error {
	if s.opts.LockPath == "" {
		return nil
	}
	if dir := filepath.Dir(s.opts.LockPath); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("server: mkdir lock dir: %w", err)
		}
	}
	f, err := os.OpenFile(s.opts.LockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("server: open lock file: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return ErrLockHeld
		}
		return fmt.Errorf("server: flock: %w", err)
	}
	s.mu.Lock()
	s.lockFile = f
	s.mu.Unlock()
	return nil
}

func (s *Server) releaseLock() {
	s.mu.Lock()
	f := s.lockFile
	s.lockFile = nil
	s.mu.Unlock()
	if f == nil {
		return
	}
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	_ = f.Close()
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
