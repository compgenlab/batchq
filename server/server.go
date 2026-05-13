// Package server runs the batchq REST API. It owns the storage handle and
// listens on a unix socket or TCP address. Clients (CLI, runner, web UI)
// drive it via the API contract in package api.
//
// Phase 3 of the v2 rewrite: unix-socket listener only, no auth, no
// auto-spawn idle timeout (those come in Phase 8).
package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/mbreese/batchq/api"
	"github.com/mbreese/batchq/service"
)

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
}

// Server is a long-lived HTTP server over the batchq REST API.
type Server struct {
	svc  *service.Service
	opts Options

	httpSrv *http.Server

	mu        sync.Mutex
	listener  net.Listener
	socketPath string // set if listening on a unix socket; cleaned up on Shutdown
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
	return s, nil
}

// Serve binds the listener and blocks until Shutdown is called or the
// listener fails. The returned error is nil for a clean Shutdown,
// non-nil otherwise.
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

	if err := s.httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		s.cleanupSocket()
		return err
	}
	s.cleanupSocket()
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

	return s.withVersionHeader(mux)
}

// withVersionHeader stamps every response with the API version header so
// clients can detect breaking changes.
func (s *Server) withVersionHeader(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(api.HeaderVersion, api.Version)
		next.ServeHTTP(w, r)
	})
}
