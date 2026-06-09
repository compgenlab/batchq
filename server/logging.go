package server

// logging.go is the server side of the --log / [batchq] log lifecycle debug
// log. s.logf forwards events to Options.Logf (wired to the shared debug file
// by cmd/server.go), and withLogging records every API request the server
// serves — so a human reading the file can see which server PID handled each
// submit and capture the exact text of any 5xx (e.g. SQLITE_BUSY).

import (
	"net/http"
	"strings"
	"time"

	"github.com/mbreese/batchq/api"
)

// logf forwards an event to Options.Logf if configured (no-op otherwise).
func (s *Server) logf(format string, args ...any) {
	if s.opts.Logf != nil {
		s.opts.Logf(format, args...)
	}
}

// loggingRW captures the response status and (for 5xx only) a bounded prefix of
// the body so the error text — e.g. "database is locked (5) (SQLITE_BUSY)" —
// makes it into the debug log.
type loggingRW struct {
	http.ResponseWriter
	status    int
	body      []byte
	capturing bool
}

const maxLoggedBody = 512

func (w *loggingRW) WriteHeader(code int) {
	w.status = code
	w.capturing = code >= 500
	w.ResponseWriter.WriteHeader(code)
}

func (w *loggingRW) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	if w.capturing && len(w.body) < maxLoggedBody {
		n := maxLoggedBody - len(w.body)
		if n > len(b) {
			n = len(b)
		}
		w.body = append(w.body, b[:n]...)
	}
	return w.ResponseWriter.Write(b)
}

// withLogging logs one line per API request: method, path, final status, and
// duration — plus the error body for 5xx. Health probes and the ownership
// monitor's self-dials are skipped to keep the signal high (they fire
// frequently and carry no real work).
func (s *Server) withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.opts.Logf == nil ||
			r.URL.Path == api.RouteHealth ||
			r.Header.Get(api.HeaderInternalOwner) != "" {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		lw := &loggingRW{ResponseWriter: w}
		next.ServeHTTP(lw, r)
		if lw.status == 0 {
			lw.status = http.StatusOK
		}
		dur := time.Since(start).Round(time.Millisecond)
		if lw.status >= 500 {
			s.logf("req %s %s -> %d (%s) %s", r.Method, r.URL.Path, lw.status, dur, strings.TrimSpace(string(lw.body)))
		} else {
			s.logf("req %s %s -> %d (%s)", r.Method, r.URL.Path, lw.status, dur)
		}
	})
}
