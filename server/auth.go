package server

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"

	"github.com/mbreese/batchq/api"
)

// withAuth enforces shared-token auth when Options.AuthToken is set. Every
// request must then carry `Authorization: Bearer <token>` matching the
// configured value; mismatches and missing tokens get a 401. When no token
// is configured the middleware is a no-op and the unix socket's filesystem
// permissions are the only access control.
//
// This is a single shared secret, not per-user auth: it is the
// "secure-enough" floor for a self-hosted single-user server. Real
// per-user identity over the network belongs in the managed server.
//
// Two requests bypass the check unconditionally:
//   - the health endpoint, so liveness probes / autospawn polling work
//     without distributing the secret to every prober; and
//   - the server's own ownership-monitor self-dials (HeaderInternalOwner),
//     which never carry a token.
func (s *Server) withAuth(next http.Handler) http.Handler {
	if s.opts.AuthToken == "" {
		return next
	}
	// Compare fixed-length digests so neither the comparison time nor the
	// token length leaks via timing.
	want := sha256.Sum256([]byte(s.opts.AuthToken))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == api.RouteHealth || r.Header.Get(api.HeaderInternalOwner) != "" {
			next.ServeHTTP(w, r)
			return
		}
		got := bearerToken(r.Header.Get(api.HeaderAuthorization))
		gotSum := sha256.Sum256([]byte(got))
		if subtle.ConstantTimeCompare(want[:], gotSum[:]) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeError(w, http.StatusUnauthorized, errors.New("missing or invalid bearer token"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// bearerToken extracts the token from an `Authorization: Bearer <token>`
// header value. The scheme match is case-insensitive; anything that isn't a
// bearer header yields "".
func bearerToken(header string) string {
	const prefix = "bearer "
	if len(header) < len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(header[len(prefix):])
}
