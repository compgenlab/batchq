package web

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/compgenlab/batchq/api"
)

// apiPrefix is the path prefix the REST proxy is mounted under. Requests below
// it authenticate with a bearer token (requireBearer), not the browser's HTTP
// Basic credentials, so requireBasic lets them pass through untouched.
const apiPrefix = "/api/"

// requireBasic gates the browser UI with a single shared admin account. When
// password is empty the middleware is a no-op — preserving the historical
// "unix socket / trusted network is the only gate" behavior. Requests under
// /api/ always pass through: they carry their own bearer token and are gated by
// requireBearer instead.
//
// Credentials are compared as fixed-length sha256 digests so neither the
// comparison time nor the value length leaks via timing (mirrors
// server/auth.go).
func requireBasic(username, password string, next http.Handler) http.Handler {
	if password == "" {
		return next
	}
	wantUser := sha256.Sum256([]byte(username))
	wantPass := sha256.Sum256([]byte(password))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, apiPrefix) {
			next.ServeHTTP(w, r)
			return
		}
		user, pass, ok := r.BasicAuth()
		if !ok {
			basicChallenge(w)
			return
		}
		gotUser := sha256.Sum256([]byte(user))
		gotPass := sha256.Sum256([]byte(pass))
		userOK := subtle.ConstantTimeCompare(wantUser[:], gotUser[:]) == 1
		passOK := subtle.ConstantTimeCompare(wantPass[:], gotPass[:]) == 1
		if !userOK || !passOK {
			basicChallenge(w)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func basicChallenge(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Basic realm="batchq"`)
	http.Error(w, "unauthorized", http.StatusUnauthorized)
}

// requireBearer gates the proxied REST API with a shared bearer token. When
// token is empty the middleware is a no-op (the caller warns when that leaves a
// TCP-exposed API unauthenticated). Matches server/auth.go's constant-time
// sha256 compare so the two behave identically.
func requireBearer(token string, next http.Handler) http.Handler {
	if token == "" {
		return next
	}
	want := sha256.Sum256([]byte(token))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := bearerToken(r.Header.Get(api.HeaderAuthorization))
		gotSum := sha256.Sum256([]byte(got))
		if subtle.ConstantTimeCompare(want[:], gotSum[:]) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "missing or invalid bearer token", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// bearerToken extracts the token from an `Authorization: Bearer <token>` header
// value (case-insensitive scheme). Anything else yields "".
func bearerToken(header string) string {
	const prefix = "bearer "
	if len(header) < len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(header[len(prefix):])
}
