package support

// backend.go parses the unified backend URL used by [batchq] backend and
// the --backend CLI flag. The scheme picks the implementation:
//
//   sqlite3:///path/to/db                  — local SQLite
//   postgres://user:pass@host:5432/dbname  — local Postgres (future)
//   batchq-remote://host[:port]/path       — remote HTTPS REST API
//
// batchq-remote always uses HTTPS — plain HTTP exposure of the REST API
// is not supported. Operators terminate TLS at a reverse proxy. Anything
// else is an error.

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

const (
	BackendSqlite3      = "sqlite3"
	BackendPostgres     = "postgres"
	BackendBatchqRemote = "batchq-remote"
)

// Backend is a parsed backend URL.
type Backend struct {
	Scheme string
	Raw    string
	URL    *url.URL
}

// ParseBackend validates raw and returns a Backend. The empty string is an
// error — callers that want "default to sqlite3 at $BATCHQ_HOME/batchq.db"
// must supply that default before calling.
func ParseBackend(raw string) (*Backend, error) {
	if raw == "" {
		return nil, errors.New("backend: empty URL")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("backend: parse %q: %w", raw, err)
	}
	scheme := strings.ToLower(u.Scheme)
	switch scheme {
	case BackendSqlite3, BackendPostgres, BackendBatchqRemote:
	default:
		return nil, fmt.Errorf("backend: unsupported scheme %q (want sqlite3, postgres, batchq-remote)", u.Scheme)
	}
	return &Backend{Scheme: scheme, Raw: raw, URL: u}, nil
}

// IsLocal reports whether this backend runs an in-process server (sqlite3
// or postgres) vs. dialing a remote one (batchq-remote).
func (b *Backend) IsLocal() bool {
	switch b.Scheme {
	case BackendSqlite3, BackendPostgres:
		return true
	default:
		return false
	}
}

// SqlitePath returns the on-disk path for a sqlite3:// backend. Errors if
// the scheme is anything else.
//
// We accept both sqlite3:///abs/path (host empty, path absolute) and
// sqlite3:/rel/path (no authority); both are valid file URLs. A host
// component is rejected because sqlite3 has no notion of a remote host.
func (b *Backend) SqlitePath() (string, error) {
	if b.Scheme != BackendSqlite3 {
		return "", fmt.Errorf("backend: SqlitePath on scheme %q", b.Scheme)
	}
	if b.URL.Host != "" {
		return "", fmt.Errorf("backend: sqlite3 URL must not have a host (got %q)", b.URL.Host)
	}
	if b.URL.Path == "" {
		return "", errors.New("backend: sqlite3 URL missing path")
	}
	return b.URL.Path, nil
}

// RemoteHTTPURL converts a batchq-remote:// URL into an https:// URL the
// client can dial. batchq does not support plain-HTTP exposure of the
// REST API; operators terminate TLS at a reverse proxy.
func (b *Backend) RemoteHTTPURL() (string, error) {
	if b.Scheme != BackendBatchqRemote {
		return "", fmt.Errorf("backend: RemoteHTTPURL on scheme %q", b.Scheme)
	}
	if b.URL.Host == "" {
		return "", errors.New("backend: batchq-remote URL missing host")
	}
	out := url.URL{
		Scheme:   "https",
		User:     b.URL.User,
		Host:     b.URL.Host,
		Path:     b.URL.Path,
		RawQuery: b.URL.RawQuery,
		Fragment: b.URL.Fragment,
	}
	return out.String(), nil
}
