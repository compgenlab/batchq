package support

// backend.go parses the [server] db URL used by the local server. The
// scheme picks the implementation:
//
//   sqlite3:///path/to/db                  — local SQLite
//   postgres://user:pass@host:5432/dbname  — local Postgres (future)
//
// Remote access (clients talking to a batchq server on another host) is
// a separate concept — see [batchq] remote and ParseRemote below — and
// is not expressed as a "backend" URL.

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

const (
	BackendSqlite3  = "sqlite3"
	BackendPostgres = "postgres"
)

// Backend is a parsed [server] db URL.
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
	case BackendSqlite3, BackendPostgres:
	default:
		return nil, fmt.Errorf("backend: unsupported scheme %q (want sqlite3 or postgres)", u.Scheme)
	}
	return &Backend{Scheme: scheme, Raw: raw, URL: u}, nil
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

// ParseRemote validates the [batchq] remote URL and returns a canonical
// https:// URL the REST client can dial. Only https:// is accepted —
// plain HTTP exposure of the REST API is not supported; operators
// terminate TLS at a reverse proxy. The default port is 443 (handled
// implicitly by net/http when the port is absent).
//
// The URL path is preserved as a mount-point prefix; the client appends
// /api/v1/... to it.
func ParseRemote(raw string) (string, error) {
	if raw == "" {
		return "", errors.New("remote: empty URL")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("remote: parse %q: %w", raw, err)
	}
	if strings.ToLower(u.Scheme) != "https" {
		return "", fmt.Errorf("remote: scheme must be https (got %q)", u.Scheme)
	}
	if u.Host == "" {
		return "", errors.New("remote: URL missing host")
	}
	out := url.URL{
		Scheme:   "https",
		User:     u.User,
		Host:     u.Host,
		Path:     u.Path,
		RawQuery: u.RawQuery,
		Fragment: u.Fragment,
	}
	return out.String(), nil
}
