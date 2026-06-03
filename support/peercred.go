package support

import "context"

// PeerCreds carries the kernel-attested identity of a unix-socket
// client. Populated by the server when a request arrives over a unix
// socket; absent (PeerCredsFromContext returns ok=false) otherwise —
// HTTP requests via TCP, in-process test transports, or remote
// clients reached over a reverse proxy.
//
// These creds are the trust root for the multi-user identity story
// (submit, hold, release, cancel). Never construct one from
// client-supplied data.
type PeerCreds struct {
	Uid uint32
	Gid uint32
}

type peerCredsKey struct{}

// WithPeerCreds returns a child context carrying the given peer
// creds. Exported so the server package's ConnContext callback can
// stash them; service-layer callers should use PeerCredsFromContext
// to read them back.
func WithPeerCreds(ctx context.Context, p PeerCreds) context.Context {
	return context.WithValue(ctx, peerCredsKey{}, p)
}

// PeerCredsFromContext returns the peer creds stashed in ctx, plus a
// flag indicating presence. Callers should treat ok=false as "no
// kernel-attested identity available" and decide their own policy.
func PeerCredsFromContext(ctx context.Context) (PeerCreds, bool) {
	p, ok := ctx.Value(peerCredsKey{}).(PeerCreds)
	return p, ok
}
