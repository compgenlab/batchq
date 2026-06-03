package server

import (
	"context"
	"log"
	"net"

	"github.com/mbreese/batchq/support"
)

// connContextWithPeerCreds is the ConnContext callback wired into the
// http.Server. For unix-socket connections it stashes the peer's
// uid/gid in the context via support.WithPeerCreds. For everything
// else it returns ctx unchanged, leaving support.PeerCredsFromContext
// to report ok=false.
//
// A getsockopt failure on a unix socket is logged and the connection
// is served without peer creds — the alternative (rejecting the
// connection) would silently break the deployment on any kernel that
// doesn't support SO_PEERCRED, and the worst case downstream is
// "submit handler can't derive identity and falls through to today's
// behavior."
func connContextWithPeerCreds(ctx context.Context, c net.Conn) context.Context {
	uid, gid, ok, err := getUnixPeerCred(c)
	if err != nil {
		log.Printf("server: peer-cred lookup failed on %s: %v", c.RemoteAddr(), err)
		return ctx
	}
	if !ok {
		return ctx
	}
	return support.WithPeerCreds(ctx, support.PeerCreds{Uid: uid, Gid: gid})
}
