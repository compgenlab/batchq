//go:build !linux && !darwin

package server

import "net"

// getUnixPeerCred is a stub for platforms without a peer-credential
// getsockopt we support. Always returns ok=false. batchq's server does
// not run in production on unsupported OSes; this stub exists only so
// cross-platform builds compile.
func getUnixPeerCred(_ net.Conn) (uid uint32, gid uint32, ok bool, err error) {
	return 0, 0, false, nil
}
