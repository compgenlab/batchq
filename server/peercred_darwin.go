//go:build darwin

package server

import (
	"net"

	"golang.org/x/sys/unix"
)

// getUnixPeerCred returns the kernel-attested {uid, gid} of the process
// on the other end of a unix-socket connection. ok is false for
// non-unix connections.
//
// macOS: getsockopt(fd, SOL_LOCAL, LOCAL_PEERCRED) returns a struct
// xucred. The uid is the effective uid of the peer at connect(2)
// time; gid is the first entry of the supplementary group list (which
// matches the peer's primary GID on a normal login).
func getUnixPeerCred(conn net.Conn) (uid uint32, gid uint32, ok bool, err error) {
	uc, isUnix := conn.(*net.UnixConn)
	if !isUnix {
		return 0, 0, false, nil
	}
	raw, rerr := uc.SyscallConn()
	if rerr != nil {
		return 0, 0, false, rerr
	}
	var xu *unix.Xucred
	var sockErr error
	cerr := raw.Control(func(fd uintptr) {
		xu, sockErr = unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	})
	if cerr != nil {
		return 0, 0, false, cerr
	}
	if sockErr != nil {
		return 0, 0, false, sockErr
	}
	primaryGid := uint32(0)
	if xu.Ngroups > 0 {
		primaryGid = xu.Groups[0]
	}
	return xu.Uid, primaryGid, true, nil
}
