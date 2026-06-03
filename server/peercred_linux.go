//go:build linux

package server

import (
	"errors"
	"net"
	"syscall"
)

// getUnixPeerCred returns the kernel-attested {uid, gid} of the process
// on the other end of a unix-socket connection. ok is false for
// non-unix connections (so the caller can fall back to its
// no-peer-creds path).
//
// Linux: getsockopt(fd, SOL_SOCKET, SO_PEERCRED) returns a struct ucred
// {pid, uid, gid}. The kernel records these at connect(2) time and the
// client cannot forge them.
func getUnixPeerCred(conn net.Conn) (uid uint32, gid uint32, ok bool, err error) {
	uc, isUnix := conn.(*net.UnixConn)
	if !isUnix {
		return 0, 0, false, nil
	}
	raw, rerr := uc.SyscallConn()
	if rerr != nil {
		return 0, 0, false, rerr
	}
	var ucred *syscall.Ucred
	var sockErr error
	cerr := raw.Control(func(fd uintptr) {
		ucred, sockErr = syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	})
	if cerr != nil {
		return 0, 0, false, cerr
	}
	if sockErr != nil {
		return 0, 0, false, sockErr
	}
	if ucred == nil {
		return 0, 0, false, errors.New("server: SO_PEERCRED returned nil")
	}
	return ucred.Uid, ucred.Gid, true, nil
}
