package support

// identity.go resolves uid → {username, primary gid, supplementary
// groups} via NSS by shelling out to standard system tools. We do not
// use os/user.LookupId because batchq is a pure-Go build (no CGO),
// which means os/user falls back to reading /etc/passwd directly and
// never consults NSS — useless on the HPC clusters batchq targets,
// where users live in LDAP / SSSD / nis with empty /etc/passwd.
//
// Shelling out to getent + id gets us correct NSS resolution on every
// platform that supports them (every modern Linux, macOS) without
// pulling in cgo or a third-party NSS library.

import (
	"bytes"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

// ErrUserNotFound is returned when getent / id reports that the
// requested uid or username does not exist in NSS. Callers can use
// errors.Is to distinguish this from transient command failures.
var ErrUserNotFound = errors.New("identity: user not found")

// UserIdentity is the full server-side view of a unix user, derived
// from NSS at the time of lookup. Used by the submit handler to
// derive a job's identity from the connection's peer creds.
type UserIdentity struct {
	Username string
	Uid      uint32
	Gid      uint32   // primary group
	Groups   []uint32 // supplementary groups (may be empty)
}

// runCommand is the seam tests use to mock shell-outs. It returns
// stdout, exit code (best-effort; 0 if unavailable), and an error.
var runCommand = func(name string, args ...string) (stdout []byte, exitCode int, err error) {
	cmd := exec.Command(name, args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if runErr := cmd.Run(); runErr != nil {
		var ee *exec.ExitError
		if errors.As(runErr, &ee) {
			return out.Bytes(), ee.ExitCode(), runErr
		}
		return out.Bytes(), -1, runErr
	}
	return out.Bytes(), 0, nil
}

// SwapRunCommandForTest replaces the NSS command runner with fn and
// returns a function that restores the previous runner. Tests use
// this to assert identity-resolution behavior without depending on
// whatever NSS thinks of the host running the test.
func SwapRunCommandForTest(fn func(string, ...string) ([]byte, int, error)) func() {
	prev := runCommand
	runCommand = fn
	return func() { runCommand = prev }
}

// LookupUserByUid returns the UserIdentity for uid by consulting NSS
// via `getent passwd <uid>` and `id -G <username>`. Returns
// ErrUserNotFound when the user is unknown.
func LookupUserByUid(uid uint32) (UserIdentity, error) {
	uidStr := strconv.FormatUint(uint64(uid), 10)
	out, code, err := runCommand("getent", "passwd", uidStr)
	if err != nil {
		// `getent passwd <unknown>` exits 2 with empty stdout.
		if code == 2 || len(bytes.TrimSpace(out)) == 0 {
			return UserIdentity{}, fmt.Errorf("uid %d: %w", uid, ErrUserNotFound)
		}
		return UserIdentity{}, fmt.Errorf("identity: getent passwd %d: %w (%s)", uid, err, strings.TrimSpace(string(out)))
	}
	username, gid, perr := parsePasswdLine(out)
	if perr != nil {
		return UserIdentity{}, fmt.Errorf("identity: getent passwd %d: %w", uid, perr)
	}
	groups, gerr := lookupSupplementaryGroups(username)
	if gerr != nil {
		// Don't fail the whole lookup just because supp-groups
		// retrieval choked; the caller can still use the user's
		// primary identity. Surface the error to the caller log via
		// a wrapped sentinel so they can decide to warn.
		return UserIdentity{Username: username, Uid: uid, Gid: gid}, gerr
	}
	return UserIdentity{Username: username, Uid: uid, Gid: gid, Groups: groups}, nil
}

// parsePasswdLine parses a single line of getent's passwd output:
//
//	username:x:uid:gid:gecos:home:shell
//
// Only the username and primary gid are extracted; the rest is
// ignored. Multiple lines (which getent doesn't produce for a uid
// lookup, but defensively) are reduced to the first.
func parsePasswdLine(data []byte) (string, uint32, error) {
	line := bytes.TrimSpace(data)
	if i := bytes.IndexByte(line, '\n'); i >= 0 {
		line = line[:i]
	}
	parts := strings.Split(string(line), ":")
	if len(parts) < 4 {
		return "", 0, fmt.Errorf("malformed passwd line: %q", string(line))
	}
	username := parts[0]
	gid64, err := strconv.ParseUint(parts[3], 10, 32)
	if err != nil {
		return "", 0, fmt.Errorf("malformed gid in passwd line: %q", string(line))
	}
	return username, uint32(gid64), nil
}

// lookupSupplementaryGroups returns the user's supplementary group
// GIDs by running `id -G <username>` and parsing the space-separated
// numeric output. `id -G` consults NSS, so this is correct on
// LDAP / SSSD systems.
func lookupSupplementaryGroups(username string) ([]uint32, error) {
	out, _, err := runCommand("id", "-G", username)
	if err != nil {
		return nil, fmt.Errorf("identity: id -G %s: %w (%s)", username, err, strings.TrimSpace(string(out)))
	}
	fields := strings.Fields(string(out))
	groups := make([]uint32, 0, len(fields))
	for _, f := range fields {
		g, perr := strconv.ParseUint(f, 10, 32)
		if perr != nil {
			return nil, fmt.Errorf("identity: id -G %s: malformed gid %q", username, f)
		}
		groups = append(groups, uint32(g))
	}
	return groups, nil
}
