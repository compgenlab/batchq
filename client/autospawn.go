package client

// autospawn.go gives clients a one-shot "connect, and if nothing is
// answering on a unix socket, fork-exec the server and wait for it to
// come up" helper. This is what makes `batchq submit` on a workstation
// Just Work without the user having to start a server explicitly.
//
// Autospawn only fires for unix:// URLs — for remote https:// URLs we
// have no idea what host to spawn on. A failed unix connect is also the
// only signal we have that the server isn't running; for https:// we
// just surface the dial error.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

// AutospawnConfig configures DialAndConnect's behavior when the initial
// health probe fails. Zero value disables autospawn (caller gets the
// raw dial error).
type AutospawnConfig struct {
	// Enabled gates the whole feature. Default off.
	Enabled bool

	// Executable is the path to the batchq binary. Defaults to
	// os.Executable() (the currently-running binary).
	Executable string

	// ExtraArgs are appended to the `batchq server` invocation after the
	// auto-supplied --listen flag. Typically `--idle-timeout 1m`.
	ExtraArgs []string

	// PollTimeout caps how long DialAndConnect waits for the spawned
	// server to bind. Default 5s.
	PollTimeout time.Duration

	// PollInterval is the delay between health probes while waiting for
	// the server to come up. Default 50ms.
	PollInterval time.Duration

	// Log, if non-nil, is called with status messages (one line each).
	Log func(format string, args ...any)

	// SpawnFunc, if non-nil, replaces the built-in fork-exec spawn. The
	// function must arrange for a batchq server to start listening on
	// socketPath; it should return quickly (without blocking on the
	// server's lifetime). Used by tests to stand up an in-process
	// server instead of exec'ing the binary.
	SpawnFunc func(socketPath string) error
}

func (a AutospawnConfig) logf(format string, args ...any) {
	if a.Log != nil {
		a.Log(format, args...)
	}
}

// DialAndConnect parses opts.URL, returns a working Client, and (if
// auto.Enabled and the URL is a unix socket) transparently fork-execs
// `batchq server` when nothing is answering on the socket.
//
// Use this from CLI entry points where the user expects "submit just
// works." For long-lived servers (the web UI, a runner that wants a
// pre-existing server), call DialWithOptions directly.
func DialAndConnect(ctx context.Context, opts Options, auto AutospawnConfig) (*Client, error) {
	c, err := DialWithOptions(opts)
	if err != nil {
		return nil, err
	}

	// First probe: if the server's already up, we're done.
	if probeErr := c.Health(ctx); probeErr == nil {
		return c, nil
	} else if !auto.Enabled || c.SocketPath() == "" || !isConnectFailure(probeErr) {
		c.Close()
		return nil, probeErr
	} else {
		auto.logf("batchq: no server on %s, autospawning (%v)", c.SocketPath(), probeErr)
	}

	// Stale socket file from a crashed prior server confuses both
	// dial(2) (refused) and bind(2) (already in use). Remove it before
	// spawning.
	if c.SocketPath() != "" {
		if err := removeStaleSocket(c.SocketPath()); err != nil {
			auto.logf("batchq: failed to remove stale socket: %v", err)
		}
	}

	spawn := auto.SpawnFunc
	if spawn == nil {
		spawn = func(sock string) error { return spawnServer(sock, auto) }
	}
	if err := spawn(c.SocketPath()); err != nil {
		c.Close()
		return nil, fmt.Errorf("autospawn: spawn server: %w", err)
	}

	pollTimeout := auto.PollTimeout
	if pollTimeout <= 0 {
		pollTimeout = 5 * time.Second
	}
	pollInterval := auto.PollInterval
	if pollInterval <= 0 {
		pollInterval = 50 * time.Millisecond
	}

	deadline := time.Now().Add(pollTimeout)
	var lastErr error
	for time.Now().Before(deadline) {
		probeCtx, cancel := context.WithTimeout(ctx, pollInterval*4)
		err := c.Health(probeCtx)
		cancel()
		if err == nil {
			return c, nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			c.Close()
			return nil, ctx.Err()
		case <-time.After(pollInterval):
		}
	}
	c.Close()
	if lastErr == nil {
		lastErr = errors.New("server unreachable")
	}
	return nil, fmt.Errorf("autospawn: server did not come up within %s: %w", pollTimeout, lastErr)
}

// isConnectFailure returns true if err looks like "nothing is listening
// on that socket" — the only failure mode where spawning makes sense.
// Other errors (auth, malformed URL, server crash mid-request) get
// surfaced as-is.
func isConnectFailure(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	// net.Dial wraps ECONNREFUSED in *net.OpError; errors.Is unwraps it
	// on modern Go, but be defensive and also check via type assertion
	// in case the OS returns a stranger underlying error.
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if opErr.Op == "dial" {
			return true
		}
	}
	// Fallback: string match. Cheap and resilient to underlying-error
	// type churn across Go versions.
	msg := err.Error()
	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "no such file or directory")
}

// removeStaleSocket unlinks the socket path if it exists *and* nothing
// is listening on it. We refuse to delete a live socket (something
// briefly answered, then went away between probe and delete).
func removeStaleSocket(path string) error {
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	// Try to connect first; if it succeeds, someone's there.
	if c, err := net.DialTimeout("unix", path, 200*time.Millisecond); err == nil {
		_ = c.Close()
		return errors.New("socket is live, refusing to remove")
	}
	return os.Remove(path)
}

// spawnServer fork-execs `batchq server` in the background with stdio
// pointed at /dev/null. The child becomes a session leader (Setsid) so
// it survives this process exiting.
func spawnServer(socketPath string, auto AutospawnConfig) error {
	exe := auto.Executable
	if exe == "" {
		var err error
		exe, err = os.Executable()
		if err != nil {
			return fmt.Errorf("locate batchq binary: %w", err)
		}
	}

	args := []string{"server", "--listen", "unix://" + socketPath}
	args = append(args, auto.ExtraArgs...)

	cmd := exec.Command(exe, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	null, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err == nil {
		cmd.Stdin = null
		cmd.Stdout = null
		cmd.Stderr = null
	} else {
		cmd.Stdin = nil
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
	}

	if err := cmd.Start(); err != nil {
		if null != nil {
			_ = null.Close()
		}
		return err
	}
	// Don't block on Wait; reap in a goroutine so the kernel doesn't
	// pile up zombies if the parent stays alive longer than the spawned
	// server.
	go func() {
		_ = cmd.Wait()
		if null != nil {
			_ = null.Close()
		}
	}()
	auto.logf("batchq: spawned server pid=%d", cmd.Process.Pid)
	return nil
}
