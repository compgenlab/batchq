package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/mbreese/batchq/client"
	"github.com/mbreese/batchq/support"
)

// clientRemote overrides [batchq] remote from the config. Empty means
// "use config / default". When non-empty, clients dial this HTTPS URL
// directly and never autospawn a local server.
var clientRemote string

// clientToken overrides [batchq] token. Used only when talking to a
// remote server; ignored for the local unix socket path.
var clientToken string

// clientNoAutospawn disables autospawn for the local socket path.
// Useful for diagnostics, or when the user wants a connect failure to
// surface as an error rather than a fork-exec.
var clientNoAutospawn bool

// clientRestartServer asks dialClient to shut down any running local
// server before proceeding. The normal autospawn flow then forks a
// fresh server. Has no effect for remote backends — operators don't
// shut down remote servers from a client flag.
var clientRestartServer bool

// dialClient picks the API endpoint and returns a connected *client.Client.
//
// If [batchq] remote is set (or --remote was given) the client dials
// that HTTPS URL directly — no local server is involved. Otherwise the
// client dials the local [server] listen unix socket and may autospawn
// a server if nothing is answering.
func dialClient() (*client.Client, error) {
	remoteRaw := clientRemote
	if remoteRaw == "" {
		remoteRaw = Config.Batchq.Remote
	}

	var dialURL string
	remoteMode := remoteRaw != ""
	if remoteMode {
		u, err := support.ParseRemote(remoteRaw)
		if err != nil {
			return nil, err
		}
		dialURL = u
	} else {
		dialURL = Config.Server.Listen
	}

	token := clientToken
	if token == "" {
		token = Config.Batchq.Token
	}
	opts := client.Options{
		URL:     dialURL,
		Token:   token,
		Timeout: 30 * time.Second,
	}

	auto := client.AutospawnConfig{}
	waitTimeout := Config.Batchq.AutospawnWaitTimeout.AsDuration()
	if waitTimeout <= 0 {
		waitTimeout = defaultsResolved.AutospawnWaitTimeout
	}
	localUnix := !remoteMode && strings.HasPrefix(dialURL, "unix://")
	if localUnix && !clientNoAutospawn {
		auto.Enabled = true
		auto.PollTimeout = waitTimeout
		auto.ExtraArgs = []string{
			"--idle-timeout", defaultsResolved.AutospawnIdleTimeout.String(),
			"--db", Config.Server.DB,
		}
	}

	// --restart-server: drain and exit any running local server before
	// the normal dial-and-autospawn path picks up. Best-effort: a
	// connect failure here just means there was nothing to shut down.
	if clientRestartServer && localUnix {
		if err := shutdownLocalServer(opts); err != nil {
			fmt.Fprintf(os.Stderr, "warning: --restart-server: %v\n", err)
		}
	}

	// Outer context must outlive the autospawn wait — otherwise the
	// context expires before the poll loop does and the user sees a
	// generic "context deadline exceeded" instead of the real
	// autospawn-timeout message.
	outerTimeout := waitTimeout + 5*time.Second
	if outerTimeout < 10*time.Second {
		outerTimeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), outerTimeout)
	defer cancel()
	return client.DialAndConnect(ctx, opts, auto)
}

// mustDialClient is a thin wrapper for CLI sites that just want to exit
// loudly on connect failure.
func mustDialClient() *client.Client {
	c, err := dialClient()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to batchq server: %v\n", err)
		os.Exit(1)
	}
	return c
}

// cmdContext returns a context with a default timeout for one-shot CLI
// calls.
func cmdContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 30*time.Second)
}

// cmdContextRetryable returns a context budgeted to outlast the client's
// reconnect-and-retry of an idle-server handoff: the graduated backoff
// (5s+10s+30s) plus the per-attempt requests. Mutating CLI calls that must
// survive a server cull/respawn (notably submit) use this instead of
// cmdContext so the retries aren't cut off by the 30s default.
func cmdContextRetryable() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 2*time.Minute)
}

// shutdownLocalServer probes the unix-socket server and, if it answers,
// asks it to drain and exit. Returns nil on either "no server" or
// "server confirmed down".
//
// If Shutdown fails (e.g. against an old server that predates the
// /admin/shutdown route and answers 404), the fallback path forcibly
// unlinks the socket file. The old server keeps its bound socket inode
// but the path no longer resolves to it, so new clients dial nothing
// and the autospawn forks a fresh server. The orphaned old process
// idles out on its own.
func shutdownLocalServer(opts client.Options) error {
	c, err := client.DialWithOptions(opts)
	if err != nil {
		return nil
	}
	defer c.Close()

	probeCtx, probeCancel := context.WithTimeout(context.Background(), 2*time.Second)
	probeErr := c.Health(probeCtx)
	probeCancel()
	if probeErr != nil {
		// Nothing answering — nothing to shut down. (The autospawn flow
		// will deal with any stale socket file on its own.)
		return nil
	}

	sctx, scancel := context.WithTimeout(context.Background(), 10*time.Second)
	shutdownErr := c.Shutdown(sctx)
	scancel()

	if shutdownErr != nil {
		return forceUnlinkSocket(c.SocketPath(), shutdownErr)
	}

	// Wait for the server to actually go away. The server's clean
	// shutdown path unlinks the socket, so a successful Health probe
	// here means the old server is still draining.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		hctx, hcancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		err := c.Health(hctx)
		hcancel()
		if err != nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	// The server accepted the request but is still answering. Probably
	// stuck mid-drain. Fall back to the force-unlink path.
	return forceUnlinkSocket(c.SocketPath(), fmt.Errorf("server still answering after shutdown request"))
}

// forceUnlinkSocket detaches the socket path from any process still
// bound to it. Used as a fallback when the graceful shutdown request
// fails or hangs. The orphaned process keeps its inode but is now
// unreachable via the path.
func forceUnlinkSocket(sockPath string, cause error) error {
	if sockPath == "" {
		return fmt.Errorf("shutdown request: %w", cause)
	}
	fmt.Fprintf(os.Stderr, "warning: graceful shutdown failed (%v); force-unlinking %s\n", cause, sockPath)
	if err := os.Remove(sockPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("shutdown request: %w; force-unlink also failed: %v", cause, err)
	}
	return nil
}

func init() {
	rootCmd.PersistentFlags().StringVar(&clientRemote, "remote", "",
		"remote batchq server URL: https://host[:port]/subpath")
	rootCmd.PersistentFlags().StringVar(&clientToken, "token", "",
		"bearer token for the remote server")
	rootCmd.PersistentFlags().BoolVar(&clientNoAutospawn, "no-autospawn", false,
		"do not auto-start a local server when the socket is unreachable")
	rootCmd.PersistentFlags().BoolVar(&clientRestartServer, "restart-server", false,
		"shut down any running local server before this command (no effect for remote)")
}
