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
	if !remoteMode && strings.HasPrefix(dialURL, "unix://") && !clientNoAutospawn {
		auto.Enabled = true
		auto.PollTimeout = waitTimeout
		auto.ExtraArgs = []string{
			"--idle-timeout", defaultsResolved.AutospawnIdleTimeout.String(),
			"--db", Config.Server.DB,
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

func init() {
	rootCmd.PersistentFlags().StringVar(&clientRemote, "remote", "",
		"remote batchq server URL: https://host[:port]/subpath")
	rootCmd.PersistentFlags().StringVar(&clientToken, "token", "",
		"bearer token for the remote server")
	rootCmd.PersistentFlags().BoolVar(&clientNoAutospawn, "no-autospawn", false,
		"do not auto-start a local server when the socket is unreachable")
}
