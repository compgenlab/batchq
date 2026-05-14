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

// clientBackend overrides [batchq] backend from the config. Empty means
// "use config / default".
var clientBackend string

// clientToken overrides [batchq] token. Used only for batchq-remote://
// backends; ignored for local sqlite3 / postgres.
var clientToken string

// clientNoAutospawn disables autospawn for local backends. Useful for
// diagnostics, or when the user wants a connect failure to surface as an
// error rather than a fork-exec.
var clientNoAutospawn bool

// dialClient resolves the backend (flag > config > default sqlite3 under
// $BATCHQ_HOME), computes the URL the REST client should dial, and
// returns a connected *client.Client.
//
// For local backends (sqlite3, postgres) the client dials the unix socket
// configured in [server] listen and may autospawn the server if nothing
// is answering. For batchq-remote:// backends the client dials the remote
// HTTPS URL directly and never autospawns.
func dialClient() (*client.Client, error) {
	backendRaw := clientBackend
	if backendRaw == "" {
		backendRaw = Config.Batchq.Backend
	}
	backend, err := support.ParseBackend(backendRaw)
	if err != nil {
		return nil, err
	}

	var dialURL string
	if backend.IsLocal() {
		dialURL = Config.Server.Listen
	} else {
		dialURL, err = backend.RemoteHTTPURL()
		if err != nil {
			return nil, err
		}
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
	if backend.IsLocal() && strings.HasPrefix(dialURL, "unix://") && !clientNoAutospawn {
		auto.Enabled = true
		auto.PollTimeout = 5 * time.Second
		auto.ExtraArgs = []string{
			"--idle-timeout", defaultsResolved.AutospawnIdleTimeout.String(),
			"--backend", backend.Raw,
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	rootCmd.PersistentFlags().StringVar(&clientBackend, "backend", "",
		"backend URL: sqlite3:///path, postgres://..., or batchq-remote://host/api/v1")
	rootCmd.PersistentFlags().StringVar(&clientToken, "token", "",
		"bearer token for batchq-remote:// backends")
	rootCmd.PersistentFlags().BoolVar(&clientNoAutospawn, "no-autospawn", false,
		"do not auto-start a local server when the socket is unreachable")
}
