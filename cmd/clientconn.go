package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mbreese/batchq/client"
	"github.com/mbreese/batchq/support"
)

// clientURL is set from --server / [client] url. Empty means "use the
// default unix socket under BATCHQ_HOME".
var clientURL string

// clientToken is set from --token / [client] token. Used only for TCP.
var clientToken string

// clientNoAutospawn disables autospawn. Useful for diagnostics or when
// the user wants connection failures to surface as errors instead of
// triggering a server fork-exec.
var clientNoAutospawn bool

// dialClient resolves the server URL from flag/config/default and returns
// a connected client. Resolution order matches the rest of batchq:
// flag > [client] url in config > default unix://$BATCHQ_HOME/server.sock.
//
// For unix:// URLs the client will autospawn `batchq server` if nothing
// is answering on the socket — that's what makes `batchq submit` Just
// Work on a workstation without a long-running daemon. Autospawn is
// disabled for TCP URLs (no way to know which host to spawn on) and
// when --no-autospawn is set.
func dialClient() (*client.Client, error) {
	url := clientURL
	if url == "" {
		if v, ok := Config.Get("client", "url"); ok && v != "" {
			url = v
		}
	}
	if url == "" {
		url = "unix://" + filepath.Join(support.GetBatchqHome(), "server.sock")
	}
	token := clientToken
	if token == "" {
		if v, ok := Config.Get("client", "token"); ok {
			token = v
		}
	}

	opts := client.Options{
		URL:     url,
		Token:   token,
		Timeout: 30 * time.Second,
	}

	auto := client.AutospawnConfig{}
	if strings.HasPrefix(url, "unix://") && !clientNoAutospawn {
		auto.Enabled = true
		auto.PollTimeout = 5 * time.Second
		auto.ExtraArgs = []string{"--idle-timeout", "1m"}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return client.DialAndConnect(ctx, opts, auto)
}

// mustDialClient is a tiny wrapper for CLI sites that just want to fatal
// out on connect failure (the same way db.OpenDB used to log.Fatalln).
func mustDialClient() *client.Client {
	c, err := dialClient()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to batchq server: %v\n", err)
		os.Exit(1)
	}
	return c
}

// cmdContext returns a context with a default timeout for one-shot CLI
// calls. 30s mirrors the prior `db.OpenDB` invocations.
func cmdContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 30*time.Second)
}

func init() {
	rootCmd.PersistentFlags().StringVar(&clientURL, "server", "",
		"batchq server URL (unix:///path or tcp://host:port). Default: $BATCHQ_HOME/server.sock")
	rootCmd.PersistentFlags().StringVar(&clientToken, "token", "",
		"bearer token for TCP transports (ignored for unix sockets)")
	rootCmd.PersistentFlags().BoolVar(&clientNoAutospawn, "no-autospawn", false,
		"do not auto-start a server when the socket is unreachable")
}
