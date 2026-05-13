package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/mbreese/batchq/client"
	"github.com/mbreese/batchq/iniconfig"
)

// clientURL is set from --server / [client] url. Empty means "use the
// default unix socket under BATCHQ_HOME".
var clientURL string

// clientToken is set from --token / [client] token. Used only for TCP.
var clientToken string

// dialClient resolves the server URL from flag/config/default and returns
// a connected client. Resolution order matches the rest of batchq:
// flag > [client] url in config > default unix://$BATCHQ_HOME/server.sock.
func dialClient() (*client.Client, error) {
	url := clientURL
	if url == "" {
		if v, ok := Config.Get("client", "url"); ok && v != "" {
			url = v
		}
	}
	if url == "" {
		url = "unix://" + filepath.Join(iniconfig.GetBatchqHome(), "server.sock")
	}
	token := clientToken
	if token == "" {
		if v, ok := Config.Get("client", "token"); ok {
			token = v
		}
	}
	return client.DialWithOptions(client.Options{
		URL:     url,
		Token:   token,
		Timeout: 30 * time.Second,
	})
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
}
