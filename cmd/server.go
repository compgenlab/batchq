package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mbreese/batchq/server"
	"github.com/mbreese/batchq/service"
	"github.com/mbreese/batchq/storage"
	"github.com/mbreese/batchq/support"
	"github.com/spf13/cobra"
)

var (
	serverListen      string
	serverWAL         bool
	serverIdleTimeout time.Duration
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Run the batchq HTTP REST API server",
	Long: `Run the batchq HTTP REST API server.

The server owns the queue database and serves the v1 REST API over a
unix domain socket. Network exposure is the reverse proxy's job —
batchq itself never binds a TCP port. All other batchq components (CLI
commands, runners, the web UI) connect to it as REST clients.

The backend (sqlite3:/// path, postgres://..., etc.) is taken from the
persistent --backend flag or the [batchq] backend config key. The server
refuses to start when the backend is batchq-remote:// because in that
mode there is no local server — the remote one is the server.

Default listener:  unix://$BATCHQ_HOME/batchq.sock
Default backend:   sqlite3://$BATCHQ_HOME/batchq.db
`,
	RunE: runServer,
}

func init() {
	d := support.NewDefaults()

	serverCmd.Flags().StringVar(&serverListen, "listen", "",
		"Listener URL (unix:///path/to/sock). Default: "+d.ServerListen)
	serverCmd.Flags().BoolVar(&serverWAL, "sqlite-wal", false,
		"Enable SQLite WAL journal mode. NOT SAFE on networked filesystems; only use when the DB file is on local disk.")
	serverCmd.Flags().DurationVar(&serverIdleTimeout, "idle-timeout", 0,
		"Auto-shut-down after this duration of no activity. Zero (default) disables.")

	rootCmd.AddCommand(serverCmd)
}

func runServer(_ *cobra.Command, _ []string) error {
	// Backend: --backend flag (persistent root) > Config (already
	// merged with defaults during init).
	backendRaw := clientBackend
	if backendRaw == "" {
		backendRaw = Config.Batchq.Backend
	}
	backend, err := support.ParseBackend(backendRaw)
	if err != nil {
		return err
	}
	if !backend.IsLocal() {
		return fmt.Errorf("server: cannot run a local server with remote backend %q", backendRaw)
	}
	if backend.Scheme != support.BackendSqlite3 {
		return fmt.Errorf("server: backend %q not yet implemented", backend.Scheme)
	}
	storagePath, err := backend.SqlitePath()
	if err != nil {
		return err
	}
	if expanded, err := support.ExpandPathAbs(storagePath); err == nil {
		storagePath = expanded
	}

	if serverListen == "" {
		serverListen = Config.Server.Listen
	}
	if !serverWAL {
		serverWAL = Config.Server.SqliteWAL
	}
	if serverIdleTimeout == 0 {
		serverIdleTimeout = Config.Server.IdleTimeout.AsDuration()
	}

	// Sanity check the listen URL. Only unix:// is supported; network
	// exposure goes through a reverse proxy.
	if !strings.HasPrefix(serverListen, "unix://") {
		return fmt.Errorf("--listen must be a unix:// URL (got %q); use a reverse proxy for network exposure", serverListen)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	store, err := storage.Open(ctx, storagePath, storage.Options{WAL: serverWAL})
	if err != nil {
		return fmt.Errorf("open storage: %w", err)
	}
	defer store.Close()

	svc := service.New(store)
	srv, err := server.New(svc, server.Options{
		Listen:      serverListen,
		IdleTimeout: serverIdleTimeout,
	})
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "batchq server listening on %s (backend: %s)\n", serverListen, backend.Raw)
	if serverIdleTimeout > 0 {
		fmt.Fprintf(os.Stderr, "batchq server: idle timeout %s\n", serverIdleTimeout)
	}
	if err := srv.Serve(ctx); err != nil {
		if errors.Is(err, server.ErrAlreadyRunning) {
			fmt.Fprintln(os.Stderr, "batchq server: another instance is already running")
			return nil
		}
		return err
	}
	return nil
}
