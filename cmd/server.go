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
	serverDB          string
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

The database (sqlite3:/// path, postgres://...) is taken from the
--db flag or [server] db config key. If [batchq] remote is set then
this host runs no local server and 'batchq server' refuses to start.

Default listener:  unix://$BATCHQ_HOME/batchq.sock
Default db:        sqlite3://$BATCHQ_HOME/batchq.db
`,
	RunE: runServer,
}

func init() {
	d := support.NewDefaults()

	serverCmd.Flags().StringVar(&serverListen, "listen", "",
		"Listener URL (unix:///path/to/sock). Default: "+d.ServerListen)
	serverCmd.Flags().StringVar(&serverDB, "db", "",
		"Database URL: sqlite3:///path or postgres://... Default: "+d.Backend)
	serverCmd.Flags().BoolVar(&serverWAL, "sqlite-wal", false,
		"Enable SQLite WAL journal mode. NOT SAFE on networked filesystems; only use when the DB file is on local disk.")
	serverCmd.Flags().DurationVar(&serverIdleTimeout, "idle-timeout", 0,
		"Auto-shut-down after this duration of no activity. Zero (default) disables.")

	rootCmd.AddCommand(serverCmd)
}

func runServer(_ *cobra.Command, _ []string) error {
	// Remote and local-server are mutually exclusive: if a remote API
	// is configured for clients, this host should not run a local
	// server — refuse to start so the misconfiguration is obvious.
	if Config.Batchq.Remote != "" {
		return fmt.Errorf("server: [batchq] remote is set (%q); a local server cannot run alongside it", Config.Batchq.Remote)
	}

	// DB: --db flag > Config (already merged with defaults during init).
	dbRaw := serverDB
	if dbRaw == "" {
		dbRaw = Config.Server.DB
	}
	backend, err := support.ParseBackend(dbRaw)
	if err != nil {
		return err
	}
	if backend.Scheme != support.BackendSqlite3 {
		return fmt.Errorf("server: db scheme %q not yet implemented", backend.Scheme)
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
		AuthToken:   Config.Server.Token,
	})
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "batchq server listening on %s (db: %s)\n", serverListen, backend.Raw)
	if serverIdleTimeout > 0 {
		fmt.Fprintf(os.Stderr, "batchq server: idle timeout %s\n", serverIdleTimeout)
	}
	if Config.Server.Token != "" {
		fmt.Fprintln(os.Stderr, "batchq server: shared-token auth enabled (Authorization: Bearer required)")
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
