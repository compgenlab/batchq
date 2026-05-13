package cmd

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
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
	serverLockPath    string
	serverIdleTimeout time.Duration
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Run the batchq REST API server",
	Long: `Run the batchq REST API server.

The server owns the queue database and serves the v1 REST API over a
unix socket (default) or TCP. All other batchq components (CLI commands,
runners, the web UI) connect to it as clients.

The backend (sqlite3:/// path, postgres://..., etc.) is taken from the
persistent --backend flag or the [batchq] backend config key. The server
refuses to start when the backend is batchq-remote:// because in that
mode there is no local server — the remote one is the server.

Default listener:  unix://$BATCHQ_HOME/server.sock
Default backend:   sqlite3://$BATCHQ_HOME/batchq.db
`,
	RunE: runServer,
}

func init() {
	home := support.GetBatchqHome()
	defaultSock := "unix://" + filepath.Join(home, "server.sock")
	defaultLock := filepath.Join(home, "server.lock")

	serverCmd.Flags().StringVar(&serverListen, "listen", "",
		"Listener URL (unix:///path/to/sock or tcp://host:port). Default: "+defaultSock)
	serverCmd.Flags().BoolVar(&serverWAL, "sqlite-wal", false,
		"Enable SQLite WAL journal mode. NOT SAFE on networked filesystems; only use when the DB file is on local disk.")
	serverCmd.Flags().StringVar(&serverLockPath, "lock", "",
		"Lock file path used to elect a single server instance. Default: "+defaultLock+". Pass an empty string to disable.")
	serverCmd.Flags().DurationVar(&serverIdleTimeout, "idle-timeout", 0,
		"Auto-shut-down after this duration of no activity. Zero (default) disables.")

	rootCmd.AddCommand(serverCmd)
}

func runServer(cmd *cobra.Command, _ []string) error {
	home := support.GetBatchqHome()

	// Backend: --backend flag (persistent root) > [batchq] backend > default.
	backendRaw := clientBackend
	if backendRaw == "" {
		backendRaw = Config.Batchq.Backend
	}
	if backendRaw == "" {
		backendRaw = "sqlite3://" + filepath.Join(home, "batchq.db")
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
		if v := Config.Server.Listen; v != "" {
			serverListen = v
		} else {
			serverListen = "unix://" + filepath.Join(home, "server.sock")
		}
	}
	if !serverWAL {
		serverWAL = Config.Server.SqliteWAL
	}
	// Lock path: default to $BATCHQ_HOME/server.lock unless the user
	// explicitly passed --lock="" or set [server] lock = "" in config.
	if !cmd.Flags().Changed("lock") {
		if v := Config.Server.Lock; v != "" {
			serverLockPath = v
		} else {
			serverLockPath = filepath.Join(home, "server.lock")
		}
	}
	if serverIdleTimeout == 0 {
		serverIdleTimeout = Config.Server.IdleTimeout.AsDuration()
	}
	if serverLockPath != "" {
		if expanded, err := support.ExpandPathAbs(serverLockPath); err == nil {
			serverLockPath = expanded
		}
	}

	// Sanity check the listen URL.
	switch {
	case strings.HasPrefix(serverListen, "unix://"):
	case strings.HasPrefix(serverListen, "tcp://"):
		log.Println("warning: TCP listener has no authentication yet; only expose behind a trusted proxy")
	default:
		return fmt.Errorf("unsupported --listen scheme; use unix:// or tcp://")
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
		LockPath:    serverLockPath,
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
		if errors.Is(err, server.ErrLockHeld) {
			fmt.Fprintln(os.Stderr, "batchq server: another instance is already running")
			return nil
		}
		return err
	}
	return nil
}
