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
	serverStorage     string
	serverWAL         bool
	serverLockPath    string
	serverIdleTimeout time.Duration
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Run the batchq REST API server",
	Long: `Run the batchq REST API server.

The server owns the SQLite database file and serves the v1 REST API over a
unix socket (default) or TCP. All other batchq components (CLI commands,
runners, the web UI) connect to it as clients.

Default listener:  unix://$BATCHQ_HOME/server.sock
Default storage:   $BATCHQ_HOME/batchq.db
`,
	RunE: runServer,
}

func init() {
	home := support.GetBatchqHome()
	defaultSock := "unix://" + filepath.Join(home, "server.sock")
	defaultDB := filepath.Join(home, "batchq.db")
	defaultLock := filepath.Join(home, "server.lock")

	serverCmd.Flags().StringVar(&serverListen, "listen", "",
		"Listener URL (unix:///path/to/sock or tcp://host:port). Default: "+defaultSock)
	serverCmd.Flags().StringVar(&serverStorage, "storage", "",
		"Storage file path. Default: "+defaultDB)
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

	if serverListen == "" {
		if v, ok := Config.Get("server", "listen"); ok && v != "" {
			serverListen = v
		} else {
			serverListen = "unix://" + filepath.Join(home, "server.sock")
		}
	}
	if serverStorage == "" {
		if v, ok := Config.Get("server", "storage"); ok && v != "" {
			serverStorage = v
		} else {
			serverStorage = filepath.Join(home, "batchq.db")
		}
	}
	if !serverWAL {
		if v, ok := Config.GetBool("server", "sqlite_wal"); ok {
			serverWAL = v
		}
	}
	// Lock path: default to $BATCHQ_HOME/server.lock unless the user
	// explicitly passed --lock="" or set [server] lock = "" in config.
	if !cmd.Flags().Changed("lock") {
		if v, ok := Config.Get("server", "lock"); ok {
			serverLockPath = v
		} else {
			serverLockPath = filepath.Join(home, "server.lock")
		}
	}
	if serverIdleTimeout == 0 {
		if v, ok := Config.Get("server", "idle_timeout"); ok && v != "" {
			parsed, err := time.ParseDuration(v)
			if err != nil {
				return fmt.Errorf("server: parse idle_timeout %q: %w", v, err)
			}
			serverIdleTimeout = parsed
		}
	}

	// Expand ~ and make absolute.
	if expanded, err := support.ExpandPathAbs(serverStorage); err == nil {
		serverStorage = expanded
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
		// TCP without TLS/auth is currently allowed but unauthenticated.
		// A reverse proxy is expected to handle TLS termination, and the
		// auth phase will add bearer-token gating later. For now we just
		// log a warning so it doesn't go unnoticed in operator logs.
		log.Println("warning: TCP listener has no authentication yet; only expose behind a trusted proxy")
	default:
		return fmt.Errorf("unsupported --listen scheme; use unix:// or tcp://")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	store, err := storage.Open(ctx, serverStorage, storage.Options{WAL: serverWAL})
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

	fmt.Fprintf(os.Stderr, "batchq server listening on %s (storage: %s)\n", serverListen, serverStorage)
	if serverIdleTimeout > 0 {
		fmt.Fprintf(os.Stderr, "batchq server: idle timeout %s\n", serverIdleTimeout)
	}
	if err := srv.Serve(ctx); err != nil {
		if errors.Is(err, server.ErrLockHeld) {
			// Another instance is already running. Exit silently with a
			// non-fatal status so autospawn races don't print noise.
			fmt.Fprintln(os.Stderr, "batchq server: another instance is already running")
			return nil
		}
		return err
	}
	return nil
}
