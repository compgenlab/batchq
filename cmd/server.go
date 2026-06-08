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
		"Listener URL (unix:///path/to/sock or tcp://host:port). Default: "+d.ServerListen)
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

	// Sanity check the listen URL. unix:// (default) or tcp://host:port
	// (plain HTTP, for containerized deployments). batchq never terminates
	// TLS — front a TCP port with a reverse proxy for network exposure.
	if !strings.HasPrefix(serverListen, "unix://") && !strings.HasPrefix(serverListen, "tcp://") {
		return fmt.Errorf("--listen must be a unix:// or tcp:// URL (got %q)", serverListen)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Lifecycle debug log (shared with the client that spawned us via --log).
	dlog := debugLog("server")
	dlog.Logf("server start listen=%s db=%s idle=%s", serverListen, backend.Raw, serverIdleTimeout)

	// Win the single-instance election BEFORE opening the database. A server
	// that loses the election must never touch the DB file — otherwise
	// concurrently autospawned servers contend on it and the winner sees
	// SQLITE_BUSY. 0 socketMode lets Elect apply its 0600 default.
	ln, socketPath, err := server.Elect(serverListen, 0)
	if err != nil {
		if errors.Is(err, server.ErrAlreadyRunning) {
			dlog.Logf("election lost: another instance already listening on %s; exiting", serverListen)
			fmt.Fprintln(os.Stderr, "batchq server: another instance is already running")
			return nil
		}
		dlog.Logf("election error on %s: %v", serverListen, err)
		return err
	}
	dlog.Logf("election won socket=%s", socketPath)
	// From here on we own the election token; release it if we bail before
	// handing it to the server.
	releaseToken := func() {
		_ = ln.Close()
		if socketPath != "" {
			_ = os.Remove(socketPath)
		}
	}

	// Open the DB only after winning the election. We hold the socket (the
	// single-instance lock) for the whole DB lifetime: the server's shutdown
	// closes the DB BEFORE unlinking the socket (see Options.OnShutdown), so a
	// freshly-autospawned server can't bind the socket — and thus can't open
	// the DB — until this one has dropped it. That ordering is what prevents
	// two servers contending on the DB and reporting SQLITE_BUSY.
	store, err := storage.Open(ctx, storagePath, storage.Options{WAL: serverWAL})
	if err != nil {
		dlog.Logf("db open error path=%s: %v", storagePath, err)
		releaseToken()
		return fmt.Errorf("open storage: %w", err)
	}
	dlog.Logf("db opened path=%s wal=%v", storagePath, serverWAL)
	// Safety net only — the server's ordered shutdown (OnShutdown) is the
	// primary close; Storage.Close is idempotent so this double-close is a
	// no-op if shutdown already ran.
	defer store.Close()

	svc := service.New(store)
	srv, err := server.New(svc, server.Options{
		Listen:      serverListen,
		IdleTimeout: serverIdleTimeout,
		AuthToken:   Config.Server.Token,
		OnShutdown:  store.Close,
		Logf:        dlog.Logf,
	})
	if err != nil {
		releaseToken()
		return err
	}

	fmt.Fprintf(os.Stderr, "batchq server listening on %s (db: %s)\n", serverListen, backend.Raw)
	if serverIdleTimeout > 0 {
		fmt.Fprintf(os.Stderr, "batchq server: idle timeout %s\n", serverIdleTimeout)
	}
	if Config.Server.Token != "" {
		fmt.Fprintln(os.Stderr, "batchq server: shared-token auth enabled (Authorization: Bearer required)")
	} else if strings.HasPrefix(serverListen, "tcp://") {
		fmt.Fprintln(os.Stderr, "batchq server: WARNING listening on a TCP port without [server] token — the API is unauthenticated (TCP carries no peer credentials). Set a token or front it with an authenticating proxy.")
	}

	serveErr := srv.ServeListener(ctx, ln, socketPath)
	dlog.Logf("serve exit err=%v", serveErr)
	return serveErr
}
