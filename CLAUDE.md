# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build, run, test

This is a Go module (Go 1.23). batchq is **pure Go** — `modernc.org/sqlite` replaces `mattn/go-sqlite3`, so there is no CGO and no C cross-toolchain anywhere in the build.

- Build for current host: `go build -o bin/batchq main.go` (or `make`, which produces `bin/batchq.linux`).
- Cross-compile is just `GOOS=… GOARCH=… go build` — no toolchain prep. The `Makefile` has targets for `linux_amd64`, `linux_arm64`, `macos_amd64`, `macos_arm64`.
- Run without building: `make run` or `go run main.go <subcommand>`.
- Tests: `go test ./...`. Single test: `go test ./cmd -run TestName`.
- The hidden `batchq debug` subcommand prints the resolved `batchq home` and config file path — useful when diagnosing config loading.
- A Docker-based Go toolchain wrapper at `/tmp/dgo` is available in the dev environment when no host `go` is installed; it shells out to the `mcr.microsoft.com/devcontainers/go:1-1.23` image with the repo bind-mounted.

## Architecture (v2 client/server)

`batchq` is a single binary that runs in two roles: a long-lived `batchq server` process that owns the SQLite database, and short-lived clients (`submit`, `run`, `show`, `hold`, `cleanup`, `web`) that talk to it over an HTTP REST API on a unix domain socket. Network exposure (remote clients, runners on other hosts) is the reverse proxy's job — batchq itself never binds a TCP port. This split exists so the DB file can safely live on a networked filesystem (NFS / Lustre): only one process touches it, so SQLite's cross-process locking failure modes don't apply.

On a workstation, a CLI client transparently fork-execs `batchq server --idle-timeout 1m` when the socket is unreachable, then polls for it to come up. The server idles out when no requests arrive — so `batchq submit ./script.sh` still Just Works without explicit server management.

### Top-level layout

- `main.go` — embeds `LICENSE` and hands off to `cmd.Execute()`.
- `cmd/` — Cobra subcommands.
  - `root.go` loads `~/.batchq/config` (overridable via `BATCHQ_HOME`) into the package-level `*support.Config`. `submit`/`show`/`hold`/`cleanup`/`run`/`web` all open a REST client via `cmd/clientconn.go:dialClient()`.
  - `server.go` is the server entrypoint; flags: `--listen`, `--sqlite-wal`, `--lock`, `--idle-timeout`. The backend (sqlite3 path) comes from the persistent `--backend` flag / `[batchq] backend` config.
  - `clientconn.go` — `dialClient()` resolves the server URL (flag > `[client] url` > default `unix://$BATCHQ_HOME/server.sock`), wires autospawn for unix URLs, and returns a `*client.Client`. `--no-autospawn` opts out.
- `api/` — shared REST contract: route constants (`api/routes.go`) and request/response DTOs (`api/types.go`). `JobDTO` is the wire format; `api.JobToDef` / `api.JobFromDef` bridge to `jobs.JobDef`.
- `storage/` — persistence layer. `Storage` interface + `sqliteStorage` impl using `modernc.org/sqlite`. `Open(ctx, path, Options)` creates the file and applies `schema.sql` (embedded) idempotently on every open. Journal mode defaults to `DELETE` (rollback) because WAL's `-shm` shared-memory file is unsafe on NFS/Lustre; `Options{WAL: true}` opts into WAL when the DB is on local disk.
- `service/` — server-side business logic that wraps `Storage`: dep resolution, queue ordering, atomic claim, hold/release, cleanup recursion. No HTTP knowledge here.
- `server/` — HTTP layer.
  - `server.go` wires service handlers into an `http.ServeMux`, listens on unix (mode `0600`) or TCP, runs the activity-tracking middleware, manages the flock-based lock file, runs the idle monitor, and shuts down cleanly on context cancel.
  - Single-instance election: `LockPath` is held via `syscall.Flock(LOCK_EX|LOCK_NB)` for the server's lifetime; a second instance fails fast with `ErrLockHeld` (and `cmd/server.go` treats that as a clean non-zero-exit). The kernel releases the flock on crash, so no manual stale-lock cleanup needed (the unix socket file *does* need cleanup, handled by autospawn).
  - Idle shutdown: `withActivity` middleware bumps `lastActivityNanos` on both request entry and exit and tracks `inFlight`; `runIdleMonitor` ticks at `IdleCheckInterval` and calls `httpSrv.Shutdown` when both the age threshold is reached AND `inFlight == 0`.
- `client/` — REST client used by every in-repo client.
  - `client.go` — one method per API endpoint over an `http.Client` whose `DialContext` dispatches on URL scheme (`unix://` for the local server, `https://` for a remote one). Plain `http://` and `tcp://` are intentionally not supported.
  - `autospawn.go` — `DialAndConnect(ctx, opts, AutospawnConfig)`: probes Health first, then on unix-connect-failure removes stale-socket-iff-nothing-answers, fork-execs `batchq server` (via `SysProcAttr{Setsid: true}` so it survives the parent), polls Health up to `PollTimeout`. Tests inject `SpawnFunc` to stand up an in-process server without exec'ing a binary.
- `runner/` — `Runner` is a one-method interface (`Start() bool`). Two implementations, both REST clients.
  - `simple.go` — long-lived loop that calls `client.Claim` (atomic QUEUED→RUNNING transition) and runs jobs locally via `os/exec`. Enforces `max_procs` / `max_mem` / `max_walltime` and optionally cgroup v1/v2 (root-only). Spool dir under `$BATCHQ_HOME/spool/`.
  - `slurm.go` — one-shot reconciliation: claims via REST, hands off to SLURM via `sbatch` (PROXYQUEUED state), polls `squeue` / `sacct`, and reports terminal state back via REST. **The SLURM runner is the only component that talks to SLURM**, in both directions — this is a permanent deployment invariant: the batchq server and the SLURM head node may live on different clusters, so reconciliation never moves server-side.
- `web/` — `batchq web` subcommand. `server.go` is a tiny HTML renderer that consumes the REST `*client.Client` and feeds DTOs (via `dtoToJobDef`) into the unchanged `web/templates/*.html`. Listens on a unix socket or `host:port`.
- `support/` — shared utilities.
  - `paths.go` — `GetBatchqHome()` resolves `$BATCHQ_HOME` or `~/.batchq`.
  - `config.go` — typed TOML Config + `LoadConfig`. `Duration` wrapper decodes Go-duration strings (`"1m"`).
  - `backend.go` — `ParseBackend`, `SqlitePath`, `RemoteHTTPURL`, `IsLocal` for `[batchq] backend` URLs.
  - `utils.go` — `ExpandPathAbs`, `Contains`, `AmIRoot`, etc.

### Job state machine

Defined in `jobs/jobdef.go` as `StatusCode`:

`UNKNOWN → (USERHOLD | WAITING | QUEUED) → (RUNNING | PROXYQUEUED) → (CANCELED | SUCCESS | FAILED)`

- `USERHOLD` — submitted with `--hold`, waiting on manual release.
- `WAITING` — has an unmet `afterok` dependency.
- `QUEUED` — eligible to run; the runner picks these up.
- `RUNNING` — executing locally under the simple runner.
- `PROXYQUEUED` — handed off to SLURM; the SLURM runner is responsible for transitioning it to a terminal state.

The atomic claim endpoint (`POST /api/v1/runners/{id}/claim`) transitions QUEUED→RUNNING in one DB transaction, closing the FetchNext/StartJob race that existed in v1.

### `JobDef` and DTOs

`jobs/jobdef.go` defines the in-memory model. A job's resource requirements (`procs`, `mem`, `walltime`, `wd`, `stdout`, `stderr`, `script`, captured `env`, …) are stored as a list of `JobDefDetail` key/value pairs rather than struct fields, so adding a new attribute typically only means a new detail key. Runtime data (e.g. `slurm_jobid`, host info) lives in a parallel `RunningDetails` list. `JobDef` is immutable after submission: `AddDetail`/`AddAfterOk` are no-ops once `JobId` is set. Memory and walltime parsing/printing helpers (`ParseMemoryString`, `ParseWalltimeString`, `WalltimeToString`, etc.) all live here.

`api.JobDTO` is the wire representation. `api.JobToDef` rebuilds a `JobDef` from a DTO **but intentionally ignores Status** (the DTO carries the status string; callers parse it separately when they need a `StatusCode`). `web/server.go:dtoToJobDef` is the canonical example.

## CLI contract (preserved byte-for-byte)

`batchq submit` is consumed by downstream pipeline tools that parse its output. The v2 rewrite preserves the v1 contract:

- Subcommand shape: `batchq submit [flags] [script-path | -- inline command…]`, with stdin fallback when no positional arg / `-` is given.
- All current flags: `--name`, `-p/--procs`, `-m/--mem`, `-t/--walltime`, `--wd`, `--stdout`, `--stderr`, `--deps`, `--hold`, `--env`, `--slurm`.
- Optional metadata flags (additive in Phase 12): `--run-id <id>`, `--input <path>` (repeatable), `--output <path>` (repeatable). `#BATCHQ -run-id / -input / -output` directives mirror these.
- SBATCH header parsing under `--slurm`: `-c/--cpus-per-task`, `--mem`, `-t/--time`, `-J`, `-D`, `-o/-e` with `%j`→`%JOBID`, `--export=ALL`, `-d afterok:…`.
- Job ID format: UUID strings with hyphens.
- stdout, stderr, and exit codes match v1.

`%JOBID` is the canonical placeholder in stdout/stderr paths — the SLURM runner rewrites it to `%j` when generating its sbatch script.

`cmd/submit_compat_test.go` is the regression harness: it starts a real in-process server, drives `submitCmd.RunE` end-to-end with each flag combination, and asserts the resulting `JobDef` (fetched via `GET /jobs/{id}`) carries the expected details. Don't break it.

## Configuration

Loaded from `~/.batchq/config` (TOML, via `github.com/BurntSushi/toml`). `support/config.go` defines the typed `Config` struct; every knob is a named field accessed directly (`Config.Server.Listen`, `Config.JobDefaults.Procs`). Empty string / zero int / false bool / zero `support.Duration` all mean "not set" — call sites fall back to flag values or built-in defaults.

Resolution order for every knob: command-line flag > config value > built-in default. `cmd/run.go` shows the canonical pattern.

Sections:

- `[batchq]` — `runner` (`simple` | `slurm`), `backend` (URL), `token`, `multiuser`.
- `[server]` — `listen`, `lock`, `idle_timeout`, `sqlite_wal`. Ignored when `backend` is `batchq-remote://`.
- `[web]` — `socket`, `listen`.
- `[job_defaults]` — submit-time defaults: `name`, `procs`, `mem`, `walltime`, `wd`, `stdout`, `stderr`, `hold`, `env`.
- `[simple_runner]` — `max_procs`, `max_mem`, `max_walltime`, `shell`, `use_cgroup_v1`, `use_cgroup_v2`.
- `[slurm_runner]` — `user`, `account`, `partition`, `max_jobs`.

### Backend selector

The `[batchq] backend` URL (also `--backend` flag) drives where queue data lives and whether a local server runs:

- `sqlite3:///path/to/db` — local SQLite file; server runs locally and may be autospawned by clients.
- `postgres://user:pass@host/db` — local server on Postgres (future).
- `batchq-remote://host[:port]/path` — remote HTTPS REST API. No local server. Plain HTTP is intentionally not supported; operators terminate TLS at a reverse proxy.

`support/backend.go` (`ParseBackend`, `SqlitePath`, `RemoteHTTPURL`, `IsLocal`) is the canonical parser. `cmd/server.go` refuses to start when the backend is `batchq-remote://`.

Job IDs are UUID strings (with hyphens) — anywhere code splits on `-` or `:` it must account for this.

## Authentication / transport

- **Unix socket** (the only thing batchq binds today): created with mode `0600`. No in-band auth — FS permissions are the contract. This is how `gpg-agent` / `ssh-agent` work.
- **Network access**: served via a reverse proxy (nginx, Caddy, Traefik, ...) in front of the unix socket. The proxy terminates TLS and forwards to batchq; remote clients use `--backend batchq-remote://your-host` (or `batchq-remote://your-host/proxy/path` for a subpath deployment). The URL path is treated as a mount-point prefix — the client adds `/api/v1/...` to every request. Bearer-token auth (`Authorization: Bearer <token>` with tokens HMAC-signed against a `$BATCHQ_HOME/master.key`) is designed but not yet implemented — until it lands, remote deployments need to gate access at the proxy layer.

## Testing

Test seam patterns to be aware of:

- `client.AutospawnConfig.SpawnFunc` — let tests stand up an in-process server instead of exec'ing the binary. `client/autospawn_test.go:spawnInProcess` is the helper.
- `server.Options.OnIdleShutdown` — fires before the HTTP server shuts down; `server/lifecycle_test.go:TestIdleShutdownFires` uses this to synchronize on the idle event.
- `web/server_integration_test.go:startWebForTest` — mounts the web handlers on a unix socket against a real server+storage and returns an `http.Client` that dials it.

The `test.sh` script (untracked) is the end-to-end smoke runner: start server, submit, run, show, cleanup. Update it when the server lifecycle changes.
