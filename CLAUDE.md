# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build, run, test

This is a Go module (Go 1.23). batchq is **pure Go** — `modernc.org/sqlite` replaces `mattn/go-sqlite3`, so there is no CGO and no C cross-toolchain anywhere in the build.

- Build for current host: `go build -o bin/batchq main.go` (or `make`, which produces `bin/batchq.linux`).
- Cross-compile is just `GOOS=… GOARCH=… go build` — no toolchain prep. The `Makefile` has targets for `linux_amd64`, `linux_arm64`, `macos_amd64`, `macos_arm64`.
- Run without building: `make run` or `go run main.go <subcommand>`.
- Tests: `go test ./...`. Single test: `go test ./cmd -run TestName`.
- Version string: `cmd.Version` is set at build time via `-ldflags`. The Makefile resolves it from `git describe`: exact-tag commits get the tag (e.g. `v0.2.0`); anything else gets `v0.2.0-dev-<short-sha>`. Plain `go run` / `go build` (no ldflags) yields the literal `"dev"`. Bump the dev base in the Makefile when starting work on the next minor series. Surfaced via `batchq version`, `batchq --version`, and a footer line on every command's `--help`.
- The hidden `batchq debug` subcommand prints the resolved configuration: `$BATCHQ_HOME`, config file path, every env var that's consumed, and every knob in every config section with its source labelled (`flag` / `env` / `config` / `default` / `unset`). First stop when diagnosing config loading.
- The lifecycle debug log (`--log <file>` flag, or `[batchq] log` config) appends timestamped, PID-tagged lines from both clients and the servers they autospawn to one shared file: client `dial`/spawn/handoff-retry, server `start`/`election`/`db opened`/`req … -> status`/`shutdown begin|done`/`db closed`, and any 5xx with its error body. First stop when diagnosing "too many servers" / SQLITE_BUSY — overlapping server PIDs are visible at a glance. Implemented in `support/debuglog.go` (`DebugLogger`), wired via `cmd/clientconn.go:debugLog` (client + spawned-server `--log` passthrough) and `server` `Options.Logf` / `server/logging.go` (`withLogging`).
- A Docker-based Go toolchain wrapper at `/tmp/dgo` is available in the dev environment when no host `go` is installed; it shells out to the `mcr.microsoft.com/devcontainers/go:1-1.23` image with the repo bind-mounted.

## Architecture (v2 client/server)

`batchq` is a single binary that runs in two roles: a long-lived `batchq server` process that owns the SQLite database, and short-lived clients (`submit`, `run`, `queue`, `hold`, `cleanup`, `web`) that talk to it over an HTTP REST API on a unix domain socket (or, opt-in, a plain-HTTP TCP port — `tcp://host:port` — for containerized deployments). batchq never terminates TLS; network exposure (TLS, remote clients, runners on other hosts) is the reverse proxy's job. This split exists so the DB file can safely live on a networked filesystem (NFS / Lustre): only one process touches it, so SQLite's cross-process locking failure modes don't apply.

On a workstation, a CLI client transparently fork-execs `batchq server --idle-timeout 1m` when the socket is unreachable, then polls for it to come up. The server idles out when no requests arrive — so `batchq submit ./script.sh` still Just Works without explicit server management.

The conventional layout under `$BATCHQ_HOME` (default `~/.batchq`):

- `config` — TOML config file.
- `batchq.db` — SQLite queue database (default `[server] db` target).
- `batchq.sock` — server's unix socket (default `[server] listen`). Also the single-instance election token — see `server/server.go:listenUnix`.
- `batchq-web.sock` — `batchq web` UI socket (default `[web] socket`).
- `spool/` — simple-runner per-job temp dir.

### Top-level layout

- `main.go` — embeds `LICENSE` and hands off to `cmd.Execute()`.
- `cmd/` — Cobra subcommands.
  - `root.go` loads `~/.batchq/config` (overridable via `BATCHQ_HOME`), then layers env-var overrides and built-in defaults on top. The resolved `Config` is exposed via the package-level `*support.Config`; the raw TOML-loaded copy is retained in `rawConfig` purely for the debug command's source labelling. `submit`/`queue`/`hold`/`cleanup`/`run`/`web` all open a REST client via `cmd/clientconn.go:dialClient()`.
  - `debug.go` — renders `batchq debug`. Compares `rawConfig`, `envOverrides`, `defaultsResolved`, and persistent flag vars to label each value's source. Token values are redacted.
  - `server.go` is the server entrypoint; flags: `--listen`, `--db`, `--sqlite-wal`, `--idle-timeout`. The DB URL (sqlite3 path) comes from `--db` / `[server] db` config. Refuses to start when `[batchq] remote` is set.
  - `clientconn.go` — `dialClient()` picks the API endpoint: if `[batchq] remote` (or `--remote`) is set, dial that HTTPS URL with no autospawn; otherwise dial `Config.Server.Listen` (the local unix socket) and autospawn if nothing is answering. `--no-autospawn` opts out of the autospawn path.
- `api/` — shared REST contract: route constants (`api/routes.go`) and request/response DTOs (`api/types.go`). `JobDTO` is the wire format; `api.JobToDef` / `api.JobFromDef` bridge to `jobs.JobDef`.
- `storage/` — persistence layer. `Storage` interface + `sqliteStorage` impl using `modernc.org/sqlite`. `Open(ctx, path, Options)` creates the file and applies `schema.sql` (embedded) idempotently on every open. Journal mode defaults to `DELETE` (rollback) because WAL's `-shm` shared-memory file is unsafe on NFS/Lustre; `Options{WAL: true}` opts into WAL when the DB is on local disk.
  - **All DB access decouples from the request context's cancellation** (`context.WithoutCancel`): reads go through `qRows`/`qRow`/`qExec`, writes through `beginTx` (which also decouples) plus a per-method `ctx = context.WithoutCancel(ctx)`. This is REQUIRED, not an optimization. `*sql.DB` is a pool capped at one connection (`SetMaxOpenConns(1)`); the cap serializes operations but doesn't pin the connection's identity — a *canceled* query (a client disconnecting mid-request) makes the pool discard that connection and open a fresh one. On a networked FS the discarded connection's SQLite file lock lingers during its slow teardown, so for a moment two connections hold locks on the same DB file and the new connection's write fails with `SQLITE_BUSY` — even with a single server running. Decoupling cancellation means no query is ever interrupted, so the pool keeps the *same* single connection for the server's lifetime and the 1-connection cap is the only serializer needed. Regression guard: `sqlite_test.go:TestOperationsIgnoreRequestCancellation`. (If churn ever recurs, the heavier fix is pinning a dedicated `*sql.Conn` for the process lifetime.)
  - **Reads vs writes: split pools (`Options.ReadPoolSize` / `[server] read_pool_size` / `--read-pool-size`).** `Open` always builds a single serialized **writer** (`s.db`, `SetMaxOpenConns(1)` — schema applied here, all `qExec`/`beginTx` use it). Reads (`qRows`/`qRow`) go through `s.readDB`. Default (`ReadPoolSize <= 1`): `readDB == db` — reads share the one writer connection, byte-for-byte the historical single-connection behavior. `ReadPoolSize > 1`: a SEPARATE read pool of that size on the same DSN, so concurrent readers (each takes a SHARED lock; readers never block readers) aren't serialized behind one another or a write. The split is clean because the helpers are the seam — reads inside a write transaction use `tx.*` and correctly stay on the writer. **Trade-off:** `>1` re-introduces reader↔writer SQLite lock contention (bounded by `busy_timeout`; SQLite's `PENDING` lock prevents writer starvation) and more `fcntl` locking on NFS — set back to `1` to revert. A single writer + read-only readers needs no `IMMEDIATE` transactions (no two connections both upgrade from SHARED, so no deadlock). Tests: `TestReadPoolDefaultSharesWriterConnection`, `TestReadPoolSeparateAndConsistent`.
- `service/` — server-side business logic that wraps `Storage`: dep resolution, queue ordering, atomic claim, hold/release, cleanup recursion. No HTTP knowledge here.
- `server/` — HTTP layer.
  - `server.go` wires service handlers into an `http.ServeMux`, listens on a unix socket (mode `0600`), runs the activity-tracking middleware, runs the idle monitor, and shuts down cleanly on context cancel.
  - Single-instance election: the unix socket *is* the election token — no separate lock file. `listenUnix` tries `bind()`; on `EADDRINUSE` it probes the socket with a short `net.Dial`. If the probe succeeds, a live server already owns the path and we return `ErrAlreadyRunning`. If the probe fails (`ECONNREFUSED` / stale leftover from a crashed process) we `unlink` and retry the bind exactly once. We never unconditionally unlink before binding — that would let a racing start clobber a live socket. The narrow race where two simultaneous recoveries both decide a socket is stale resolves itself: the loser's retry hits `EADDRINUSE` against the new winner and exits with `ErrAlreadyRunning`. The clean shutdown path unlinks the socket so the next start has no recovery to do.
  - Idle shutdown: `withActivity` middleware bumps `lastActivityNanos` on both request entry and exit and tracks `inFlight`; `runIdleMonitor` ticks at `IdleCheckInterval` and triggers shutdown when both the age threshold is reached AND `inFlight == 0`.
  - Ordered teardown — *the socket is the lock, so it is released last*. All shutdown triggers (idle monitor, caller-context cancel, ownership monitor, `/admin/shutdown`) funnel through `server.go:shutdown(ctx, reason, abortIfBusy)`, which closes the DB (`Options.OnShutdown`, wired to `Storage.Close` in `cmd/server.go`) **before** the unix socket is unlinked. The socket no longer auto-unlinks on `listener.Close()` (`electUnix` calls `SetUnlinkOnClose(false)`); `cleanupSocket` is the sole unlink point and runs after `Serve` returns. So a freshly-autospawned server cannot bind the socket — and therefore cannot open the DB — until the dying server has dropped it. This closes the idle-cull → respawn handoff window that otherwise let two servers touch the DB at once and report SQLITE_BUSY. A `draining atomic.Bool` admission gate (set under `shutdownMu` before reading `inFlight`; mirrored by the middleware's `inFlight++`-then-read-`draining`) rejects requests arriving mid-cull with `503 + api.HeaderDraining` so they touch no DB; the idle path aborts the cull (`errCullAborted`) if a request slipped in. Clients recover transparently — see `client/client.go:do` retry below.
- `client/` — REST client used by every in-repo client.
  - `client.go` — one method per API endpoint over an `http.Client` whose transport dispatches on URL scheme: `unix://` (local socket), `tcp://`/`http://` (plain HTTP over a TCP port — a containerized local server on a trusted network), and `https://` (remote, typically a reverse proxy). `[batchq] remote` stays https-only (`ParseRemote`); the `tcp://` path is reached via `[server] listen`. `do` is a reconnect-and-retry wrapper over the single-shot `doOnce`: for local autospawn-capable clients (`c.auto.Enabled`, set by `DialAndConnect`) it retries a request that hit an idle-server handoff — a draining `503` (`api.HeaderDraining`) or a connect failure, both of which mean the server did *no* work, so even a non-idempotent submit is safe to replay — backing off `5s/10s/30s` (`handoffBackoffs`) and calling `ensureUp` to respawn a fresh server between tries. `Health`/`HealthStatus`/`Shutdown` use `doOnce` directly (no retry) since `ensureUp` builds on them. The caller's context must budget for the backoff — mutating CLI calls use `cmd/clientconn.go:cmdContextRetryable` (2 min) instead of the 30s `cmdContext`.
  - `autospawn.go` — `DialAndConnect(ctx, opts, AutospawnConfig)` records the config on the client and calls `ensureUp`. `ensureUp` (idempotent; also called by `do` to recover from a handoff) probes Health first, then on unix-connect-failure removes stale-socket-iff-nothing-answers, fork-execs `batchq server` (via `SysProcAttr{Setsid: true}` so it survives the parent), polls Health up to `PollTimeout`. Tests inject `SpawnFunc` to stand up an in-process server without exec'ing a binary.
- `runner/` — `Runner` is a one-method interface (`Start() bool`). Two implementations, both REST clients.
  - `simple.go` — long-lived loop that calls `client.Claim` (atomic QUEUED→RUNNING transition) and runs jobs locally via `os/exec`. Enforces `max_procs` / `max_mem` / `max_walltime` and optionally cgroup v1/v2 (root-only). Spool dir under `$BATCHQ_HOME/spool/`.
  - `slurm.go` — one-shot reconciliation: claims via REST, hands off to SLURM via `sbatch` (PROXYQUEUED state), polls `squeue` / `sacct`, and reports terminal state back via REST. **The SLURM runner is the only component that talks to SLURM**, in both directions — this is a permanent deployment invariant: the batchq server and the SLURM head node may live on different clusters, so reconciliation never moves server-side.
- `web/` — `batchq web` subcommand. `server.go` is a tiny HTML renderer that consumes the REST `*client.Client` and feeds DTOs (via `dtoToJobDef`) into the unchanged `web/templates/*.html`. Listens on a unix socket or `host:port`.
- `support/` — shared utilities.
  - `paths.go` — `GetBatchqHome()` resolves `$BATCHQ_HOME` or `~/.batchq`.
  - `defaults.go` — `NewDefaults()` is the single source of truth for every home-relative built-in default (`Backend`, `ServerListen`, `ServerLock`, `WebSocket`, `ConfigFile`, plus `Shell`, `AutospawnIdleTimeout`, `JobStdout`/`Stderr`/`Wd`). Anything that needs a default should pull from here rather than inlining a path or string.
  - `config.go` — typed TOML Config + `LoadConfig` (pure TOML parse) + `Config.Clone` / `Config.ApplyEnv(EnvOverrides)` / `Config.ApplyDefaults(Defaults)`. `Duration` wrapper decodes Go-duration strings (`"1m"`). The expected init pattern is *load → clone (for debug) → ApplyEnv → ApplyDefaults*; after that, call sites just read `Config.X` directly and the fallback chain is gone.
  - `backend.go` — `ParseBackend` / `SqlitePath` for `[server] db` URLs (local sqlite3/postgres only), plus `ParseRemote` for `[batchq] remote` (https-only client URL).
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
- Generic required resources: `--resource <name[=value]>` (repeatable), mirrored by `#BATCHQ -resource`. Stored as `job_details` keys under the `resource.` prefix (`jobs.ResourcePrefix`). Count-vs-label is inferred from the value: an integer value (`gpu=2`, `gpu:a100=2`) is a countable requirement matched with `>=`; a non-integer value (`cluster=xyz`) is a label matched by set membership; a bare `name` is a feature flag. `procs`/`mem`/`walltime` are reserved names (rejected — use `-p/-m/-t`). Runners advertise what they have via `[simple_runner]`/`[slurm_runner] resources` config or `batchq run --resource`; the storage-side `jobFitsResources` (in the atomic claim) does the matching. A runner advertising nothing only claims jobs that require nothing. The claim response distinguishes `MoreEligible` (a job fit the runner's limits but it lost the claim race — retry) from `Blocked` (queued jobs don't fit the runner's limits/resources); a non-`--forever` runner that is idle (no running jobs, so its offered limits equal full capacity) stops instead of polling forever for `Blocked` jobs it can never satisfy.
- Job arrays: `--array <spec>` (`0-99`, `1-10:2`, `1,3,5`, `0-99%N` throttle), mirrored by `#BATCHQ -array` and `#SBATCH --array=`. One `submit --array` expands (via `POST /jobs/array` → `service.SubmitArray` → `storage.InsertArray`, one transaction) into N task-jobs sharing an `array_id` detail (each also carries `array_index`/`array_size`/`array_throttle`); it prints the single `array_id` (matches `sbatch --parsable`). Tasks are plain job rows — the per-job claim/state machine is unchanged. Output placeholders: `%A`→array id, `%a`→array index, substituted at **run time** (the simple runner; the SLURM runner passes them through to sbatch's own `%A`/`%a`) so one `sbatch --array` -o/-e pattern works; a `%JOBID` in an array path is rewritten to `%A_%a` at submit. The simple runner injects the task index under `BATCHQ_ARRAY_TASK_ID` plus `SLURM_ARRAY_TASK_ID`/`PBS_ARRAY_INDEX`/`SGE_TASK_ID` aliases, and honors `%N` via a per-array RUNNING cap in `ClaimNextJob`. The SLURM runner claims a whole array via `storage.ClaimNextArrayBatch` (budget-bounded by `max_slurm_jobs`/`max_jobs` so a 1000-task array is drip-fed, not dumped), submits one `sbatch --array=<indices>%N`, records `slurm_array_id`+`slurm_task_index` per task, and reconciles all tasks from one `sacct -j <arrayid>` (`SlurmGetArrayState`). Array dependencies: `--deps afterok:<id>` (B waits for all of array A's tasks) and `aftercorr:<id>` (B[i] waits for A[i]; array submit only, matching index sets), also `#BATCHQ -aftercorr` / `#SBATCH -d aftercorr:…` — resolved server-side into per-task `job_deps` edges (existing `ResolveDependencies`/`cascadeCancel`, no core change). `batchq queue --array-id <id>` filters to one array's tasks (mirrors `--run-id`); the web UI adds an array column/link on the queue and an "Array" tab (per-status progress) on the job page.
- Array batch management: `cancel`/`hold`/`release`/`status`/`details` auto-detect their id argument (`cmd/idresolve.go:resolveTarget`): a bare id is tried as a job, then as an array id, so `batchq cancel <arrayid>` (the id `submit` printed) acts on the whole array; `<array_id>_<index>` (SLURM-style; array ids are UUIDs with no `_`, so the last `_` splits cleanly) targets one task. Whole-array ops go through atomic `POST /arrays/{array_id}/{cancel,hold,release}` → `service`/`storage` `CancelArray`/`HoldArray`/`ReleaseArray` (one statement/transaction over the members). `cancel` also runs `scancel <slurm_array_id>` (whole array) or `scancel <slurm_array_id>_<index>` (one task) — mirrors the single-job `scancel <slurm_job_id>`.
- Simple-runner wakeups: the run loop waits on an always-available buffered `wakeup` channel (`runner/simple.go`) rather than a nil-able `interrupt` cancel, so a finishing job re-polls the loop immediately (the old race made quick jobs — e.g. array tasks — wait the full poll interval). Fallback heartbeat is 15s.
- SBATCH header parsing under `--slurm`: `-c/--cpus-per-task`, `--mem`, `-t/--time`, `-J`, `-D`, `-o/-e` with `%j`→`%JOBID`, `--export=ALL`, `-d afterok:…`/`aftercorr:…`, `--array=<spec>`, `--gres=name[:type][:count]` → `resource.name[:type]=count`, `-C/--constraint=feat&feat` → bare `resource.feat` flags (simple AND forms only).
- Job ID format: UUID strings with hyphens.
- stdout, stderr, and exit codes match v1.

`%JOBID` is the canonical placeholder in stdout/stderr paths — the SLURM runner rewrites it to `%j` when generating its sbatch script.

`cmd/submit_compat_test.go` is the regression harness: it starts a real in-process server, drives `submitCmd.RunE` end-to-end with each flag combination, and asserts the resulting `JobDef` (fetched via `GET /jobs/{id}`) carries the expected details. Don't break it.

## Configuration

Loaded from `~/.batchq/config` (TOML, via `github.com/BurntSushi/toml`). `support/config.go` defines the typed `Config` struct; every knob is a named field accessed directly (`Config.Server.Listen`, `Config.JobDefaults.Procs`).

**Resolution order** for every knob: command-line flag > env var > config value > built-in default.

The lower three layers (env, config, default) are folded into a single resolved `Config` during `cmd/root.go:init` via `LoadConfig → ApplyEnv → ApplyDefaults`. After that, call sites just read `Config.X` and get the answer — *no per-site fallback chain*. Flags are layered on top at the call site where they live (typically as `if flag != "" { use flag } else { use Config.X }`).

For knobs that have a built-in default (`Server.DB`, `Server.Listen`, `Web.Socket`, `Batchq.Runner`, `Batchq.AutospawnWaitTimeout`, `JobDefaults.Stdout`/`Stderr`/`Wd`, `SimpleRunner.Shell`), `Config.X` is never empty after init. For optional knobs (numeric/bool fields, `Batchq.Remote`, `JobDefaults.Procs/Mem/Walltime/Name`, `SlurmRunner.User/Account/...`), zero values still mean "not set" and call sites should treat them that way.

**Env-var overrides**: two secret-bearing vars are consumed today — `BATCHQ_TOKEN` overrides `[batchq] token` (the client's bearer token) and `BATCHQ_SERVER_TOKEN` overrides `[server] token` (the server's required shared token) — both kept in env so they stay out of argv and out of a checked-in config. `BATCHQ_HOME` is consumed earlier, at `support.GetBatchqHome()`, and shifts every home-relative default. Adding another env var means extending `support.EnvOverrides` and `Config.ApplyEnv`; the debug command picks it up automatically once you add a row.

Sections:

- `[batchq]` — `runner` (`simple` | `slurm`), `remote` (HTTPS URL of remote API server, optional), `token`, `multiuser`, `autospawn_wait_timeout`, `log` (lifecycle debug-log file path; `--log` flag overrides).
- `[server]` — `listen` (`unix:///path` default, or `tcp://host:port` for a plain-HTTP TCP port; `server/server.go:listenTCP`), `db` (local sqlite3 / postgres URL), `idle_timeout`, `sqlite_wal`, `read_pool_size` (concurrent-read connections; default 1 = share the single writer connection; `--read-pool-size` overrides), `token` (shared bearer token; when set, the server's `withAuth` middleware requires `Authorization: Bearer <token>` on every request except `/healthz`). Ignored when `[batchq] remote` is set.
- `[web]` — `socket`, `listen`.
- `[job_defaults]` — submit-time defaults: `name`, `procs`, `mem`, `walltime`, `wd`, `stdout`, `stderr`, `hold`, `env`.
- `[simple_runner]` — `max_procs`, `max_mem`, `max_walltime`, `shell`, `use_cgroup_v1`, `use_cgroup_v2`, plus `[simple_runner.resources]` (a `name = "value"` subtable advertising generic resources; counts are integer strings, labels are plain/comma-set strings). Counts are consumed by running jobs (the runner advertises pool-minus-running each claim); labels are advertised as-is.
- `[slurm_runner]` — `user`, `account`, `partition`, `max_jobs` (per-invocation submit cap; `--max-jobs` flag), `max_slurm_jobs` (cap on this user's live SLURM-queue jobs; `--slurm-max-jobs` flag), plus `[slurm_runner.resources]` (typically cluster/feature labels so resource-tagged jobs route to the right cluster; procs/mem/walltime stay SLURM-enforced).

### Local vs remote

Two orthogonal config keys decide where data lives and how clients reach it:

- `[server] db` (also `--db` on `batchq server`) — the **server-side** database URL. Local-only:
  - `sqlite3:///path/to/db` — local SQLite file.
  - `postgres://user:pass@host/db` — local server on Postgres (future).
- `[batchq] remote` (also `--remote` on every client) — optional **client-side** URL of a remote batchq server. When set, clients dial this HTTPS URL directly and no local server is involved (and `batchq server` refuses to start). When unset, clients use the local `[server] listen` unix socket and may autospawn a server.
  - Form: `https://host[:port]/subpath`. Default port `443`. Only `https://` is accepted — plain HTTP exposure of the REST API is not supported; operators terminate TLS at a reverse proxy.
  - The URL path is treated as a mount-point prefix — the client appends `/api/v1/...` to it.

`support/backend.go` exposes `ParseBackend` / `SqlitePath` for `[server] db` and `ParseRemote` for `[batchq] remote`.

Job IDs are UUID strings (with hyphens) — anywhere code splits on `-` or `:` it must account for this.

## Authentication / transport

- **Unix socket** (the default bind): created with mode `0600`. Without `[server] token`, FS permissions + kernel peer credentials are the contract — no in-band auth. This is how `gpg-agent` / `ssh-agent` work.
- **TCP port** (`tcp://host:port`, opt-in): plain HTTP, for containers/orchestrators. No filesystem ACL and **no peer credentials**, so a TCP-bound server should set `[server] token` (`cmd/server.go` warns when it doesn't). Still no TLS — front it with a proxy for untrusted networks.
- **Network access**: served via a reverse proxy (nginx, Caddy, Traefik, ...) in front of the unix socket. The proxy terminates TLS and forwards to batchq; remote clients set `[batchq] remote = "https://your-host"` (or `"https://your-host/proxy/path"` for a subpath deployment). The URL path is treated as a mount-point prefix — the client adds `/api/v1/...` to every request.
- **Shared-token auth** (implemented): set `[server] token` (or `BATCHQ_SERVER_TOKEN`) and the server's `withAuth` middleware (`server/auth.go`) requires `Authorization: Bearer <token>` on every request except `/healthz` (constant-time sha256 compare; `HeaderInternalOwner` self-dials are exempt). It's a single shared secret — a secure-enough floor for a self-hosted single-user server, **not** per-user auth. Real per-user identity (tokens bound to a user, validated server-side and fed into the hold/release/cancel authz checks; the HMAC-against-`master.key` design) belongs in the managed Postgres-backed server, not the single binary.
- **Client token plumbing**: `--token` flag > `BATCHQ_TOKEN` env > `[batchq] token` config. The client sends the bearer header on *every* transport when a token is set — over https to a remote server and over the unix socket to a local one (so a local runner can authenticate to a token-protected server). The env var is the recommended slot — it stays out of `ps` listings and out of any checked-in TOML.

## Testing

Test seam patterns to be aware of:

- `client.AutospawnConfig.SpawnFunc` — let tests stand up an in-process server instead of exec'ing the binary. `client/autospawn_test.go:spawnInProcess` is the helper.
- `server.Options.OnIdleShutdown` — fires before the HTTP server shuts down; `server/lifecycle_test.go:TestIdleShutdownFires` uses this to synchronize on the idle event.
- `web/server_integration_test.go:startWebForTest` — mounts the web handlers on a unix socket against a real server+storage and returns an `http.Client` that dials it.

The `test.sh` script (untracked) is the end-to-end smoke runner: start server, submit, run, show, cleanup. Update it when the server lifecycle changes.
