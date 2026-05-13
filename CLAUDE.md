# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build, run, test

This is a Go module (Go 1.23) requiring `CGO_ENABLED=1` because of the `mattn/go-sqlite3` driver.

- Build for current host: `make` (produces `bin/batchq.linux`) or directly `CGO_ENABLED=1 go build -o bin/batchq main.go`.
- Run without building: `make run` or `CGO_ENABLED=1 go run main.go <subcommand>`.
- Cross-compile targets are defined in the `Makefile` (`bin/batchq.macos_amd64`, `bin/batchq.macos_arm64`, `bin/batchq.linux_musl_amd64`, `bin/batchq.linux_musl_aarch64`). The musl targets require `x86_64-linux-musl-gcc` / `aarch64-linux-musl-gcc` and produce static binaries.
- Tests: `go test ./...`. Single test: `go test ./cmd -run TestName`. Test coverage is currently limited (cleanup logic).
- The hidden `batchq debug` subcommand prints the resolved `batchq home`, config file, and dbpath — useful when diagnosing config loading.

## Architecture

`batchq` is a single-binary, SQLite-backed job queue. There is no long-running daemon by default — submission, scheduling, and execution all happen in transient invocations that coordinate purely through the database file.

### Top-level layout

- `main.go` — embeds `LICENSE` and hands off to `cmd.Execute()`.
- `cmd/` — Cobra subcommands (`submit`, `run`, `show`, `hold`, `cleanup`, `web`, plus hidden `debug` and `license`). `cmd/root.go` loads `~/.batchq/config` (overridable via `BATCHQ_HOME`) at init time into the package-level `Config`, and resolves `dbpath` (default `sqlite3://~/.batchq/batchq.db`).
- `jobs/jobdef.go` — `JobDef` is the in-memory model. A job's resource requirements (`procs`, `mem`, `walltime`, `wd`, `stdout`, `stderr`, `script`, captured `env`, ...) are stored as a list of `JobDefDetail` key/value pairs rather than struct fields, so adding a new attribute typically only means a new detail key. Runtime data (e.g. `slurm_jobid`, host info) lives in a parallel `RunningDetails` list. `JobDef` is immutable after submission: `AddDetail`/`AddAfterOk` are no-ops once `JobId` is set. Memory and walltime parsing/printing helpers (`ParseMemoryString`, `ParseWalltimeString`, `WalltimeToString`, etc.) all live here.
- `db/` — `db.BatchDB` is the storage interface; `sqlite.go` is the only implementation. `OpenDB` dispatches on the `sqlite3://` URL prefix, so adding a backend means a new prefix here. The interface is the contract the runners and CLI code program against — prefer adding methods there over reaching into sqlite directly.
- `runner/` — `Runner` is a one-method interface (`Start() bool`). Two implementations:
  - `simple.go` — executes jobs locally via `os/exec`, enforcing `max_procs` / `max_mem` / `max_walltime` and optionally cgroup v1/v2 (root-only). Tracks live children in `curJobs`. Spool/working files land under `~/.batchq/spool/`.
  - `slurm.go` — proxies jobs into SLURM via `sbatch`, polls `squeue`/`sacct`, and writes results back. Jobs in this path move through the `PROXYQUEUED` state instead of `RUNNING`, and dependencies are translated to `afterok:<slurm-id>`. Capped by `--slurm-max-jobs` / `[slurm_runner] max_jobs` so the local queue can hold far more than SLURM is allowed to see.
- `iniconfig/config.go` — INI-style config loader with env-var override support and `~` expansion. `GetBatchqHome()` resolves `$BATCHQ_HOME` or `~/.batchq`.
- `support/utils.go` — shared helpers (path expansion, slices, root check, etc.).
- `web/` — optional local web UI (`batchq web`, served on a unix socket or `host:port`), using `web/templates/*.html`.

### Job state machine

Defined in `jobs/jobdef.go` as `StatusCode`:

`UNKNOWN → (USERHOLD | WAITING | QUEUED) → (RUNNING | PROXYQUEUED) → (CANCELED | SUCCESS | FAILED)`

- `USERHOLD` — submitted with `--hold`, waiting on manual release.
- `WAITING` — has an unmet `afterok` dependency.
- `QUEUED` — eligible to run; the runner picks these up.
- `RUNNING` — executing locally under the simple runner.
- `PROXYQUEUED` — handed off to SLURM; the SLURM runner is responsible for transitioning it to a terminal state. The `DB` interface distinguishes start vs. proxy hand-off explicitly (`StartJob` vs. `ProxyQueueJob`, `EndJob` vs. `ProxyEndJob`) — pick the right pair when wiring a new runner.

### Submission paths

`cmd/submit.go` accepts a script path, a positional command, or stdin (when no TTY), and reads `[job_defaults]` from config for unset fields. `--slurm` parses `#SBATCH` headers from an existing SBATCH script and maps a documented subset (`-c/--cpus-per-task`, `--mem`, `-t/--time`, `-J`, `-D`, `-o/-e` with `%j` → `%JOBID`, `--export=ALL`, `-d afterok:...`) into `JobDef` details. `%JOBID` is the canonical placeholder in stdout/stderr paths — the SLURM runner rewrites it to `%j` again when generating its sbatch script.

### Concurrency / locking

There is no in-process coordinator across invocations. Concurrent CLI invocations rely on SQLite's own locking. `batchq_locking_wrapper` is an external bash helper that serializes calls behind a directory-based PID lock at `~/.batchq/lock/` — use it as the model when something needs to be globally serialized outside the DB.

## Configuration model

Resolution order for runner/job knobs is consistently: command-line flag > `[section]` in `~/.batchq/config` > built-in default. `cmd/run.go` shows the canonical pattern (check the flag value, then `Config.Get*(section, key)`, then fall back). Important sections:

- `[batchq]` — `runner` (`simple` | `slurm`), `dbpath`.
- `[job_defaults]` — defaults applied at submit time only (not enforced later).
- `[simple_runner]` — `max_procs`, `max_mem`, `max_walltime`, `shell`, `use_cgroup_v1`, `use_cgroup_v2`.
- `[slurm_runner]` — `user`, `account`, `partition`, `max_jobs`.

Job IDs are UUID strings (with hyphens) — anywhere code splits on `-` or `:` it must account for this.
