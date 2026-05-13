# batchq

batchq is a small job scheduler for queuing and running tasks. As of v2 it
runs as a client/server pair over a REST API: a `batchq server` process
owns the SQLite-backed queue, and `submit` / `run` / `show` / `hold` /
`cleanup` / `web` are all clients that talk to it over a unix socket
(default) or TCP.

This split exists so the database file can safely live on a networked
filesystem (NFS / Lustre) on an HPC cluster: only the server process
touches the file, eliminating the cross-process SQLite locking problems
that plague networked-FS deployments.

On a single workstation you typically never start the server explicitly —
the CLI auto-spawns one with a short idle timeout when the socket isn't
reachable, so `batchq submit ./script.sh` Just Works.

## Quick start

```sh
batchq submit ./script.sh        # submit (auto-spawns server if needed)
batchq run                       # run jobs with the simple runner
batchq show queue                # list queued/running jobs
```

Defaults:
- working directory: `.`
- stdout/stderr: `./batchq-%JOBID.stdout` and `./batchq-%JOBID.stderr` (if a directory is given, files are placed inside it)
- other defaults can be set under `[job_defaults]` in `~/.batchq/config`

Job IDs are UUID strings and may include hyphens.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Clients                                                     │
│ ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌──────────────────┐ │
│ │   CLI   │  │ runner  │  │ web UI  │  │ external clients │ │
│ └────┬────┘  └────┬────┘  └────┬────┘  └────────┬─────────┘ │
└──────┼────────────┼────────────┼────────────────┼───────────┘
       │            │            │                │
       └──── REST over unix socket or TCP ────────┘
                          │
                ┌─────────▼─────────┐
                │   batchq server    │
                │   ── SQLite DB ──  │
                └────────────────────┘
```

The server is the only process that opens the database file. The simple
runner stays long-lived and talks to the server over REST; the SLURM runner
shells out to `sbatch` / `sacct` / `squeue` on a SLURM head node and
reconciles state back into the server via REST. The two roles can run on
different hosts.

## Server

Default listener: `unix://$BATCHQ_HOME/server.sock` (mode `0600`).
Default storage: `$BATCHQ_HOME/batchq.db` (created on first open).

```sh
batchq server                          # foreground server on the unix socket
batchq server --listen tcp://0:8080    # TCP listener (put behind a TLS proxy)
batchq server --idle-timeout 5m        # exit if no requests for 5m
batchq server --sqlite-wal             # WAL mode (LOCAL-DISK ONLY; unsafe on NFS/Lustre)
```

Only one server instance may run per `$BATCHQ_HOME`. Election uses a
`flock` on `$BATCHQ_HOME/server.lock`; a second instance exits cleanly.

A typical CLI client will fork-exec `batchq server --idle-timeout 1m`
automatically when the unix socket isn't reachable. Pass
`--no-autospawn` to opt out of this behavior.

## Submitting jobs

```sh
batchq submit ./myjob.sh
batchq submit --name align --procs 8 --mem 16GB --walltime 1-00:00:00 ./align.sh
echo "echo hello" | batchq submit
```

Useful flags:
- `--name NAME` name the job
- `-p/--procs N`, `-m/--mem MEM`, `-t/--walltime D-HH:MM:SS`
- `--wd DIR` working directory
- `--stdout FILE`, `--stderr FILE` (supports `%JOBID`)
- `--deps <job-id>,<job-id>` run after other jobs succeed
- `--hold` submit held
- `--env` capture current environment and replay at run time

### Submitting SLURM scripts

If you already have an SBATCH script, pass `--slurm` to parse its headers:
```sh
batchq submit --slurm job.sbatch
```

Supported SBATCH directives:
- `-c/--cpus-per-task`, `--mem`, `-t/--time`, `-J/--job-name`, `-D/--chdir`
- `-o/--output`, `-e/--error` (`%j` is remapped to `%JOBID`)
- `--export=ALL` to capture environment
- `-d afterok:<job-ids>` for dependencies

## Running jobs

### Simple runner (local)
```sh
batchq run --max-procs 4 --max-mem 16GB --max-walltime 1-00:00:00 --forever
```
Config equivalents under `[simple_runner]`: `max_procs`, `max_mem`,
`max_walltime`, `use_cgroup_v1`, `use_cgroup_v2`, `shell`.

### SLURM runner (proxy to SLURM)
```sh
batchq run --slurm --slurm-user $USER --slurm-acct acct123 --slurm-max-jobs 200
```

Or set `[batchq] runner = slurm` and configure `[slurm_runner]`:
```
[slurm_runner]
user = myuser          # default: current user
account = acct123      # optional
max_jobs = 200         # cap of concurrent jobs visible to SLURM
```

Behavior:
- Submits queued batchq jobs to SLURM via `sbatch`.
- Caps in-flight SLURM jobs (`--slurm-max-jobs`) so you can queue many
  jobs locally without flooding SLURM.
- Tracks status via `squeue` / `sacct` and writes SLURM state, start/end
  times, and exit codes back to batchq.
- Translates batchq dependencies to `afterok:<slurm-id>`.
- If a job was submitted with `--env` or `#BATCHQ -env`, the captured
  environment is passed to SLURM.

## Configuration

Example `~/.batchq/config`:
```
[batchq]
runner = simple                       # or slurm

[server]
listen = unix:///home/me/.batchq/server.sock
storage = /home/me/.batchq/batchq.db
sqlite_wal = false                    # true ONLY on local disk
idle_timeout = 1m                     # 0 disables

[client]
url = unix:///home/me/.batchq/server.sock
token =                               # required for TCP

[job_defaults]
procs = 4
mem = 8GB
walltime = 2-00:00:00
wd = /workdir
stdout = /logs/batchq-%JOBID.out
stderr = /logs/batchq-%JOBID.err
hold = false
env = false

[simple_runner]
max_procs = 4
max_mem = 16GB
max_walltime = 1-00:00:00
use_cgroup_v2 = false
use_cgroup_v1 = false

[slurm_runner]
account = acct123
max_jobs = 200
```

Hidden helper: `batchq debug` prints the resolved `batchq home` and
config path.

## Building from source

batchq is pure Go (modernc.org/sqlite) — no CGO and no C toolchain.

```sh
go build -o bin/batchq main.go        # current host
make                                  # bin/batchq.linux
make bin/batchq.macos_arm64           # cross-compile on Linux, no toolchain prep
```

Tests: `go test ./...`
