# Architecture

batchq is a single binary that runs in one of two roles: a **server**
that owns the queue database, and short-lived **clients** that talk to
it. The split is a fundamental design choice, not an implementation
detail — it is what makes batchq safe to run with its database on a
networked filesystem and what lets the SLURM runner live on a different
host than the queue.

The server itself is also short-lived in normal use. It is not a
permanent daemon and you do not have to run one under a service
supervisor. Any client that finds the socket unreachable transparently
forks a server with a short idle timeout, and that one process serves
every concurrent client in its lifetime before exiting on its own. This
model exists because many HPC clusters forbid long-running user
processes, and because the *only* thing the server needs to guarantee
is that exactly one process is touching the database file at a time.

## The two roles

```
┌─────────────────────────────────────────────────────────────────────┐
│  Clients (one per process invocation)                               │
│  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐  │
│  │ submit │ │ queue  │ │  hold  │ │cleanup │ │  run   │ │  web   │  │
│  └────┬───┘ └───┬────┘ └───┬────┘ └───┬────┘ └───┬────┘ └───┬────┘  │
└───────┼─────────┼──────────┼──────────┼──────────┼──────────┼───────┘
        │         │          │          │          │          │
        └─────────┴──────────┴────┬─────┴──────────┴──────────┘
                                  │
                       HTTP REST over unix socket
                       ($BATCHQ_HOME/batchq.sock)
                                  │
                       ┌──────────▼──────────┐
                       │   batchq server     │
                       │  ┌──────────────┐   │
                       │  │  SQLite DB   │   │
                       │  └──────────────┘   │
                       └─────────────────────┘
```

The server is the only process that ever opens the database file. Every
other interaction — submitting a job, listing the queue, claiming a job
to run, releasing a hold — goes through the REST API. That contract is
the same whether the client and server are in the same process during a
test, on the same host through a unix socket in production, or on
different hosts through a reverse proxy.

## Why a server at all?

The blunt answer is: SQLite over NFS or Lustre is unsafe. SQLite's
cross-process locking relies on POSIX advisory locks, which network
filesystems implement with varying degrees of correctness. If two
processes on different cluster nodes both open the same database file —
or even two processes on the same node, on some filesystems — they will
eventually corrupt it.

Putting a single server in front of the database removes the problem.
The database file can live on a network filesystem (which is useful —
it survives a node reboot, it is backed up by your storage tier) while
all access to it happens through one process. Locking inside that one
process is fine, and overlapping client requests from different shells,
different cron jobs, or a runner submitting in parallel with a user
typing `batchq queue` are all funneled through the same in-process lock.
This is the case even when the server lives only for the duration of
those overlapping requests.

A secondary benefit: clients become trivial. They speak HTTP REST, they
do not need to know whether the server is local, remote, or in-process
for a test. The same code path serves all three.

## Unix sockets by default (opt-in TCP), never TLS

By default the server binds a unix domain socket — and that's the
recommended shape, for two reasons:

- **Authentication.** Filesystem permissions on the socket (mode `0600`,
  owned by the user running the server) plus kernel peer credentials are
  the access control mechanism. This is the same model `ssh-agent` and
  `gpg-agent` use, and it is dead simple to reason about.
- **Deployment.** Exposing the API to the network is a reverse proxy's
  job. nginx, Caddy, Traefik all forward HTTP to a unix socket without
  fuss; the proxy terminates TLS, applies network-level access
  controls, and forwards to batchq. See [remote access](remote.md).

For containers and orchestrators, where a host-path socket is awkward,
the server can instead bind a plain-HTTP **TCP port**
(`[server] listen = "tcp://host:port"`). A TCP port has no filesystem
ACL and no peer credentials, so it should be paired with a shared
`[server] token` (the server warns if it isn't) and/or a proxy. What the
server never does is terminate TLS — that stays the proxy's job in every
topology.

## Single-instance election

Only one server may be running per `$BATCHQ_HOME`. The unix socket
itself is the election token — there is no separate lock file.

When `batchq server` starts, it tries to `bind()` the socket. Three
things can happen:

1. The bind succeeds. We are the server. We unlink the socket cleanly
   on shutdown so the next start has nothing to recover from.
2. The bind fails with `EADDRINUSE`. We probe the socket with a short
   `net.Dial`. If the dial succeeds, a live server already owns the
   path — we exit with `ErrAlreadyRunning`.
3. The bind fails with `EADDRINUSE` but the probe fails (typically
   `ECONNREFUSED`, meaning the socket file is left over from a crashed
   server). We `unlink` the path and retry the `bind` exactly once.

The narrow race where two simultaneous recoveries both decide a socket
is stale resolves itself: the loser's retry hits `EADDRINUSE` against
the new winner and exits with `ErrAlreadyRunning`. We never
unconditionally unlink before binding, because that would let a racing
start clobber a live socket.

## Autospawn

Autospawn is the normal way a server starts in every deployment except
when an operator chooses to supervise one explicitly. Every CLI client
checks whether the socket is reachable; if not, it fork-execs
`batchq server --idle-timeout 1m` (with `SysProcAttr{Setsid: true}` so
the server survives the parent exiting) and polls the socket until it
comes up.

The autospawned server exits after its idle timeout passes with no
requests in flight. The result is that `batchq submit ./script.sh`
"just works" — on a workstation, on a shared server, on a cluster
login node — without anyone running a long-lived daemon. Overlapping
client requests in the same window share the spawned server; once they
stop, it goes away.

Pass `--no-autospawn` to opt out of this behavior. Pass `--restart-server`
to ask the local server to shut down before this command runs (no
effect when the configured backend is remote).

## The runner is a client

`batchq run` is a client too. The simple runner pulls jobs from the
server one at a time via `POST /api/v1/runners/{id}/claim` (an atomic
QUEUED→RUNNING transition done in a single database transaction), runs
each one with `os/exec`, and reports the outcome back through the REST
API. The server never reaches "out" to the runner — the runner pulls.

This pull-only model is what makes it possible to keep the SLURM runner
on a different host than the server. The SLURM runner needs network
access to the SLURM head node (for `sbatch`, `squeue`, `sacct`) and
HTTPS access to the batchq server through its reverse proxy. The batchq
server itself never needs to know about SLURM at all.

## Repository layout

The directories under the repository root map roughly to layers:

- `main.go` — embeds the LICENSE, hands off to `cmd.Execute()`.
- `cmd/` — Cobra subcommands. Every CLI verb lives here.
- `api/` — the wire contract: route constants and DTOs. Importable by
  both the server and the client.
- `client/` — the Go client. One method per REST endpoint plus the
  autospawn helper. Used by every in-repo subcommand.
- `server/` — the HTTP layer: route wiring, the listener (unix socket or
  TCP), the shared-token auth middleware, and the activity-tracking
  middleware that drives idle shutdown.
- `service/` — server-side business logic that wraps the storage layer
  with dependency resolution, queue ordering, atomic claim, hold and
  release, recursive cleanup. No HTTP knowledge here.
- `storage/` — persistence. A `Storage` interface and a
  `modernc.org/sqlite`-backed implementation. The schema is embedded
  and applied idempotently on every open.
- `runner/` — the two runner implementations. `simple.go` runs jobs
  locally; `slurm.go` submits to SLURM and reconciles state.
- `web/` — the `batchq web` subcommand. A small HTML renderer that
  consumes the same REST client as everything else.
- `jobs/` — the in-memory `JobDef` model and its detail/parsing
  helpers.
- `support/` — shared utilities: `$BATCHQ_HOME` resolution, the typed
  TOML config, env-var overrides, defaults, backend URL parsing.

The boundary between `service/` (business logic) and `server/` (HTTP)
is deliberate: tests can drive the service layer directly without
spinning up an HTTP server, and a hypothetical alternate transport
(gRPC, command-line IPC, anything) would not have to touch the
business logic.

## Job lifecycle through the components

A typical submission and execution looks like this:

1. **`batchq submit ./script.sh`** parses flags, builds a `JobDef`, opens
   the REST client, and POSTs to `/api/v1/jobs`.
2. The server's submit handler in `service/` writes the job to the
   database in state `QUEUED` (or `USERHOLD`, `WAITING` depending on
   flags) and returns the job ID.
3. **`batchq run`** opens the REST client and loops on
   `POST /api/v1/runners/{id}/claim`. The server atomically picks the
   next eligible job and transitions it `QUEUED → RUNNING`, returning
   the job DTO.
4. The runner executes the job, captures stdout and stderr, and reports
   the outcome back via the runner endpoints (`POST
   /api/v1/runners/{id}/jobs/{id}/end` and friends). The server writes
   the terminal state and resolves any waiting dependents.
5. **`batchq queue`** GETs `/api/v1/queue/jobs` and pretty-prints
   the result.

For SLURM the runner step is different: the runner submits to SLURM via
`sbatch`, moves the job to `PROXYQUEUED`, and then polls `squeue` /
`sacct` to report the final state. See [SLURM](slurm.md) for the
specifics.

## Where to go next

- [Configuration](configuration.md) — every knob, with sources and
  defaults.
- [SLURM](slurm.md) — how the SLURM runner fits into this picture, and
  why it is the only component that talks to SLURM.
- [Remote access](remote.md) — exposing the server to remote clients
  over a reverse proxy or a TCP port.
