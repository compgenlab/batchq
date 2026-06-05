# Overview

batchq is a job scheduler for asynchronous task execution. You submit
shell scripts (or one-line inline commands) into a queue, and a runner
picks them up and executes them.

It is designed for three kinds of use:

1. **A single workstation.** Submit jobs, walk away, come back later for
   the results. The server starts on demand and exits when idle.
2. **A shared server.** One batchq instance per host with the database
   on local disk; the server still comes and goes on demand unless an
   operator chooses to supervise it.
3. **An HPC cluster fronted by SLURM.** batchq sits between users and
   SLURM, holds a much larger backlog of jobs than the SLURM queue
   allows, and pushes them into SLURM as capacity becomes available.
   The server runs on the login or submit host with its database on a
   networked filesystem; it autospawns and idles out so it isn't a
   permanent daemon (which most clusters disallow).

A single binary, `batchq`, plays every role. The subcommand you invoke
decides whether you are running the server (`batchq server`), submitting
a job (`batchq submit`), or running queued jobs (`batchq run`). In normal
use you never invoke `batchq server` yourself — the CLI does it for you.

## Why batchq exists

Three motivations, in roughly the order they came up:

**A simple parallel queue on a single server.** Sometimes you just want
to run a bunch of jobs in parallel on one machine without standing up
SLURM. SLURM is a lot of operational weight to take on for a single
host, and most people who reach for it on a single server end up
running one node anyway. batchq fills that gap: install a binary, set
`max_procs` and `max_mem` to whatever the host can take, point a
runner at the queue. No `slurmctld`, no `slurmd`, no munge, no
database server.

**A test queue for [cgpipe](https://github.com/mbreese/cgpipe).**
cgpipe is a make-style tool for HPC pipelines — it figures out which
jobs need to run and submits them to a scheduler. During pipeline
development you usually don't want to test against a real cluster
queue (slow, contended, requires real cluster access). batchq is a
lightweight target for cgpipe to submit into locally: same submission
shape, same dependency semantics as the SLURM backend, but runs on
your laptop. When the pipeline works against batchq, it'll work
against SLURM.

**A throttled front-end for shared SLURM clusters.** SLURM itself will
happily hold tens of thousands of jobs — there is no built-in per-user
cap — but admins of shared clusters routinely set a per-user `MaxJobs`
policy because one user with a huge backlog can slow the scheduler
down for everyone else. So on a shared cluster, submitting a
ten-thousand-job pipeline directly is usually not allowed by the local
policy. The usual workaround is a wrapper script that throttles
submissions, but that wrapper has no durable memory, no concept of
dependencies between jobs, and falls over the moment the user's login
session ends. batchq is that wrapper, persisted: the SLURM runner
submits at a configurable rate, keeping SLURM busy but staying under
whatever per-user cap the cluster admins have set.

The same machinery also runs jobs locally on a workstation, so the same
submission scripts work everywhere — switching between a laptop and a
cluster is a config change, not a code change.

## Key concepts

**Job.** A unit of work: a script (file or inline), a working directory,
captured stdout/stderr paths, resource requirements (procs, memory,
walltime), and optional metadata (name, environment, dependencies).
Every job has a UUID job ID with hyphens.

**Queue.** The single ordered list of jobs the server owns. There is no
notion of multiple named queues — one batchq instance has one queue.

**Server.** The `batchq server` process. It owns the SQLite database
file and serves the REST API over a unix socket. Exactly one server
instance may run per `$BATCHQ_HOME` — that singleton is what makes the
database safe to put on a networked filesystem, where SQLite's
cross-process locking is unreliable. The server is short-lived by
default: a CLI client autospawns one with a one-minute idle timeout
when the socket is unreachable, so overlapping or rapid requests share
a single process without anyone running a long-lived daemon.

**Client.** Every other subcommand (`submit`, `queue`, `status`, `hold`,
`cancel`, `cleanup`, `run`, `web`, `search`, `stop`, …). Clients open the
server's unix socket and speak HTTP REST to it.

**Runner.** A long-running client that pulls jobs off the queue and
runs them. Two implementations ship today: the **simple runner** runs
jobs locally with `os/exec`, and the **SLURM runner** hands them off to
`sbatch` and reconciles state through `squeue` and `sacct`.

**`$BATCHQ_HOME`.** The directory where batchq keeps its config file,
the SQLite database, its unix socket, and the web UI's socket. Defaults
to `~/.batchq` but can be overridden — see
[installation](installation.md).

## Job state machine

Every job moves through one of these paths from submission to a terminal
state:

```
   submit ──┬──► USERHOLD ──┐
            │               ▼                              ┌──► RUNNING ─────┐
            └────────────► WAITING ──► QUEUED ─────────────┤                 ├──► SUCCESS
                                                           └──► PROXYQUEUED ─┤    FAILED
                                                                (SLURM)      │    CANCELED
                                                                             ▼
                                                                          terminal
```

- `USERHOLD` — submitted with `--hold`. Stays held until you run
  `batchq release <id>`, which moves it into `WAITING`.
- `WAITING` — sitting on dependency resolution. Becomes `QUEUED` as
  soon as every `afterok` parent has succeeded; a job with no
  dependencies passes through this state immediately.
- `QUEUED` — eligible to run. The next runner that calls claim picks
  this up.
- `RUNNING` — claimed by a runner. Local execution under the simple
  runner stays in this state until completion; the SLURM runner
  transitions immediately to `PROXYQUEUED` after `sbatch` returns.
- `PROXYQUEUED` — handed off to SLURM. The SLURM runner is polling
  `squeue` / `sacct` for the outcome.
- `CANCELED`, `SUCCESS`, `FAILED` — terminal. The job stays in the
  database until `batchq cleanup` removes it.

A failed dependency cancels every descendant. If you cancel a job, all
of its `afterok` dependents are cancelled with reason "parent canceled".

## What batchq does not do

- **It is not a cluster scheduler.** It does not allocate nodes, manage
  cgroups across machines, or know about hardware topology. For real
  cluster execution it delegates to SLURM.
- **It does not terminate TLS.** The server binds a unix socket
  (default) or a plain-HTTP TCP port; HTTPS for remote clients is a
  reverse proxy's job — see [Remote access](remote.md).
- **It is not multi-tenant in the database sense.** One server, one
  `$BATCHQ_HOME`, one shared queue and database file — not isolated
  per-user queues. Shared deployments are still supported: the server
  derives each unix-socket client's identity from kernel-attested peer
  credentials and gates hold/release/cancel on it (and, run as root, can
  execute each job under the submitter's account). Over the network it
  offers a **shared-token** gate (`[server] token`) — secure-enough for a
  single-user server, but one secret shared by all callers, *not*
  per-user identity. Token-bound, per-user auth lives in the managed
  server, not this binary. See [running jobs](running-jobs.md) and
  [remote access](remote.md).

## Where to go next

If you have never run batchq, start with [installation](installation.md)
and then read the [architecture](architecture.md) page to understand
what is running where. If you have batchq installed and want to put it
on a cluster, jump to [SLURM](slurm.md) and [remote access](remote.md).
