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

The motivating problem is HPC pipelines. SLURM and similar schedulers
cap how many jobs a user can have in the queue at once, so submitting a
ten-thousand-job pipeline directly is not allowed. The usual workaround
is a wrapper script that throttles submissions — but that wrapper has no
durable memory, no concept of dependencies between jobs, and falls over
the moment the user's login session ends.

batchq is that wrapper, persisted. A `batchq server` process holds the
full backlog in a SQLite database; the SLURM runner submits to SLURM at
a configurable rate, keeping the SLURM queue full but never over the
per-user cap.

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

**Client.** Every other subcommand (`submit`, `show`, `hold`, `cleanup`,
`run`, `web`, `search`, `stop`). Clients open the server's unix socket
and speak HTTP REST to it.

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
                   ┌─────────────────────────┐
                   │                         │
            ┌──► USERHOLD ──► QUEUED ──► RUNNING ──► SUCCESS
            │                  ▲             │
   submit ──┼──► WAITING ──────┘             ├──► FAILED
            │                                │
            └──► QUEUED ──► PROXYQUEUED ─────┴──► CANCELED
                            (SLURM)
```

- `USERHOLD` — submitted with `--hold`. Stays held until you run
  `batchq release`.
- `WAITING` — has an unmet `afterok` dependency. Becomes `QUEUED` as
  soon as the last parent finishes successfully.
- `QUEUED` — eligible to run. The runner will pick this up.
- `RUNNING` — executing locally under the simple runner.
- `PROXYQUEUED` — the SLURM runner has handed this job off to SLURM and
  is polling `squeue` / `sacct` for its outcome.
- `CANCELED`, `SUCCESS`, `FAILED` — terminal. The job stays in the
  database until `batchq cleanup` removes it.

A failed dependency cancels every descendant. If you cancel a job, all
of its `afterok` dependents are cancelled with reason "parent canceled".

## What batchq does not do

- **It is not a cluster scheduler.** It does not allocate nodes, manage
  cgroups across machines, or know about hardware topology. For real
  cluster execution it delegates to SLURM.
- **It does not own its own network listener.** The server only ever
  binds a unix socket. Remote access goes through a reverse proxy that
  terminates TLS — see [Remote access](remote.md).
- **It is not multi-tenant.** One server, one `$BATCHQ_HOME`, one
  database file. Shared deployments are supported by running the server
  as a system user that owns the socket, but there is no per-user
  authentication today.

## Where to go next

If you have never run batchq, start with [installation](installation.md)
and then read the [architecture](architecture.md) page to understand
what is running where. If you have batchq installed and want to put it
on a cluster, jump to [SLURM](slurm.md) and [remote access](remote.md).
