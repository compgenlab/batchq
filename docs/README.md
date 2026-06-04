# batchq documentation

batchq is a small job scheduler for queuing and running tasks on a single
workstation, a shared server, or an HPC cluster fronted by SLURM. It runs
as a client/server pair: a `batchq server` process owns the queue
database, and every other subcommand is a client that talks to it over an
HTTP REST API on a unix domain socket.

The server is not a permanent daemon — many HPC clusters don't allow
those. Instead, any client that finds the socket unreachable transparently
spawns a server with a short idle timeout, so overlapping or rapid client
requests are funneled through a single process that owns the database
file. That serialization is the whole point on networked filesystems
(NFS, Lustre, …) where SQLite's cross-process file locking is unsafe.

These pages cover how batchq is put together and how to deploy and use it.

## Getting started

- [Overview](overview.md) — what batchq is, its core concepts, and the job
  state machine.
- [Installation](installation.md) — building from source, `BATCHQ_HOME`,
  and what happens on first run.

## How it works

- [Architecture](architecture.md) — the client/server split, the
  components in the repository, and how a job moves through the system.
- [Configuration](configuration.md) — the config file, environment
  variables, command-line flags, and every knob that ships today.

## Using batchq

- [Submitting jobs](submitting-jobs.md) — the `submit` command in depth:
  flags, dependencies, workflow tagging, and SBATCH header parsing.
- [Managing jobs and the queue](managing-jobs.md) — inspect, re-prioritize,
  hold, cancel, and clean up jobs (`queue`, `summary`, `search`, `status`,
  `details`, `top`, `nice`, `hold`, `release`, `cancel`, `cleanup`, `stop`).
- [Job arrays](job-arrays.md) — submit a range of indexed tasks with one
  command and manage them as a batch (hold / release / cancel).
- [Generic resources](resources.md) — request GPUs, licenses, and other
  named resources with `--resource`, and advertise them on runners.
- [Running jobs locally](running-jobs.md) — the simple runner: resource
  ceilings, cgroup enforcement, foreground vs. `--forever` modes.
- [The web UI](web.md) — what `batchq web` shows and how to reach it from
  another machine over SSH.

## Deployment

- [SLURM](slurm.md) — running batchq on top of a SLURM cluster, including
  the head-node split between the batchq server and the SLURM runner.
- [Remote access](remote.md) — exposing a batchq server to remote clients
  via a reverse proxy, with HTTPS and (eventually) bearer-token auth.

## A note on the README

The repository's top-level `README.md` is a short pitch and quick start.
These docs are intended to stand on their own — read them first if you are
deploying batchq, integrating with it, or trying to understand how it
works under the hood.
