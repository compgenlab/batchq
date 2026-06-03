# Running jobs locally

`batchq run` is the runner. Without `--slurm` it is the **simple
runner**: a process that pulls queued jobs off the server one at a time
and executes them locally with `os/exec`. The simple runner is
appropriate for a workstation, a shared server, or a compute node that
you have logged into for the purpose of running batchq jobs.

For sending jobs into a SLURM cluster instead, see [SLURM](slurm.md).

## What "running a job" means

The simple runner does the following for each job it claims:

1. Calls `POST /api/v1/runners/{id}/claim` on the batchq server. This
   atomically picks the next eligible job and transitions it from
   `QUEUED` to `RUNNING` in a single database transaction (so two
   runners can never claim the same job).
2. Creates a per-job temp directory under `$BATCHQ_HOME/spool/`,
   writes the job's script into it, and `chmod +x`'s it.
3. Sets up the environment. If the job was submitted with `--env`, the
   captured environment replaces the runner's environment; otherwise
   the runner's environment is inherited.
4. Opens the job's stdout/stderr files (the configured `--stdout` /
   `--stderr` paths, with `%JOBID` substituted and a relative path
   anchored to the job's working directory).
5. Optionally wraps the process in a cgroup (v1 or v2) with the
   configured procs and memory limits — see below.
6. `exec`s the configured shell (default `/bin/bash`) on the script.
7. Waits for the process to exit and reports the outcome back via the
   REST API. A zero exit code is `SUCCESS`; non-zero is `FAILED`; a
   walltime overrun is `FAILED` with reason "walltime exceeded".
8. Cleans up the spool directory.

## Invocation

```sh
batchq run                                          # one pass over the queue, then exit
batchq run --forever                                # keep waiting for new jobs
batchq run --max-procs 4 --max-mem 16GB \           # enforce per-job resource ceilings
           --max-walltime 1-00:00:00 --forever
batchq run --max-jobs 100 --forever                 # exit after 100 jobs
```

By default `batchq run` makes one pass — it pulls and runs jobs until
the queue has nothing eligible, then exits. Pass `--forever` to keep
polling, which is what you want when you launch a runner under `tmux`
or screen on a compute node and want it to stay alive between job
submissions.

## Flags

| Flag | Default | Meaning |
|---|---|---|
| `--max-procs N` | from `[simple_runner] max_procs` | Per-job processor ceiling. Jobs requesting more sit in `QUEUED` and are skipped over by this runner. |
| `--max-mem MEM` | from `[simple_runner] max_mem` | Per-job memory ceiling. |
| `--max-walltime TIME` | from `[simple_runner] max_walltime` | Per-job walltime ceiling. |
| `--max-jobs N` | -1 (unlimited) | Exit after running this many jobs. |
| `--forever` | `false` | Keep polling for new jobs instead of exiting when the queue empties. |
| `--use-cgroupv1` | from `[simple_runner] use_cgroup_v1` | Enforce per-job procs/mem via cgroup v1. Requires root. |
| `--use-cgroupv2` | from `[simple_runner] use_cgroup_v2` | Enforce per-job procs/mem via cgroup v2. Requires root. |

The corresponding config keys live under `[simple_runner]` — see
[configuration](configuration.md).

## Resource ceilings vs. cgroup enforcement

`--max-procs`, `--max-mem`, and `--max-walltime` are *eligibility*
ceilings: a job whose requested resources exceed the ceiling will not
be claimed by this runner. They control what gets picked up, not what
a running job is actually limited to.

Two layers of enforcement happen once a job is running:

- **Walltime is always enforced.** If a job exceeds its requested
  walltime, the runner sends it SIGTERM (then SIGKILL) and reports
  `FAILED` with reason "walltime exceeded".
- **Procs and memory are only enforced if you opt into cgroups.** With
  `--use-cgroupv1` or `--use-cgroupv2`, the runner creates a per-job
  cgroup with the requested procs/mem limits, places the process into
  it, and lets the kernel enforce them. Without cgroups, the
  requested procs/mem are advisory — the runner trusts the job not to
  exceed them.

Cgroup enforcement requires the runner to be root (only root can
create cgroups). On a workstation you typically run without cgroups
and rely on jobs being well-behaved. On a shared server, a system
batchq running as root with `use_cgroup_v2 = true` gives you real
isolation.

## Running multiple jobs concurrently

A single `batchq run` invocation runs one job at a time. To run jobs
in parallel, start multiple runners. They all coordinate through the
server's atomic claim endpoint, so two runners can never grab the same
job. A simple pattern:

```sh
# In two terminals, or two tmux panes, or under nohup:
batchq run --max-procs 8 --max-mem 32GB --forever
batchq run --max-procs 8 --max-mem 32GB --forever
```

Or scale further by running one runner per compute node (each pointing
at the same `[batchq] remote` or sharing the same `$BATCHQ_HOME` if
the home directory is on a networked filesystem).

## The spool directory

The simple runner uses `$BATCHQ_HOME/spool/<job-id>/` as a per-job
scratch area: it holds the script file (`script.sh`), and any artifacts
the runner needs during execution. The directory is created on claim
and removed on completion.

Job stdout and stderr do **not** go into the spool directory — they go
to the configured `--stdout` / `--stderr` paths. The spool is purely
internal to the runner.

## Stopping a runner

Send the runner process SIGINT or SIGTERM. It finishes the job
currently in flight (if any) before exiting. If you want to stop the
in-flight job too, send a second signal.

To cancel a job from a *different* terminal:

```sh
batchq cancel <job-id>
```

The server marks the job `CANCELED`, and the runner notices on its
next status check (and signals the process if it is the one running
it).

## Where to go next

- [Submitting jobs](submitting-jobs.md) — what the runner picks up
  and what those `procs` / `mem` / `walltime` numbers mean at submit
  time.
- [SLURM](slurm.md) — the other runner: hand jobs off to a real
  cluster scheduler instead of running them locally.
- [Configuration](configuration.md) — every `[simple_runner]` knob in
  one place.
