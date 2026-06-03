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

## Three ways to drive it

You can run `batchq run` in any of three shapes. The work it does for
each claimed job is the same in every case — the difference is how
often it loops and who starts it.

Unlike the SLURM runner, the simple runner is what actually executes
the jobs, so on hardware you control `--forever` is usually the
natural shape: the runner sits on the compute host, waiting for work.
The other two shapes still come up.

### Manually, from a shell

Type the command at a prompt and watch it run:

```sh
batchq run --max-procs 4 --max-mem 16GB
```

The runner drains every eligible job in the queue, one at a time, then
exits when there is nothing left to claim. You get your prompt back
when it's done.

This is the right shape for ad-hoc work: you submitted a couple of
jobs and want to run them now without leaving a process behind,
debugging a job that keeps failing, or running batchq inside an
interactive HPC allocation where you want full control of when the
runner stops.

### Continuously, with `--forever`

```sh
batchq run --forever --max-procs 8 --max-mem 32GB --max-walltime 1-00:00:00
```

The runner stays alive between submissions, polling for new work as
the queue fills. This is the typical shape on a workstation, a
dedicated job host, or a compute node you have allocated for a session
of work.

Run it under `tmux`, `screen`, or `nohup` so it survives your SSH
session ending. Send it SIGINT or SIGTERM to stop it gracefully —
it finishes the in-flight job (if any) and exits.

This is generally the recommended shape for the simple runner, with
two caveats:

- **Don't do this on a shared HPC login or submit node.** The simple
  runner shouldn't be running there in the first place (the actual
  job execution belongs on a compute node), but if you find yourself
  reaching for it on a login node, use the cron pattern below
  instead — most cluster sysadmins frown on long-running user
  processes there.
- **Inside an HPC allocation, mind your walltime.** If you started
  `batchq run --forever` inside an `salloc` or interactive job, it
  will be killed when the allocation ends. Pair it with `--max-jobs N`
  or arrange to stop it cleanly before the allocation expires.

### From cron

If you can't (or won't) leave a process running but still want batchq
to drain the queue periodically, the same cron + lockfile pattern
that works for the SLURM runner works here:

```sh
#!/bin/bash
LOCK=$HOME/jobs/batchq_runner.lock
LOG=$HOME/jobs/batchq_runner.log

if [ ! -e "$LOCK" ]; then
    echo $$ > "$LOCK"
    {
        echo "--"
        date
        batchq run --max-procs 4 --max-mem 16GB --max-jobs 10
    } &>> "$LOG"
    rm "$LOCK"
fi
```

```cron
*/5 * * * * ~/jobs/batchq_runner.sh
```

This is the right shape when long-running user processes are
disallowed on the host but jobs are short and infrequent enough that a
five-minute cadence is fine. Use `--max-jobs` to cap how much work
each tick does, so one cron-launched runner can't sit there for hours
draining a huge backlog. The lockfile keeps the next cron tick from
starting a second runner while the previous one is still in flight.

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
and rely on jobs being well-behaved. Running the runner as root to
get cgroup enforcement on a shared host is possible, but it has
real caveats — see [Running as root in a multi-user deployment](#running-as-root-in-a-multi-user-deployment)
below.

## Running as root in a multi-user deployment

The simple runner does support being run as root, in which case it
setuid/setgid's to the submitting user before exec'ing each job.
**You almost certainly should not do this.** There is exactly one
good reason to reach for it: cgroup-enforced procs and memory limits,
which the kernel won't let you set without privilege. If you can live
with advisory resource ceilings (the runner refuses to claim oversized
jobs, but trusts running jobs not to exceed their declared limits),
run a per-user runner instead and let normal Unix accounts give you
isolation for free.

### How the runner runs jobs under the submitter's identity

- When a unix-socket client submits, the server reads the connecting
  peer's `{uid, gid}` from the kernel via `SO_PEERCRED` (Linux) or
  `LOCAL_PEERCRED` (macOS) — the client cannot forge these. The
  server then resolves the user's supplementary groups by shelling
  out to `getent passwd <uid>` and `id -G <username>`, both of which
  consult NSS, so users in LDAP / SSSD work correctly even when
  `/etc/passwd` has no entry for them.
- The resulting `uid`, `gid`, and comma-separated `groups` are
  written into the job's details, overriding anything the client
  sent. The runner reads them at claim time and sets
  `cmd.SysProcAttr.Credential = {Uid, Gid, Groups}`. The kernel
  performs `setuid` / `setgid` / `setgroups` before `exec`, so the
  job process runs as the submitting user with the user's full
  supplementary group set.
- The job's stdout/stderr files are `chown`'d to the user so they
  own their own output.
- If the `uid` / `gid` details are missing or unparseable when the
  runner claims a job, the job is failed closed — never executed as
  root.
- The cgroup (v1 or v2) is created and the process placed into it,
  giving you kernel-enforced procs and memory limits.

Hold, release, and cancel are gated by the same identity. A non-root
peer can only act on jobs whose stored uid matches their own; root
(uid 0) is the admin escape hatch and can act on any job.

### Known security gaps

The remaining gaps in this picture — real, but smaller than they
were before peer-cred derivation landed:

- **No filesystem sandbox.** The job has the user's normal access to
  the filesystem. No bind-mount sandbox, no mount namespace, no
  chroot.
- **No PID or user namespace.** Jobs can see each other's processes
  in `ps` and can signal anything Unix permissions allow them to
  signal.
- **No PAM session.** The job does not go through PAM, so it does
  not get the usual login-time setup: resource limits from
  `/etc/security/limits.conf`, session leader assignment, audit
  attribution, etc.
- **Bearer-token auth on the REST API is not yet implemented.**
  Peer-cred derivation only works for unix-socket clients. Requests
  that arrive over HTTPS through the reverse proxy still trust the
  client's submitted `uid` / `gid`, and the hold / release / cancel
  authz check is skipped for them. Until server-side bearer-token
  validation lands, network-exposed deployments depend entirely on
  the reverse proxy for access control. See [remote](remote.md).

### When this is fine, and when it isn't

- **Fine** when (a) every reachable client comes in over the unix
  socket (so peer creds give you kernel-attested identity), and
  (b) the host's existing user accounts are the right granularity
  of isolation for your needs. Cgroup limits give you resource
  fairness on top.
- **Not fine yet** if your deployment exposes the REST API to
  remote clients and you need them to be subject to the same
  per-user identity enforcement. That requires bearer-token
  validation server-side, which is not yet implemented; until it
  is, lean on the reverse proxy.

### The simpler alternative: per-user runners

If you don't need cgroup enforcement, you can sidestep the entire
root-running picture by running a separate `batchq run` under each
user's own account against their own `$BATCHQ_HOME`. Standard Unix
permissions enforce isolation, there is no shared root-owned
component, and the NSS dependency goes away. This is the simplest
deployment for most multi-user hosts; reach for root only when
cgroup-enforced resource ceilings are the specific thing you need.

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
