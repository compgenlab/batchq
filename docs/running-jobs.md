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

### What does happen when the runner is root

- Each job's `uid` and `gid` are captured from `user.Current()` on
  the *submitting client* at submit time and stored as job details
  (`jobs/jobdef.go:106`).
- When the runner claims a job, it reads those values and sets
  `cmd.SysProcAttr.Credential = {Uid, Gid}` so the kernel
  setuid/setgid's before `exec`. The job process runs as the
  submitting user, not as root (`runner/simple.go:483`).
- The captured stdout/stderr files are `chown`'d to the user so they
  own their own output.
- If the `uid`/`gid` details are missing or unparseable, the job is
  failed closed — never executed as root.
- The cgroup (v1 or v2) is created and the process placed into it,
  giving you kernel-enforced procs and memory limits.

### Known security gaps

These are real problems for any deployment that takes "multi-user"
seriously. None of them are bugs in the small sense — they are
features batchq simply hasn't built yet.

- **No supplementary groups.** `Credential.Groups` is left unset,
  which on Linux causes Go to call `setgroups(0, NULL)` and clear the
  full group set the user would normally have. The job process ends
  up with only the user's primary GID, so users who rely on group
  membership for filesystem access (shared project directories, for
  instance) hit permission errors that don't happen at an interactive
  login.
- **No server-side verification of the claimed uid/gid.** The uid and
  gid in the job's details are whatever the submitting client put on
  the wire. The server does not cross-check them against the unix
  socket's peer credentials. With the default mode-`0600` socket the
  server is only reachable by its owner, so this is moot in
  single-user mode. For genuine multi-user (a relaxed-mode socket, or
  a network-fronted server) anybody who can reach the socket can
  claim to be any uid — including uid 0.
- **No filesystem sandbox.** The job has the (correctly-attributed)
  user's normal access to the filesystem. No bind-mount sandbox, no
  mount namespace, no chroot.
- **No PID or user namespace.** Jobs can see each other's processes
  in `ps` and can signal anything Unix permissions allow them to
  signal.
- **No PAM session.** The job does not go through PAM, so it does not
  get the usual login-time setup: resource limits from
  `/etc/security/limits.conf`, session leader assignment, audit
  attribution, etc.
- **Bearer-token auth on the REST API is not yet implemented.** Any
  network-exposed deployment depends entirely on the reverse proxy
  for access control. See [remote](remote.md).

### When this is fine, and when it isn't

The honest answer:

- **Fine** when you control every user on the host, none of them are
  hostile, they all already have shell access to the same machine,
  and your only goal is to keep one runaway job from starving another
  via cgroup limits. You are using root for *resource fairness*, not
  for *security*.
- **Not fine** as soon as your threat model includes anyone you don't
  fully trust. The combination of "anyone who can reach the socket
  can claim any uid" + "no FS sandbox" + "no PAM" means a hostile
  submitter on a relaxed-permission deployment can run arbitrary code
  as any user — including root, by simply asking nicely.

### The recommended alternative: per-user runners

For multi-user hosts where you don't need cgroup enforcement, run a
separate `batchq run` under each user's own account against their own
`$BATCHQ_HOME`. Standard Unix permissions enforce isolation: each
user's batchq server, socket, and database are owned by them, jobs
run as them, group memberships are correct, and there is no shared
root-owned component to attack. This sidesteps every gap listed
above. If users want a shared queue, give them a shared submission
*server* (still per-user runners) reached via the reverse-proxied
HTTPS endpoint — that decouples "where the queue lives" from "what
identity each job runs as."

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
