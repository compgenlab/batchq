# Running on a SLURM cluster

The SLURM runner is one of the original motivations for batchq (see
[overview](overview.md#why-batchq-exists) for the others). It lets you
queue a much larger backlog of jobs in batchq than your shared
cluster's admins want you putting into the SLURM queue directly, then
submits them into SLURM at a controlled rate as capacity becomes
available.

SLURM itself doesn't impose a per-user job cap — a single-user cluster
can happily hold tens of thousands of jobs. But admins of shared
clusters routinely configure a per-user `MaxJobs` (and similar) limit
because one user's huge backlog degrades scheduler responsiveness for
everyone else. The SLURM runner exists to be a good citizen on those
clusters: hold the backlog locally in batchq, feed SLURM at whatever
rate the local policy permits.

The runner is the **only** component that talks to SLURM. The batchq
server itself does not know SLURM exists — it just sees a runner that
claims jobs, eventually moves them to `PROXYQUEUED`, and later reports
their terminal status. This is a permanent deployment invariant: the
batchq server and the SLURM head node may live on different clusters,
and reconciliation never moves server-side.

## When to use it

Use the SLURM runner when:

- You're on a shared SLURM cluster where admins have set a per-user
  `MaxJobs` limit, or you want to be polite about not flooding the
  queue even where there is no hard cap.
- You want to submit a pipeline of thousands of jobs from one place,
  with `afterok` dependencies, without managing the SLURM queue
  yourself.
- You want a persistent record of every job you ever submitted, with
  its inputs and outputs tagged, that outlives SLURM's job-accounting
  retention.

Use the simple runner instead when you just want jobs to run on the
host the runner is on (a workstation, a single shared server, or a
compute node you have grabbed for the day).

## Deployment topology

The pieces and where they run:

```
┌─────────────────────────────────┐    ┌──────────────────────────┐
│  Login node / submit host       │    │  SLURM head node         │
│                                 │    │                          │
│  batchq server  ─── socket ───┐ │    │  sbatch / squeue / sacct │
│  (autospawned by clients)     │ │    │                          │
│                               │ │    └──────────┬───────────────┘
│  $BATCHQ_HOME/                │ │               │
│    batchq.db   (on NFS/Lustre)│ │               │
│    batchq.sock (on local FS)  │ │   ssh / direct exec, depending
│                               │ │       on cluster topology
│  CLI clients (submit/show)    │ │               │
│                               │ │               │
│  batchq run --slurm  ─── REST over socket ─────┘
└─────────────────────────────────┘
```

In most clusters the same node is the login node and the SLURM submit
host, so batchq server, the SLURM runner, the SLURM client binaries,
and the user's CLIs all live there. On clusters where the SLURM head
is a different host than where users land, the SLURM runner needs to
be where `sbatch` works.

The database file (`batchq.db`) goes on a networked filesystem so
every cluster node sees the same queue. The socket (`batchq.sock`)
goes on a *local* filesystem — `/tmp` or similar — because unix
sockets do not work reliably on NFS. The batchq server's `[server]
listen` can point at a local-disk socket while `[server] db` points at
the networked database.

The batchq server is **not** a long-running daemon in this picture.
Clients spawn one on demand with a short idle timeout, and overlapping
requests from the user's CLIs and from `batchq run --slurm` are
funneled through that one process so the database stays safe.

## Running the SLURM runner

```sh
batchq run --slurm \
  --slurm-user $USER \
  --slurm-account acct123 \
  --slurm-partition general \
  --slurm-max-jobs 200 \
  --forever
```

Or set the same in config and just run `batchq run --slurm --forever`:

```toml
[batchq]
runner = "slurm"             # makes --slurm the default for `batchq run`

[slurm_runner]
user = "myuser"
account = "acct123"
partition = "general"
max_jobs = 0                 # 0 / unset = unlimited submissions per invocation
max_slurm_jobs = 200         # cap on this user's live SLURM-queue jobs
```

The runner loops as follows:

1. **Check the SLURM queue depth.** Calls `squeue` for the configured
   user; if the live job count is at `max_slurm_jobs`, sleep and
   retry.
2. **Reconcile.** For every batchq job currently in `PROXYQUEUED`,
   call `squeue` / `sacct` to see whether it finished. If it did,
   report its terminal status back to the batchq server.
3. **Submit.** Claim the next eligible batchq job (atomic
   `QUEUED → RUNNING` on the server, then immediately `RUNNING →
   PROXYQUEUED` after `sbatch` returns), generate an sbatch script
   from its details, and shell out to `sbatch`.
4. **Repeat.**

With `--forever`, this loop runs indefinitely. Without `--forever`, it
runs one reconciliation + submission pass and exits — which is the
shape you want for a cron-driven setup (see [Running it from cron](#running-it-from-cron-recommended)
below).

## What gets translated to SLURM

A batchq job submitted with the usual flags becomes an sbatch
invocation that looks roughly like:

```sh
sbatch \
  --job-name <name> \
  --cpus-per-task <procs> \
  --mem <mem> \
  --time <walltime> \
  --chdir <wd> \
  --output <stdout-with-%j-substituted> \
  --error <stderr-with-%j-substituted> \
  --account <account> \
  --partition <partition> \
  [--dependency afterok:<slurm-id>:<slurm-id>...] \
  [--export=<captured-env>] \
  <generated-script>
```

Notes:

- **stdout/stderr placeholders.** batchq's `%JOBID` placeholder is
  rewritten to SLURM's `%j` when the sbatch script is generated.
- **Dependencies.** When a batchq job has `afterok` dependencies on
  jobs that the SLURM runner already submitted, those dependencies are
  translated to `--dependency afterok:<slurm-id>:...`. The runner
  knows the SLURM job id for each PROXYQUEUED batchq job (it stores it
  as a running detail) and looks it up when generating dependency
  flags.
- **Environment.** If the batchq job was submitted with `--env` (or
  `#BATCHQ -env`, or an `--export=ALL` SBATCH header), the captured
  environment is passed to sbatch.

## Throttling

Two knobs control the submission rate:

- **`max_slurm_jobs` (`--slurm-max-jobs`).** The maximum number of
  this user's jobs that may exist in the SLURM queue at once. The
  runner polls `squeue` before every submission and waits if the live
  count is at the limit. On a shared cluster, set this to your
  cluster's per-user `MaxJobs` policy minus a margin; on a cluster
  with no cap, set it to whatever you think is polite.
- **`max_jobs` (`--max-jobs`).** A cap on how many jobs this runner
  invocation submits before exiting. Mostly useful for cron-driven
  setups; with `--forever`, leave it unset.

## Failure handling

- A failed `sbatch` (non-zero exit, network blip) leaves the batchq
  job back in `QUEUED` for the next pass.
- A SLURM job that ends with a non-zero exit code is reported back to
  batchq as `FAILED` with the exit code in its running details. Its
  `afterok` descendants are cancelled with reason "parent failed".
- A SLURM job that is cancelled (`scancel`, time-out, node failure)
  is reported back as `CANCELED`. Descendants are cancelled with
  reason "parent canceled".
- A `sacct` lag where SLURM reports "unknown" for a recently-completed
  job does **not** mark the batchq job FAILED — the runner records the
  reason and tries again later. (See commit `fae265f`.)

## Running it from cron (recommended)

The reconciliation loop is designed to work in single-pass mode, so on
a shared cluster the cleanest way to keep the SLURM runner going is to
drive it from `cron` rather than leave a long-running process in
`tmux`. A single-pass invocation submits whatever it can up to its
caps, reports any newly-finished jobs back to the batchq server, and
exits — there is no `--forever` and no process to babysit.

A lockfile is the only thing you have to add on top, so that a still-
running invocation doesn't get a second copy of itself when cron fires
again:

```sh
#!/bin/bash
LOCK=$HOME/jobs/batchq_slurm_runner.lock
LOG=$HOME/jobs/batchq_slurm_runner.log

if [ ! -e "$LOCK" ]; then
    echo $$ > "$LOCK"
    {
        echo "--"
        date
        batchq run --slurm \
            --max-jobs 50 \
            --slurm-account MYACCT \
            --slurm-max-jobs 475
    } &>> "$LOG"
    rm "$LOCK"
fi
```

Drop that into `~/jobs/batchq_slurm_runner.sh` and add it to your
crontab. Every five minutes is a reasonable cadence — SLURM job state
doesn't change fast enough to need finer granularity, and the lockfile
shrugs off the occasional run that takes longer than the interval:

```cron
*/5 * * * * ~/jobs/batchq_slurm_runner.sh
```

The pattern in this example:

- **`--max-jobs 50`** caps the number of jobs this one invocation
  moves from `QUEUED` → `PROXYQUEUED`. Even with a huge batchq
  backlog and plenty of SLURM headroom, no single cron tick pushes
  more than fifty new jobs into SLURM.
- **`--slurm-max-jobs 475`** is the ceiling on this user's total
  live SLURM jobs (anything visible to `squeue` for the user). Before
  submitting, the runner checks the live count and stops if it is at
  the cap; set this to whatever leaves room under your cluster's
  per-user policy.
- **Lockfile.** If the previous run is still in flight (a slow
  `squeue` or `sacct`, or batchq itself spawning a server), the new
  cron tick exits immediately instead of starting a second runner.
- **Append-only log.** Every tick prints a `--` separator and a
  timestamp, so the log is greppable when you need to know what the
  runner was doing at 03:14.

The lockfile is just a flag file, not a real lock — it's racy in
principle, but cron at minute granularity gives you plenty of time
between checks, and the worst case is a leftover lock after a crash
(which is easy to spot and delete).

## Operating tips

- **One runner per user.** There is no point in running two SLURM
  runners for the same user — they would compete for the same SLURM
  queue budget. Two for different users is fine.
- **Cron vs. `--forever`.** Cron + lockfile is the recommended pattern
  on shared clusters (no permanent process, survives logout). If you
  prefer a long-running process — on your own hardware, or anywhere
  long-lived user processes are acceptable — `batchq run --slurm
  --forever` under `tmux` or `nohup` works equally well; the same
  reconciliation loop runs either way.
- **Watch the rate.** Submitting too fast can be unfriendly to the
  SLURM scheduler. A handful of submissions per second is a reasonable
  default; if your sysadmins ask you to slow down, lower
  `max_slurm_jobs` so the runner waits more often between submissions,
  or lower `--max-jobs` so each cron tick submits fewer.

## Where to go next

- [Submitting jobs](submitting-jobs.md) — every flag the SLURM runner
  picks up at submission time, including the `#SBATCH` parsing
  shortcut.
- [Configuration](configuration.md) — every `[slurm_runner]` knob.
- [Remote access](remote.md) — if your CLI users are on a different
  machine than the SLURM submit host, point them at a reverse-proxied
  batchq server.
