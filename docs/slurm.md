# Running on a SLURM cluster

The SLURM runner is the reason batchq exists. It lets you queue a much
larger backlog of jobs in batchq than SLURM would allow you to put in
its queue directly, then submits them into SLURM at a controlled rate
as capacity becomes available.

The runner is the **only** component that talks to SLURM. The batchq
server itself does not know SLURM exists — it just sees a runner that
claims jobs, eventually moves them to `PROXYQUEUED`, and later reports
their terminal status. This is a permanent deployment invariant: the
batchq server and the SLURM head node may live on different clusters,
and reconciliation never moves server-side.

## When to use it

Use the SLURM runner when:

- You have a SLURM cluster and a per-user `MaxJobs` limit (or you want
  to be polite about not flooding the queue).
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
runs one reconciliation + submission pass and exits — useful in a
cron-driven setup if you really do not want a runner process sitting
around.

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
  count is at the limit. This is the knob to set to your cluster's
  per-user `MaxJobs` minus a margin.
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

## Operating tips

- **Run it under `nohup` or `tmux`.** `batchq run --slurm --forever`
  is fine to leave running for days. If you must keep it shorter,
  drop `--forever` and run it from cron every minute or two — the
  reconciliation loop is designed to work either way.
- **One runner per user.** There is no point in running two SLURM
  runners for the same user — they would compete for the same SLURM
  queue budget. Two for different users is fine.
- **Watch the rate.** Submitting too fast can be unfriendly to the
  SLURM scheduler. A handful of submissions per second is a reasonable
  default; if your sysadmins ask you to slow down, lower
  `max_slurm_jobs` so the runner waits more often between submissions.

## Where to go next

- [Submitting jobs](submitting-jobs.md) — every flag the SLURM runner
  picks up at submission time, including the `#SBATCH` parsing
  shortcut.
- [Configuration](configuration.md) — every `[slurm_runner]` knob.
- [Remote access](remote.md) — if your CLI users are on a different
  machine than the SLURM submit host, point them at a reverse-proxied
  batchq server.
