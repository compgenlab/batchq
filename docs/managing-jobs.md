# Managing jobs and the queue

Once jobs are submitted, a handful of commands let you inspect the queue,
re-prioritize work, hold or cancel jobs, and tidy up finished ones. They
are all thin clients of the batchq server, so they work the same whether
the server is a long-lived process or one that autospawns for the duration
of the command.

Every command that takes a `job-id` accepts the UUID that `submit` printed.
The lifecycle commands (`hold`, `release`, `cancel`, `status`, `details`)
also accept an **array id** or a `<array_id>_<index>` task address — see
[Job arrays](job-arrays.md) for that.

## Inspecting the queue

### `queue` — the tabular view

```sh
batchq queue                          # active jobs (not yet completed)
batchq queue --all                    # include SUCCESS / FAILED / CANCELED
```

| Flag | Meaning |
|---|---|
| `--all` | Include completed jobs (success, failed, canceled). |
| `--run-id ID` | Only jobs tagged with this workflow run id. |
| `--array-id ID` | Only the tasks of this job array. |
| `--output PATH` | Only jobs that list `PATH` as an output (who *produced* it). |
| `--input PATH` | Only jobs that list `PATH` as an input (who *consumes* it). |
| `-t`, `--time` | Sort by submit time, newest first (default is by status). |
| `-r`, `--reverse` | Reverse the sort order (use with `-t`). |

```sh
batchq queue --run-id run-2025-Q1     # one workflow run
batchq queue --array-id $ARRAY        # one array's tasks
batchq queue --output /data/x.bam     # which job produced this file
batchq queue -t                       # most recently submitted first
```

### `summary` — counts per status

A one-line-per-status tally of the whole queue:

```sh
$ batchq summary
USERHOLD    : 0
WAITING     : 3
QUEUED      : 17
RUNNING     : 4
SUCCESS     : 120
FAILED      : 2
CANCELED    : 1
```

By default only non-zero statuses are shown; `--all` prints every status
including the empty ones.

### `search` — find a job by anything

```sh
batchq search sample42
```

Matches the term against a job's id, name, script body, and tagged
input/output paths. If exactly one job matches, you get its full details;
if several match, you get the queue table. Takes exactly one search term.

### `status` — just the status code

```sh
batchq status                 # id + status for every active job, one per line
batchq status <job-id>        # the status of one job
batchq status <job-id> <job-id>
```

With an array id, `status` prints a per-status summary of the array's
tasks instead of a single line.

### `details` — everything we know about a job

```sh
batchq details <job-id>
```

The long-form view: every resource requirement, the captured script,
working directory, output paths, dependencies, timestamps, and (for a
running or finished job) runtime details like the pid or SLURM job id.
Given an array id, it prints the array summary.

## Changing scheduling priority

When several jobs are eligible at once, a runner claims them in priority
order (highest priority first, ties broken by submit time). Two commands
nudge a waiting job up or down that order:

| Command | Effect |
|---|---|
| `batchq top <job-id>…` | Raise the job's priority by one step (claimed sooner). |
| `batchq nice <job-id>…` | Lower the job's priority by one step (claimed later). |

```sh
batchq top <job-id>      # bump it up — run it ahead of its peers
batchq nice <job-id>     # push it back — let other work go first
```

Each call shifts the priority by one, so repeating `top` raises a job
further above the pack. Re-prioritizing only applies to jobs that haven't
started yet (`QUEUED`, `WAITING`, or `USERHOLD`) — once a job is `RUNNING`
or finished there is nothing to reorder, and the command reports an error.
Both commands accept several job ids at once.

## Holding, releasing, and canceling

| Command | Effect |
|---|---|
| `batchq hold <job-id>…` | Move an eligible job to `USERHOLD` so no runner picks it up. |
| `batchq release <job-id>…` | Release a held job back into the queue. |
| `batchq cancel <job-id>…` | Cancel a queued or running job. |

```sh
batchq hold <job-id>           # park it
batchq release <job-id>        # un-park it
batchq cancel <job-id>         # stop it
batchq cancel --reason "bad inputs" <job-id>
```

`cancel` takes a `--reason` (default `"Canceled by user"`) recorded on the
job, cascades the cancellation to any jobs that depended on it, and — when
the job had been handed to SLURM — issues the matching `scancel` so the
kill reaches the cluster.

All three commands auto-detect an **array id** (acting on the whole batch)
or a `<array_id>_<index>` **task address** (acting on one task). See
[Job arrays › Managing an array](job-arrays.md#managing-an-array).

## Removing finished jobs

`cleanup` deletes terminal jobs from the database. You must say *which*
terminal states to remove — running it with no selector just prints help,
so you can't wipe the wrong thing by reflex:

| Flag | Meaning |
|---|---|
| `--success` | Remove `SUCCESS` jobs. |
| `--failed` | Remove `FAILED` jobs. |
| `--canceled` | Remove `CANCELED` jobs. |
| `--all` | Shorthand for all three. |
| `--older-than DUR` | Only remove jobs that finished more than `DUR` ago. |

```sh
batchq cleanup --success                       # drop completed jobs
batchq cleanup --all                           # drop everything terminal
batchq cleanup --all --older-than 30d          # keep the last 30 days
batchq cleanup --failed --older-than 1w        # prune old failures only
```

`--older-than` accepts `s`, `m`, `h`, `d` (days), and `w` (weeks)
suffixes — e.g. `90m`, `12h`, `30d`, `2w`. The unit is required.

cleanup is dependency-aware: it removes jobs in an order that never orphans
a record, and it skips a finished job whose dependents are not themselves
being removed (reporting `Skipping job …: dependents not eligible for
removal`) so the dependency history stays intact.

## Stopping the local server

```sh
batchq stop
```

Asks the autospawned local server to drain in-flight requests and exit,
freeing the unix socket. This is the explicit form of the global
`--restart-server` flag — use it when you want to stop the server *without*
immediately running another command, e.g. before swapping in a new binary.

It only acts on a local server: when `[batchq] remote` is set, `batchq
stop` refuses (a client does not shut down a shared remote server). See
[Remote access](remote.md).

## Other utilities

| Command | What it does |
|---|---|
| `batchq version` | Print the version string (also `batchq --version`). |
| `batchq debug` | Print the fully resolved configuration with each value's source. See [Configuration › Debugging](configuration.md#debugging-configuration). |
| `batchq license` | Print the license. |

## Where to go next

- [Submitting jobs](submitting-jobs.md) — the `submit` command and its
  flags.
- [Job arrays](job-arrays.md) — batch hold / release / cancel and the
  `<array_id>_<index>` task address.
- [Running jobs locally](running-jobs.md) — the runner that actually picks
  jobs up.
- [Configuration](configuration.md) — `batchq debug`, the config file, and
  every knob.
