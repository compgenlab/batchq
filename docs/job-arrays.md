# Job arrays

A *job array* submits one script as many near-identical tasks that differ
only by an index. Instead of running a shell loop that fires off a
thousand `batchq submit` calls, you submit once with `--array` and batchq
expands it into N task-jobs for you — and, just as importantly, lets you
**manage the whole batch as a unit**: hold, release, or cancel all the
tasks with a single command.

```sh
batchq submit --array 0-99 ./process.sh
```

That submits one hundred tasks (indices 0 through 99). Each task is an
ordinary job — it claims, runs, and reaches a terminal state on its own —
but they share an *array id*, and that's what makes batch operations
possible.

## What `submit --array` prints

A normal `batchq submit` prints the new job's UUID. `submit --array`
prints a single **array id** instead — the handle for the whole batch:

```sh
$ batchq submit --array 0-99 ./process.sh
b3f1c2a8-9e44-4d21-bb07-2c5e6f0a1d33
```

This mirrors `sbatch --parsable`: one line, parseable by downstream
tooling. You use that id to list, hold, release, or cancel the array (see
[Managing an array](#managing-an-array)).

## Array specs

The `--array` value describes which indices to create:

| Spec | Tasks created |
|---|---|
| `0-99` | 100 tasks, indices 0…99 |
| `1-10` | 10 tasks, indices 1…10 |
| `1-10:2` | indices 1, 3, 5, 7, 9 (step 2) |
| `1,3,5` | exactly those three indices |
| `0-9,20,30-32` | a mix: 0…9, 20, 30, 31, 32 |
| `0-99%4` | indices 0…99, but at most **4 running at once** (throttle) |

The `%N` throttle suffix combines with any of the other forms
(`1-10:2%3`, `1,3,5%1`, …). The total number of indices is capped at
100000 per array.

The same spec syntax is accepted three ways, so an existing SLURM script
works unchanged:

```sh
batchq submit --array 0-99 ./process.sh     # flag
```

```sh
#BATCHQ -array 0-99                          # inside the script
```

```sh
#SBATCH --array=0-99%4                       # parsed under --slurm
```

## Per-task output files

Array tasks would clobber each other if they all wrote to one log file, so
output paths get per-task placeholders, substituted **at run time**:

| Placeholder | Expands to |
|---|---|
| `%A` | the array id |
| `%a` | the task index |

If you don't set `--stdout`/`--stderr`, batchq rewrites the default
`./batchq-%JOBID.stdout` into the array form automatically, so each task
already writes a distinct file:

```sh
batchq submit --array 0-9 ./process.sh
# task 3 writes ./batchq-<arrayid>_3.stdout
```

You can also place them yourself:

```sh
batchq submit --array 0-99 \
  --stdout /logs/run-%A/task-%a.out \
  --stderr /logs/run-%A/task-%a.err \
  ./process.sh
```

(`%JOBID` in an array path is rewritten to the SLURM-style `%A_%a` at
submit time, so one `-o`/`-e` pattern works whether the job runs under the
simple runner or is proxied to SLURM.)

## Knowing which task you are

Inside the script, the task index is exported under the environment
variable names used by every common scheduler, so a script written for
SLURM, PBS/Torque, or SGE finds its index without changes:

| Variable | Value |
|---|---|
| `BATCHQ_ARRAY_ID` | the array id |
| `BATCHQ_ARRAY_TASK_ID` | this task's index |
| `BATCHQ_ARRAY_TASK_COUNT` | number of tasks in the array |
| `SLURM_ARRAY_JOB_ID` | the array id |
| `SLURM_ARRAY_TASK_ID` | this task's index |
| `SLURM_ARRAY_TASK_COUNT` | number of tasks in the array |
| `PBS_ARRAY_INDEX`, `PBS_ARRAYID` | this task's index |
| `SGE_TASK_ID` | this task's index |

A typical "pick my slice of the work from the index" pattern:

```sh
#!/bin/bash
# process.sh — one task per input file
FILES=(/data/*.fastq.gz)
INPUT="${FILES[$BATCHQ_ARRAY_TASK_ID]}"
echo "task $BATCHQ_ARRAY_TASK_ID of $BATCHQ_ARRAY_TASK_COUNT -> $INPUT"
align.sh "$INPUT"
```

```sh
batchq submit --array 0-$(( $(ls /data/*.fastq.gz | wc -l) - 1 )) ./process.sh
```

## Managing an array

The whole point of an array is batch management. The array-management
commands accept the **array id** that `submit` printed, and act on every
task at once:

```sh
ARRAY=$(batchq submit --hold --array 0-99 ./process.sh)

batchq status  $ARRAY      # per-status summary of the array's tasks
batchq release $ARRAY      # release all 100 tasks to run
batchq hold    $ARRAY      # put them all back on hold
batchq cancel  $ARRAY      # cancel the whole array
```

These are auto-detecting: each command first tries its argument as a
single job id, then as an array id, so the id `submit` gave you "just
works" — you don't pass a special flag.

### Addressing a single task

To act on one task rather than the whole batch, use the SLURM-style
`<array_id>_<index>` address (array ids are UUIDs and never contain `_`,
so the split is unambiguous):

```sh
batchq cancel $ARRAY            # cancel every remaining task
batchq cancel ${ARRAY}_3        # cancel only task index 3
batchq status ${ARRAY}_3        # status of one task
```

### Listing an array's tasks

```sh
batchq queue --array-id $ARRAY        # active tasks of this array
batchq queue --array-id $ARRAY --all  # including completed/canceled
```

When the SLURM runner is in play, `cancel` also issues the matching
`scancel` — `scancel <slurm_array_id>` for a whole-array cancel, or
`scancel <slurm_array_id>_<index>` for a single task — so the kill
propagates to the cluster, not just to batchq's own state.

## Dependencies between arrays

Two dependency shapes apply to arrays:

- **`afterok:<array_id>`** — the dependent job waits for **all** tasks of
  the array to succeed. Works for any submit (single job or array).

  ```sh
  ARRAY=$(batchq submit --array 0-99 ./map.sh)
  batchq submit --deps "afterok:$ARRAY" ./reduce.sh   # runs after every task succeeds
  ```

- **`aftercorr:<array_id>`** — element-wise: task *i* of the new array
  waits for task *i* of the parent array. This is **array-to-array only**,
  and both arrays must cover the same index set.

  ```sh
  A=$(batchq submit --array 0-99 ./stage1.sh)
  batchq submit --array 0-99 --deps "aftercorr:$A" ./stage2.sh
  # stage2 task 7 starts when stage1 task 7 succeeds
  ```

`aftercorr` is also available as `#BATCHQ -aftercorr <id>` and, under
`--slurm`, as `#SBATCH -d aftercorr:<id>`.

## On the web UI

[`batchq web`](web.md) surfaces arrays in two places:

- The **queue** shows an array column; clicking it filters the queue to
  that array's tasks (the `?array_id=` view).
- A task's **job page** gains an **Array** tab with a per-status progress
  summary (how many tasks are queued / running / done / failed) and a
  list of every sibling task linking to its own page.

## Under the SLURM runner

When the array is proxied to SLURM, the SLURM runner does **not** submit
one `sbatch` per task. It claims a batch of the array's tasks (bounded by
`max_jobs` / `max_slurm_jobs`, so a 1000-task array is drip-fed rather
than dumped on the scheduler), submits them as a **single**
`sbatch --array=<indices>%N`, and records each task's SLURM array id and
task index. Reconciliation is symmetric: one `sacct -j <arrayid>` call
reports the state of every task in the array. The `%N` throttle from your
spec becomes sbatch's native `%N`.

See [SLURM](slurm.md) for the runner's deployment model and the rest of
the translation table.

## Where to go next

- [Submitting jobs](submitting-jobs.md) — every `submit` flag, including
  the `#BATCHQ` and `#SBATCH` directive forms `--array` mirrors.
- [Generic resources](resources.md) — requesting GPUs and other resources
  (combine `--resource` with `--array` to fan a GPU job across a range).
- [SLURM](slurm.md) — how the SLURM runner submits and reconciles arrays.
- [The web UI](web.md) — the queue array filter and the per-job Array tab.
