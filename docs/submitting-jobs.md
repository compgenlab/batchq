# Submitting jobs

`batchq submit` is the entry point for everything batchq does. It puts a
job into the queue and returns its job ID — what happens next is up to
whichever runner picks it up.

## Usage

```
batchq submit [flags] [script-path | -- inline command...]
```

The positional argument is interpreted in this order:

1. If `arg[0]` is a path to an existing file, that file is the script.
2. Otherwise, if there are positional args (or `--` was used), the
   remaining args are joined as an inline shell command.
3. Otherwise, the script is read from standard input.

So all three of these work:

```sh
batchq submit ./align.sh                       # script from file
batchq submit -- bwa mem ref.fa reads.fq       # inline command
echo "echo hello" | batchq submit              # script from stdin
```

The submitted script is stored verbatim in the database (as a
`script` detail on the job). When the runner executes the job, it
writes the script to a temp file under the spool directory and runs
it with the configured shell (`[simple_runner] shell`, default
`/bin/bash`).

On success, `batchq submit` prints the new job ID to stdout — a UUID
string with hyphens. This output is part of the stable CLI contract:
downstream pipeline tools parse it.

## Flags

### Naming and identification

| Flag | Default | Meaning |
|---|---|---|
| `--name NAME` | (none) | Human-friendly job name shown in queue listings. |
| `--run-id ID` | (none) | Workflow run identifier. Groups related jobs so you can list them or build a dependency forest. |

### Resource requirements

| Flag | Default | Meaning |
|---|---|---|
| `-p`, `--procs N` | from `[job_defaults] procs` | Processors required. |
| `-m`, `--mem MEM` | from `[job_defaults] mem` | Maximum memory (e.g. `8GB`, `1500MB`). |
| `-t`, `--walltime TIME` | from `[job_defaults] walltime` | Maximum walltime as `D-HH:MM:SS`. |

A job that requests more than the runner's `max_procs` / `max_mem` /
`max_walltime` ceiling stays in `QUEUED` and is skipped over by that
runner.

For anything beyond procs/mem/walltime — GPUs, licenses, a specific
cluster or node feature — use `--resource`:

| Flag | Default | Meaning |
|---|---|---|
| `--resource name[=value]` | (none, repeatable) | A generic resource the job requires. A count (`gpu=2`), a typed count (`gpu:a100=2`), a label (`cluster=biocluster`), or a bare feature flag (`fastio`). A runner only claims the job if it advertises enough to satisfy every requirement. |

`procs`, `mem`, and `walltime` are reserved names — pass those via
`-p`/`-m`/`-t`, not `--resource`. See [Generic resources](resources.md)
for the full model (counts vs. labels, runner advertisement, SLURM
`--gres`/`-C` mapping).

### Working directory and output

| Flag | Default | Meaning |
|---|---|---|
| `--wd DIR` | `.` (current directory at submit time) | Working directory the job runs in. Stored as an absolute path. |
| `--stdout FILE` | `./batchq-%JOBID.stdout` | Captured stdout path. `%JOBID` is substituted with the assigned job ID. If you pass a directory, the file lands inside it. |
| `--stderr FILE` | `./batchq-%JOBID.stderr` | Captured stderr path. Same rules. |

`%JOBID` is the canonical placeholder. The SLURM runner rewrites it to
SLURM's `%j` when generating the sbatch script — you do not need to
care about the difference.

### Environment

| Flag | Default | Meaning |
|---|---|---|
| `--env` | from `[job_defaults] env` (`false`) | Capture the submitter's current environment and replay it when the job runs. Useful when the runtime environment differs from the submission environment (e.g. you submit from a login node and the runner is on a compute node). |

The captured environment is stored on the job and is used by both the
simple runner (when running the script) and the SLURM runner (passed to
sbatch as `--export=…`).

### Dependencies

| Flag | Default | Meaning |
|---|---|---|
| `--deps id,id,…` | (none) | The job will not start until all listed parent jobs reach `SUCCESS`. Comma-separated UUIDs. |

A job with unmet `afterok` dependencies sits in `WAITING` until they
all resolve. If any parent fails or is cancelled, every descendant is
cancelled with reason "parent failed" or "parent canceled".

### Holding

| Flag | Default | Meaning |
|---|---|---|
| `--hold` | from `[job_defaults] hold` (`false`) | Submit in `USERHOLD` state. The runner will not pick it up until you `batchq release <id>`. |

### Input/output file tags

| Flag | Default | Meaning |
|---|---|---|
| `--input PATH` | (none, repeatable) | Tag the job with an input file path. Look up later with `batchq queue --input PATH`. |
| `--output PATH` | (none, repeatable) | Tag the job with an output file path. Look up later with `batchq queue --output PATH`. |

These tags are metadata only — batchq does not check that the files
exist, that the job reads or writes them, or that producer/consumer
relationships are consistent. They are a way to ask "which job
produced this file?" after the fact.

### Job arrays

| Flag | Default | Meaning |
|---|---|---|
| `--array SPEC` | (none) | Submit the script as an array of indexed tasks. `0-99`, `1-10:2`, `1,3,5`, or a `%N` throttle suffix (`0-99%4`). Prints a single **array id** instead of a job id. |

One `submit --array` expands into N task-jobs you can then hold, release,
or cancel as a batch (`batchq cancel <array-id>`), with `%A`/`%a`
placeholders for per-task output paths. See [Job arrays](job-arrays.md)
for the full feature.

## Submitting SBATCH scripts

If you already have an SBATCH script (e.g. you are running batchq in
front of SLURM), pass `--slurm` to parse `#SBATCH` headers as flag
defaults:

```sh
batchq submit --slurm job.sbatch
```

Supported `#SBATCH` directives:

| Directive | Maps to |
|---|---|
| `-c`, `--cpus-per-task` | `--procs` |
| `--mem` | `--mem` |
| `-t`, `--time` | `--walltime` |
| `-J`, `--job-name` | `--name` |
| `-D`, `--chdir` | `--wd` |
| `-o`, `--output` | `--stdout` (with `%j` → `%JOBID`) |
| `-e`, `--error` | `--stderr` (with `%j` → `%JOBID`) |
| `--export=ALL` | `--env` |
| `-d afterok:id[:id…]` | `--deps` (afterok) |
| `-d aftercorr:id` | element-wise array dependency |
| `--array=spec` | `--array` |
| `--gres=name[:type][:count]` | `--resource name[:type]=count` |
| `-C`, `--constraint=feat[&feat…]` | `--resource feat` (feature flags) |

Explicit `batchq submit` flags still win — `--slurm` parsing only fills
in values you did not pass on the command line.

## `#BATCHQ` directives

Inside the script you can use `#BATCHQ` lines as an alternative to
flags. The submit-time metadata flags have parallels:

- `#BATCHQ -run-id RUN`
- `#BATCHQ -input /path`
- `#BATCHQ -output /path`
- `#BATCHQ -resource name[=value]`
- `#BATCHQ -array SPEC`
- `#BATCHQ -aftercorr ARRAY_ID`

These are parsed at submission time and merged with command-line
flags, with command-line flags winning.

## Examples

A typical pipeline job — eight cores, 16GB, one day, named for the
sample, with the input and output tagged so you can find it later:

```sh
batchq submit \
  --name align-sample42 \
  --procs 8 --mem 16GB --walltime 1-00:00:00 \
  --input /data/sample42.fq.gz \
  --output /data/sample42.bam \
  --run-id run-2025-Q1 \
  ./align.sh sample42
```

A job that depends on the alignment finishing first:

```sh
ALIGN_ID=$(batchq submit --name align-sample42 ./align.sh sample42)
batchq submit --deps "$ALIGN_ID" \
  --name count-sample42 \
  --run-id run-2025-Q1 \
  --input /data/sample42.bam \
  --output /data/sample42.counts \
  ./count.sh sample42
```

A held job, released by hand later:

```sh
HOLD_ID=$(batchq submit --hold --name maybe-later ./optional-step.sh)
# ...think about it...
batchq release "$HOLD_ID"
```

## Finding jobs you submitted

```sh
batchq queue                          # active jobs
batchq queue --all                    # including completed
batchq queue --run-id run-2025-Q1     # one workflow run
batchq queue --output /data/x.bam     # who produced this file
batchq queue --input /data/y.fq       # who needs this file
batchq search sample42                # name / id / script search
batchq details <job-id>               # everything we know
batchq status <job-id>                # just the status code
```

For the full set of inspection and management commands — including
`summary`, re-prioritizing with `top`/`nice`, `cancel`, and `cleanup` —
see [Managing jobs and the queue](managing-jobs.md).

## Where to go next

- [Managing jobs and the queue](managing-jobs.md) — inspect, re-prioritize,
  hold, cancel, and clean up submitted jobs.
- [Job arrays](job-arrays.md) — submit and manage a range of indexed
  tasks with one command.
- [Generic resources](resources.md) — request GPUs and other resources
  with `--resource`.
- [Running jobs](running-jobs.md) — how submitted jobs actually execute
  under the simple runner.
- [SLURM](slurm.md) — how the SLURM runner picks up submitted jobs and
  hands them to `sbatch`.
- [Web UI](web.md) — a browser view of the queue, with per-job pages
  and a dependency-shaped view of workflow runs.
