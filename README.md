# batchq

batchq is a small job scheduler for running tasks. Jobs and state live in a local SQLite database. In default mode it is serverless: submission, scheduling, and execution all happen in the current process, coordinated through the database.

Recent change (commit `7c2c07c`): a new SLURM runner lets batchq act as a front-end to a SLURM cluster. You can queue many jobs locally, and batchq will trickle them into SLURM while respecting SLURM job limits and reporting SLURM status back into batchq.

## Quick start

```sh
batchq initdb                    # create ~/.batchq/batchq.db (path configurable)
batchq submit ./script.sh        # submit a shell script
batchq run                       # run jobs with the simple runner
```

Defaults:
- working directory: `.`
- stdout/stderr: `./batchq-%JOBID.stdout` and `./batchq-%JOBID.stderr` (if a directory is given, files are placed inside it)
- other defaults can be set under `[job_defaults]` in `~/.batchq/config`

## Submitting jobs

Submit a script file:
```sh
batchq submit ./myjob.sh
```

Submit inline:
```sh
batchq submit --name align --procs 8 --mem 16GB --walltime 1-00:00:00 ./align.sh
```

Pipe from stdin:
```sh
echo "echo hello" | batchq submit
```

Useful flags:
- `--name NAME` name the job
- `-p/--procs N`, `-m/--mem MEM`, `-t/--walltime D-HH:MM:SS`
- `--wd DIR` working directory
- `--stdout FILE`, `--stderr FILE` (supports `%JOBID`)
- `--deps 12,13` run after other jobs succeed
- `--hold` submit held
- `--env` capture current environment and replay at run time

### Submitting SLURM scripts

If you already have an SBATCH script, pass `--slurm` to parse its headers:
```sh
batchq submit --slurm job.sbatch
```

Supported SBATCH directives:
- `-c/--cpus-per-task`, `--mem`, `-t/--time`, `-J/--job-name`, `-D/--chdir`
- `-o/--output`, `-e/--error` ( `%j` is remapped to `%JOBID` )
- `--export=ALL` to capture environment
- `-d afterok:<jobids>` for dependencies

## Running jobs

### Simple runner (local)
```sh
batchq run --max-procs 4 --max-mem 16GB --max-walltime 1-00:00:00 --forever
```
Config equivalents live under `[simple_runner]` (e.g., `max_procs`, `max_mem`, `max_walltime`, `use_cgroup_v1`, `use_cgroup_v2`, `shell`).

### SLURM runner (proxy to SLURM)
```sh
batchq run --slurm --slurm-user $USER --slurm-acct acct123 --slurm-max-jobs 200
```
Or set `[batchq] runner = slurm` and configure `[slurm_runner]`:
```
[slurm_runner]
user = myuser          # optional default to current user
account = acct123      # optional
max_jobs = 200         # cap jobs submitted for the user
```

Behavior:
- Submits queued batchq jobs to SLURM via `sbatch`, mapping job details to SBATCH flags.
- Respects `--slurm-max-jobs`/`max_jobs` so you can queue locally without flooding SLURM.
- Tracks status with `squeue`/`sacct` and writes SLURM state, start/end times, and exit codes back to batchq.
- Preserves dependencies by translating batchq dependencies to `afterok` using SLURM job IDs.
- If a job was submitted with `--env` or `#BATCHQ -env`, the captured environment is passed to SLURM.

## Configuration

Example `~/.batchq/config`:
```
[batchq]
runner = simple            # or slurm
dbpath = sqlite3:///path/to/batchq.db

[job_defaults]
procs = 4
mem = 8GB
walltime = 2-00:00:00
wd = /workdir
stdout = /logs/batchq-%JOBID.out
stderr = /logs/batchq-%JOBID.err
hold = false
env = false

[simple_runner]
max_procs = 4
max_mem = 16GB
max_walltime = 1-00:00:00
use_cgroup_v2 = false
use_cgroup_v1 = false

[slurm_runner]
account = acct123
max_jobs = 200
```

Hidden helper: `batchq debug` shows config and paths.
