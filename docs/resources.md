# Generic resources (GPUs and friends)

`--procs`, `--mem`, and `--walltime` cover the universal resources every
job has. For everything else — GPUs, licenses, a specific cluster, a node
feature — batchq has a generic mechanism: a job **requires** named
resources, a runner **advertises** the resources it has, and a runner only
claims a job whose requirements it can satisfy.

```sh
batchq submit --resource gpu=2 ./train.sh        # needs 2 GPUs
batchq run    --resource gpu=4                    # this runner has 4
```

The runner with four GPUs will claim the job (and, while it runs, advertise
only two to the next claim). A runner that advertises no GPUs never picks
it up — the job waits for one that can.

## Requesting resources at submit time

`--resource <name[=value]>` is repeatable and mirrored by
`#BATCHQ -resource` inside the script:

```sh
batchq submit --resource gpu=2 --resource fastio ./job.sh
```

```sh
#BATCHQ -resource gpu=2
#BATCHQ -resource fastio
```

`procs`, `mem`, and `walltime` are **reserved** names — batchq rejects them
as `--resource` so you don't accidentally bypass the dedicated flags. Use
`-p`/`-m`/`-t` for those.

### Three kinds of resource

batchq infers what a requirement *means* from the shape of its value — you
don't declare a type:

| Form | Example | Meaning | Matched by |
|---|---|---|---|
| **Count** | `gpu=2` | need at least N | advertised count `>=` required count |
| **Typed count** | `gpu:a100=2` | need N of a specific variant | advertised `gpu:a100` count `>=` 2 |
| **Label / set** | `cluster=biocluster` | must run somewhere with this label | advertised set must contain it |
| **Feature flag** | `fastio` (no value) | a capability must be present | advertised by name |

A few details that follow from those rules:

- **Untyped counts absorb typed supply.** A request for `gpu=2` is
  satisfied by a runner advertising `gpu=2`, *or* `gpu:a100=3`, *or* a mix
  — all advertised `gpu`/`gpu:<type>` counts are summed. Ask for
  `gpu:a100=2` and only `a100` supply counts.
- **Labels are sets.** `--resource cluster=a,b` requires a runner whose
  advertised `cluster` set is a superset of `{a, b}`.
- **A runner that advertises nothing** claims only jobs that require
  nothing. So tagging *some* jobs with resources doesn't strand them: a
  bare `batchq run` simply won't touch them, and a runner that advertises
  the resource will.

## Advertising resources on a runner

A runner declares what it has two ways. On the command line,
`--resource <name=value>` is repeatable:

```sh
batchq run --resource gpu=4 --resource cluster=biocluster --forever
```

Or in the config file, under a `resources` subtable of the runner section:

```toml
[simple_runner]
max_procs = 16
max_mem = "64GB"

[simple_runner.resources]
gpu = "4"                 # countable: integer string
"gpu:a100" = "4"          # a typed variant (quote keys containing ':')
cluster = "biocluster"    # label
fastio = ""               # feature flag (presence only)
```

Command-line `--resource` flags override the config subtable per key, so
you can keep a base set in the config and tweak one value for a single run.

**Counts are consumed; labels are not.** For a countable resource, the
runner advertises *pool minus what its running jobs hold* on every claim —
so a 4-GPU runner already running a `gpu=2` job offers `gpu=2` to the next
job. Labels and feature flags are advertised as-is to every claim (a
cluster name isn't "used up").

### How idle runners decide to stop

When a runner can't claim anything, the claim response tells it *why*: a
job that fit its limits but lost the claim race (`MoreEligible` — worth
retrying) is different from a job that simply doesn't fit this runner's
limits or resources (`Blocked`). An idle, non-`--forever` runner that sees
only `Blocked` jobs stops instead of spinning forever on work it can never
satisfy. A `--forever` runner keeps polling.

## SLURM equivalents

Under `--slurm`, batchq translates the SLURM resource directives into the
same `--resource` model when you submit:

| SLURM directive | Becomes |
|---|---|
| `--gres=gpu:2` | `--resource gpu=2` |
| `--gres=gpu:a100:2` | `--resource gpu:a100=2` |
| `--gres=gpu:2,mps:50` | `--resource gpu=2 --resource mps=50` |
| `-C`, `--constraint=haswell` | `--resource haswell` (feature flag) |
| `--constraint=haswell&infiniband` | `--resource haswell --resource infiniband` |

```sh
batchq submit --slurm job.sbatch       # #SBATCH --gres / -C parsed into resources
```

(Only the simple AND forms of `--constraint` are supported — `a&b`, `a,b`,
`[a&b]`. OR / counts / `*` expressions are not translated.)

For the **SLURM runner**, resources are typically used as routing labels —
a `[slurm_runner.resources]` cluster/partition label so resource-tagged
jobs land on the right cluster — while procs/mem/walltime stay enforced by
SLURM itself through the generated `sbatch` flags:

```toml
[slurm_runner.resources]
cluster = "biocluster"
```

A job submitted with `--resource cluster=biocluster` is then only claimed
by the SLURM runner advertising that cluster, and handed to `sbatch`
there.

## Where to go next

- [Submitting jobs](submitting-jobs.md) — `--resource` alongside every
  other `submit` flag.
- [Configuration](configuration.md) — the `[simple_runner.resources]` and
  `[slurm_runner.resources]` subtables.
- [Running jobs locally](running-jobs.md) — how the simple runner enforces
  limits and advertises capacity.
- [Job arrays](job-arrays.md) — fan a resource request across a range of
  tasks.
