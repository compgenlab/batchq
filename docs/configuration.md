# Configuration

batchq is configured from three sources, in this order of precedence:

1. **Command-line flag** on the subcommand being run (highest).
2. **Environment variable** (only a small set is consumed today).
3. **Config file**, `$BATCHQ_HOME/config`, parsed as TOML.
4. **Built-in default** (lowest).

The lower three layers (env, config, default) are folded into a single
resolved `Config` at startup. After that, call sites just read the
field they need — there is no per-site fallback chain.

Whenever something is not behaving the way you expect, run
`batchq debug`. It prints `$BATCHQ_HOME`, the config file path, every
environment variable consumed, and every knob in every section labelled
with its source (`flag` / `env` / `config` / `default` / `unset`). Token
values are redacted.

## `$BATCHQ_HOME`

Resolved before anything else by reading the `BATCHQ_HOME` environment
variable, falling back to `~/.batchq`. Every home-relative default
(database path, socket path, web socket path, config file path) is
derived from this — set it once and the whole layout moves with it.

## The config file

Lives at `$BATCHQ_HOME/config` (no extension). It is plain TOML. A
missing file is fine — every knob has either a built-in default or an
"unset means not configured" semantic.

A complete example:

```toml
[batchq]
runner = "simple"                 # "simple" or "slurm"
# remote = "https://batchq.example.com"
# token = ""                      # better to use BATCHQ_TOKEN env
multiuser = false
autospawn_wait_timeout = "10s"

[server]
listen = "unix:///home/me/.batchq/batchq.sock"
db = "sqlite3:///home/me/.batchq/batchq.db"
idle_timeout = "1m"               # zero / unset disables idle shutdown
sqlite_wal = false                # true ONLY when DB is on local disk

[web]
socket = "/home/me/.batchq/batchq-web.sock"
# listen = "127.0.0.1:8081"       # alternative to socket

[job_defaults]
procs = 4
mem = "8GB"
walltime = "2-00:00:00"
wd = "/workdir"
stdout = "/logs/batchq-%JOBID.out"
stderr = "/logs/batchq-%JOBID.err"
hold = false
env = false

[simple_runner]
max_procs = 4
max_mem = "16GB"
max_walltime = "1-00:00:00"
shell = "/bin/bash"
use_cgroup_v2 = false
use_cgroup_v1 = false

[simple_runner.resources]       # generic resources this runner advertises
gpu = "4"
cluster = "biocluster"

[slurm_runner]
user = "myuser"
account = "acct123"
partition = "general"
max_jobs = 0                      # zero / unset means unlimited
max_slurm_jobs = 200

[slurm_runner.resources]        # usually cluster/feature routing labels
cluster = "biocluster"
```

## `[batchq]` — deployment-wide

| Key | Type | Default | Meaning |
|---|---|---|---|
| `runner` | string | `"simple"` | Which runner `batchq run` uses by default. `"simple"` or `"slurm"`. The `--slurm` flag overrides. |
| `remote` | string | (unset) | HTTPS URL of a remote batchq server. When set, clients dial this URL and no local server is started. See [remote](remote.md). |
| `token` | string | (unset) | Bearer token for `remote`. Prefer `BATCHQ_TOKEN` env so the value stays out of `ps` and out of checked-in config. |
| `multiuser` | bool | `false` | Turns on owner-displaying modes in `show` output. Set on shared deployments. |
| `autospawn_wait_timeout` | duration | `10s` | How long a CLI client waits for an autospawned local server to bind its socket. Bump on slow filesystems. |

## `[server]` — server runtime

Ignored entirely when `[batchq] remote` is set (no local server runs).
`batchq server` refuses to start in that case.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `listen` | string | `unix://$BATCHQ_HOME/batchq.sock` | The unix socket the server binds. Only `unix://` URLs are accepted. |
| `db` | string | `sqlite3://$BATCHQ_HOME/batchq.db` | The database URL. `sqlite3:///path` today; `postgres://…` is reserved for future use. |
| `idle_timeout` | duration | (unset, no shutdown) | If non-zero, the server shuts down after this duration of no requests. Autospawned servers use a built-in `1m` value. |
| `sqlite_wal` | bool | `false` | Opt into SQLite WAL journaling. Off by default because WAL's `-shm` shared-memory file is unsafe on NFS/Lustre. Only enable when the DB file is on local disk. |

## `[web]` — `batchq web` listener

| Key | Type | Default | Meaning |
|---|---|---|---|
| `socket` | string | `$BATCHQ_HOME/batchq-web.sock` | Unix socket for the web UI. |
| `listen` | string | (unset) | TCP `host:port` to listen on instead of a socket. Set exactly one of `socket` and `listen`. |

## `[job_defaults]` — submit-time defaults

These are applied at submission time when the corresponding `submit`
flag is not given. A zero value means "no default" — `submit` will use
the runner's own defaults.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `name` | string | (unset) | Default job name. |
| `procs` | int | (unset) | Default `--procs`. |
| `mem` | string | (unset) | Default `--mem` (e.g. `"8GB"`). |
| `walltime` | string | (unset) | Default `--walltime` (e.g. `"1-00:00:00"`). |
| `wd` | string | `.` | Default working directory. |
| `stdout` | string | `./batchq-%JOBID.stdout` | Default stdout file. `%JOBID` is substituted with the assigned job ID. If a directory is given, the file lands inside it. |
| `stderr` | string | `./batchq-%JOBID.stderr` | Default stderr file. Same substitution rules. |
| `hold` | bool | `false` | Submit every job held. |
| `env` | bool | `false` | Capture the submitter's environment by default. |

## `[simple_runner]` — local runner caps

Read by `batchq run` (without `--slurm`). Per-job ceilings: any job
that requests more than these values stays in the queue and waits.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `max_procs` | int | (unset) | Per-job processor ceiling the runner enforces. |
| `max_mem` | string | (unset) | Per-job memory ceiling (e.g. `"16GB"`). |
| `max_walltime` | string | (unset) | Per-job walltime ceiling. |
| `shell` | string | `/bin/bash` | The interpreter used to run job scripts. |
| `use_cgroup_v1` | bool | `false` | Enforce procs/mem via cgroup v1. Requires root. |
| `use_cgroup_v2` | bool | `false` | Enforce procs/mem via cgroup v2. Requires root. |

### `[simple_runner.resources]` — what this runner advertises

A subtable of `name = "value"` entries declaring the generic resources
this runner provides, beyond procs/mem/walltime. A job's `--resource`
requirements are matched against what's advertised here. Integer strings
are **counts** (consumed by running jobs); other strings are **labels**;
an empty value is a **feature flag**.

```toml
[simple_runner.resources]
gpu = "4"                 # count: 4 GPUs
"gpu:a100" = "4"          # typed count (quote keys with ':')
cluster = "biocluster"    # label
fastio = ""               # feature flag
```

A runner with no `resources` subtable advertises nothing and so claims
only jobs that require nothing. `batchq run --resource name=value`
(repeatable) overrides individual keys for one invocation. See
[Generic resources](resources.md) for the matching rules.

## `[slurm_runner]` — SLURM-runner specifics

Read by `batchq run --slurm` (or when `[batchq] runner = "slurm"`).

| Key | Type | Default | Meaning |
|---|---|---|---|
| `user` | string | current user | The SLURM username used for `squeue` lookups. |
| `account` | string | (unset) | SLURM account to pass to `sbatch`. |
| `partition` | string | (unset) | SLURM partition to pass to `sbatch`. |
| `max_jobs` | int | `0` (unlimited) | Cap on how many jobs the runner submits in one `batchq run --slurm` invocation. |
| `max_slurm_jobs` | int | `0` (unlimited) | Cap on this user's live SLURM-queue jobs. The runner polls `squeue` and pauses submission when this is reached. |

### `[slurm_runner.resources]` — cluster routing labels

Same subtable shape as `[simple_runner.resources]`, but for the SLURM
runner these are typically **labels** that route resource-tagged jobs to
the right cluster — procs/mem/walltime stay enforced by SLURM itself
through the generated `sbatch` flags, so you don't re-declare them here.

```toml
[slurm_runner.resources]
cluster = "biocluster"
```

A job submitted with `--resource cluster=biocluster` is then claimed only
by the SLURM runner advertising that label. See
[Generic resources](resources.md).

## Environment variables

Only a small, deliberately short list is consumed. Env vars are useful
for secrets and CI / container deployments, not as a general substitute
for the config file.

| Variable | Overrides | Notes |
|---|---|---|
| `BATCHQ_HOME` | Resolves before anything else | Defaults to `~/.batchq`. |
| `BATCHQ_TOKEN` | `[batchq] token` | Recommended place to put the bearer token — stays out of `ps` and out of any checked-in config. An empty value is treated as unset (so an unset env var cannot accidentally blank a config value). |

Adding another env override means extending `support.EnvOverrides` and
`Config.ApplyEnv`. `batchq debug` will pick the new variable up
automatically once it is wired in.

## Persistent flags

Available on every subcommand, defined on the root command:

| Flag | Overrides | Notes |
|---|---|---|
| `--remote URL` | `[batchq] remote` | Talk to a remote server for just this invocation. Skips autospawn. |
| `--token TOKEN` | `[batchq] token`, `BATCHQ_TOKEN` | Pass a bearer token on the command line. Visible in `ps`; prefer the env var. |
| `--no-autospawn` | — | Refuse to fork-exec a local server when the socket is unreachable. |
| `--restart-server` | — | Shut down the running local server before this command. No effect when the backend is remote. |

## Resolution rules

For each knob the resolved value is the first non-empty source:

- **Strings:** a non-empty flag wins, then a non-empty env var, then a
  non-empty config value, then the default.
- **Numbers and booleans:** a flag explicitly set wins, otherwise the
  config value if non-zero, otherwise the default. Zero values mean
  "not set" for the optional numeric knobs (procs, mem, walltime caps),
  so a config that sets `max_procs = 0` is treated the same as no
  config at all.
- **Durations:** parsed with Go's `time.ParseDuration` (`"1m"`,
  `"30s"`, `"1h30m"`). An empty string is zero.

For knobs that *do* have a built-in default (`Server.Listen`,
`Server.DB`, `Web.Socket`, `Batchq.Runner`, `Batchq.AutospawnWaitTimeout`,
`JobDefaults.Stdout`, `JobDefaults.Stderr`, `JobDefaults.Wd`,
`SimpleRunner.Shell`), the resolved `Config` field is never empty after
init. For optional knobs (every numeric or bool field, plus
`Batchq.Remote`, `JobDefaults.Procs/Mem/Walltime/Name`,
`SlurmRunner.User/Account/Partition`), zero values still mean "not
set" and call sites treat them that way.

## Debugging configuration

```sh
batchq debug
```

Prints the fully resolved configuration with the source of every
non-default value. This is the authoritative answer when you need to
know "where is this setting coming from?"

The output includes:

- `$BATCHQ_HOME` and how it was resolved.
- The config file path and whether it was found.
- Every environment variable consumed.
- Every knob in every section, with a `source` column.

Use it whenever a flag, env var, or config value does not appear to be
taking effect — it eliminates guessing.

## Where to go next

- [Submitting jobs](submitting-jobs.md) — every `submit` flag and how
  it maps onto `[job_defaults]`.
- [Running jobs](running-jobs.md) — how `[simple_runner]` ceilings are
  enforced.
- [Generic resources](resources.md) — the `resources` subtables and how
  requirements are matched.
- [SLURM](slurm.md) — `[slurm_runner]` in context.
- [Remote access](remote.md) — `[batchq] remote` and `[batchq] token`.
