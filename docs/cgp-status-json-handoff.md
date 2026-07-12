# Handoff: add `batchq status --json` (for `cgp status` / `hux` monitoring)

> Audience: a Claude Code instance working in the **batchq** repo (`batchq_v1`).
> This is a spec, not a patch — explore the repo and implement idiomatically.

## Why

`cgp status --json` (in the cgpipe Go repo, `../cgp`, added in PR #5) shells out to
`batchq` to report per-job status, normalizes it, and re-emits JSON for the `hux`
job-monitor dashboard. Today it can only get **two** things from batchq: the state
word (`batchq status --porcelain <id>`) and the end time (`batchq status -e <id>`).
Every other detail field (submit/start times, exit code, node, cpus, mem, reason,
work dir, stdout/stderr paths, deps) is **omitted for batchq jobs** because batchq
has no machine-readable way to surface them.

batchq already computes all of this — it lives in the `api.JobDTO`
(`api/types.go:22`), which is already fully JSON-tagged. We just need a
`status --json` flag that emits the DTO. cgp will parse it directly.

## What to build

Add a **`--json`** flag to the existing `status` command (`cmd/show.go`,
`statusCmd` at ~`:294`; flags registered at ~`:426-430` next to `--porcelain`).

**Behavior:** `batchq status --json <id...>` prints a **single JSON array** of the
per-job `api.JobDTO` values — one element per queried id, in argument order — and
nothing else on stdout. Example:

```
$ batchq status --json 1042
[
  {
    "job_id": "1042",
    "status": "RUNNING",
    "name": "align.bam",
    "submit_time": "2026-06-10T12:30:00Z",
    "start_time": "2026-06-10T12:34:56Z",
    "return_code": 0,
    "after_ok": ["1041"],
    "running_details": { "exec_host": "gpu-04", "cpus": "8", "mem_used": "3.2G" },
    "details": { "queue": "gpu", "mem_req": "16G", "work_dir": "/scratch/run" },
    "output_files": ["align.bam"]
  }
]
```

Notes / requirements:

- **Reuse the DTO as-is.** `api.JobDTO` already has the JSON tags cgp will read
  (`job_id`, `status`, `name`, `submit_time`, `start_time`, `end_time`,
  `return_code`, `after_ok`, `details`, `running_details`, `input_files`,
  `output_files`). Just `json.NewEncoder(os.Stdout).SetIndent(...)` the slice —
  don't invent a new shape.
- **A JSON array, even for one id** (simplest for the consumer to `Unmarshal`;
  matches the multi-id `status {job1 job2...}` surface). An unknown id should be
  **omitted** from the array (not an error, not a null) — consistent with how
  `--porcelain` prints nothing for an unknown id and exits 0.
- **`--json` is exclusive with the human table and `--porcelain`** (if both are
  passed, `--json` wins, or error — your call). The `-s/-b/-e/-t` time-suffix flags
  are irrelevant under `--json` (times are always present as fields); ignore them.
- **Times as RFC3339 UTC.** `*time.Time` already marshals to RFC3339; ensure they're
  stored/marshaled in UTC (cgp parses with `time.RFC3339`), matching the existing
  `-e` end-time contract.
- Register the flag: `statusCmd.Flags().BoolVar(&statusJSON, "json", false, "Machine-readable: a JSON array of job objects")`.

### Populate the detail maps (best-effort, incremental)

cgp reads scheduler specifics from `details` / `running_details` by **documented
key**. batchq should fill in whatever the backend knows; cgp omits any key that's
absent, so this can land field-by-field. Standard keys cgp will look for (check
either map):

| cgp field | key(s) cgp reads |
|---|---|
| nodes (exec host) | `exec_host`, `node`, `nodes` |
| cpus | `cpus`, `ncpus` |
| mem_req | `mem_req` |
| mem_used | `mem_used`, `maxvmem` |
| reason (pending/blocked) | `reason` |
| partition/queue | `queue`, `partition` |
| time_limit | `time_limit`, `walltime` |
| elapsed | `elapsed` |
| account | `account` |
| user | `user` |
| work_dir | `work_dir` |
| stdout_path | `stdout`, `stdout_path` |
| stderr_path | `stderr`, `stderr_path` |

(`exit_code` and the three times come from the DTO's top-level `return_code` /
`*_time` fields, not the maps.) You already stash backend state under keys like
`slurm_status` / `slurm_job_id` in `running_details` — same mechanism, just add the
resource/placement keys as the backend exposes them.

## The cgp side (already implemented — cgp PR #6)

cgp's `batchqDetail` (`../cgp/internal/runner/sched/detail.go`) already consumes this:

1. Runs `batchq status --json <id>`, `json.Unmarshal`s the array, picks the matching id.
2. Maps `status` → native word + normalized state (`CANCELED` → `cancelled`);
   `return_code` → `exit_code` (terminal states only); the `*_time` fields.
3. Pulls placement/resource fields from **batchq's own native keys** in
   `details`/`running_details` — cgp checks a few candidate names per field
   (e.g. `nodes` ← `host`/`exec_host`/`node`). **batchq does not need to rename
   anything to match cgp** — cgp does the conversion internally. The key table
   above is just what cgp *looks for*; populate whatever the backend already has
   under its own names and cgp will pick it up (add more candidate names on the
   cgp side if batchq uses a key not listed).
4. **Falls back** to the current `--porcelain` state + `-e` end-time probes when
   `--json` is unsupported (cached, mirroring `batchqPorcelainUnsupported`), so an
   older batchq keeps working and transparently upgrades.

So the two sides are decoupled: cgp already degrades gracefully, and shipping this
batchq `--json` change lights up the richer fields automatically.

## Tests

Add a `cmd/show_test.go` case (there's an existing one) that runs `status --json`
against a seeded fake job and asserts the emitted JSON `Unmarshal`s into a
`[]api.JobDTO` with the expected `status`, `return_code`, times, and a couple of
`running_details` keys. Keep `--porcelain` output byte-for-byte unchanged
(cgp's fallback path and any other consumer depend on it).

## Acceptance

`batchq status --json <id>` prints a JSON array of `api.JobDTO`; unknown ids are
omitted; times are RFC3339 UTC; `--porcelain` is unchanged. cgp can then
`json.Unmarshal` it directly and populate the Tier-2 fields of `cgp status --json`
for batchq-backed jobs.
