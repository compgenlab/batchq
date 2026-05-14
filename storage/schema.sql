-- batchq v2 schema. Identical entity shape to v1 except:
--   * timestamps are stored as RFC3339 strings (UTC).
--   * supporting indexes added for queue scans and dependent lookups.
-- Pragmas (foreign_keys, journal_mode, busy_timeout, synchronous) are set
-- from the driver-side DSN, not here, so that the same SQL can be re-applied
-- safely as a migration without overriding the runtime configuration.

CREATE TABLE IF NOT EXISTS jobs (
    id          TEXT PRIMARY KEY,
    status      INTEGER NOT NULL DEFAULT 0,
    priority    INTEGER NOT NULL DEFAULT 0,
    name        TEXT NOT NULL DEFAULT '',
    notes       TEXT NOT NULL DEFAULT '',
    submit_time TEXT NOT NULL DEFAULT '',
    start_time  TEXT NOT NULL DEFAULT '',
    end_time    TEXT NOT NULL DEFAULT '',
    return_code INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS job_details (
    job_id TEXT NOT NULL REFERENCES jobs(id),
    key    TEXT NOT NULL,
    value  TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (job_id, key)
);

CREATE TABLE IF NOT EXISTS job_deps (
    job_id     TEXT NOT NULL REFERENCES jobs(id),
    afterok_id TEXT NOT NULL REFERENCES jobs(id),
    PRIMARY KEY (job_id, afterok_id)
);

-- job_running stores the runner ownership claim. Its UNIQUE PRIMARY KEY on
-- job_id is the atomic-claim primitive: only one runner can INSERT for a
-- given job_id within a single transaction.
CREATE TABLE IF NOT EXISTS job_running (
    job_id     TEXT NOT NULL PRIMARY KEY REFERENCES jobs(id),
    job_runner TEXT NOT NULL DEFAULT '',
    kind       TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS job_running_details (
    job_id TEXT NOT NULL REFERENCES jobs(id),
    key    TEXT NOT NULL,
    value  TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (job_id, key)
);

-- Per-job input and output files. Optional submit-time metadata used for
-- pipeline introspection ("which job produced X?" / "which jobs need X?").
-- Multi-valued, so they get dedicated tables rather than CSV-encoded
-- job_details rows.
CREATE TABLE IF NOT EXISTS job_inputs (
    job_id TEXT NOT NULL REFERENCES jobs(id),
    path   TEXT NOT NULL,
    PRIMARY KEY (job_id, path)
);

CREATE TABLE IF NOT EXISTS job_outputs (
    job_id TEXT NOT NULL REFERENCES jobs(id),
    path   TEXT NOT NULL,
    PRIMARY KEY (job_id, path)
);

CREATE INDEX IF NOT EXISTS jobs_status_priority_submit
    ON jobs(status, priority DESC, submit_time, id);

CREATE INDEX IF NOT EXISTS job_deps_afterok
    ON job_deps(afterok_id, job_id);

CREATE INDEX IF NOT EXISTS job_details_key
    ON job_details(key, job_id);

-- Value-covering index so `WHERE key='run_id' AND value=?` (the workflow
-- run filter) is an index lookup instead of a scan.
CREATE INDEX IF NOT EXISTS job_details_kv
    ON job_details(key, value, job_id);

CREATE INDEX IF NOT EXISTS job_inputs_path
    ON job_inputs(path);

CREATE INDEX IF NOT EXISTS job_outputs_path
    ON job_outputs(path);
