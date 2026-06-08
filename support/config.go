package support

// config.go is the typed TOML loader for ~/.batchq/config. Each section is
// a named struct; reading a value is a plain field access. Empty strings,
// zero ints, false bools, and zero Durations all mean "not set" — callers
// fall back to flag values or built-in defaults at each site.

import (
	"fmt"
	"os"
	"time"

	"github.com/BurntSushi/toml"
)

// Config is the in-memory shape of the batchq config file.
type Config struct {
	Batchq       BatchqConfig       `toml:"batchq"`
	Server       ServerConfig       `toml:"server"`
	Web          WebConfig          `toml:"web"`
	JobDefaults  JobDefaultsConfig  `toml:"job_defaults"`
	SimpleRunner SimpleRunnerConfig `toml:"simple_runner"`
	SlurmRunner  SlurmRunnerConfig  `toml:"slurm_runner"`
}

// BatchqConfig holds deployment-wide settings: which runner is in use,
// the (optional) remote API server to talk to, and how to authenticate.
type BatchqConfig struct {
	// Runner selects the runner type when `batchq run` is invoked without
	// an explicit --slurm / runner flag. "simple" | "slurm".
	Runner string `toml:"runner"`

	// Remote is the HTTPS URL of a remote batchq server (e.g.
	// "https://host[:port]/subpath"). When set, all clients dial this
	// URL and no local server is started. When empty, clients use the
	// local [server] socket.
	//
	// Only https:// is accepted. Operators terminate TLS at a reverse
	// proxy that fronts the remote server's unix socket. The default
	// port is 443.
	Remote string `toml:"remote"`

	// Token is the bearer token used when Remote is set. Ignored for the
	// purely local socket path.
	Token string `toml:"token"`

	// Multiuser triggers display modes that surface job owners. Set on
	// shared deployments where many users share a batchq.
	Multiuser bool `toml:"multiuser"`

	// AutospawnWaitTimeout caps how long a CLI client waits for a
	// freshly-spawned local server to bind its socket. Bumping this is
	// useful on slow filesystems (NFS, Lustre) where 5s is tight.
	AutospawnWaitTimeout Duration `toml:"autospawn_wait_timeout"`

	// Log, if set to a file path, turns on the lifecycle debug log: every
	// client appends timestamped, PID-tagged lines when it spawns/connects to
	// a server and when a request fails, and every (auto)spawned server logs
	// its start, each request it serves, every shutdown (with reason), and any
	// DB error (e.g. SQLITE_BUSY) — to the SAME file, so overlapping server
	// PIDs are visible at a glance. Overridable per-invocation with --log.
	Log string `toml:"log"`
}

// ServerConfig holds server-runtime knobs. None of these apply when
// [batchq] remote is set (no local server is running).
type ServerConfig struct {
	Listen string `toml:"listen"`

	// DB is the database backend URL the server opens. Exactly one form:
	//   sqlite3:///path/to/db                  — local SQLite
	//   postgres://user:pass@host:5432/dbname  — local Postgres (future)
	DB          string   `toml:"db"`
	IdleTimeout Duration `toml:"idle_timeout"`
	SqliteWAL   bool     `toml:"sqlite_wal"`

	// ReadPoolSize sets how many connections serve concurrent reads. Default
	// (0/1) shares the single writer connection — historical behavior. A value
	// > 1 opens a separate read pool of that size so status-poll bursts run
	// concurrently. Trade-off: > 1 re-introduces reader↔writer SQLite lock
	// contention (bounded by busy_timeout) and more fcntl locking on NFS; set
	// back to 1 to revert. `--read-pool-size` overrides.
	ReadPoolSize int `toml:"read_pool_size"`

	// Token, if non-empty, turns on shared-token auth: every API request
	// (except the health check) must carry `Authorization: Bearer <token>`
	// matching this value. Left empty (the default), the server enforces
	// nothing in-band and relies on unix-socket filesystem permissions.
	// This is a single shared secret — deliberately a "secure-enough"
	// floor for a self-hosted single-user server, not per-user auth.
	// Prefer the BATCHQ_SERVER_TOKEN env var so the secret stays out of a
	// checked-in config file.
	Token string `toml:"token"`
}

type WebConfig struct {
	Socket string `toml:"socket"`
	Listen string `toml:"listen"`
}

type JobDefaultsConfig struct {
	Name     string `toml:"name"`
	Procs    int    `toml:"procs"`
	Mem      string `toml:"mem"`
	Walltime string `toml:"walltime"`
	Wd       string `toml:"wd"`
	Stdout   string `toml:"stdout"`
	Stderr   string `toml:"stderr"`
	Hold     bool   `toml:"hold"`
	Env      bool   `toml:"env"`

	// Cluster / Host are default required resources attached to every job this
	// user submits — the submit-side counterpart to the runner's advertised
	// cluster/host. They only matter once a single server fronts multiple
	// clusters/hosts: the scheduler then routes a job to a runner advertising
	// the matching cluster/host. Locally (one runner) they are redundant and
	// usually left unset. Mirrored by submit's --cluster / --host flags;
	// an explicit --resource (or #BATCHQ -resource) of the same name overrides.
	Cluster string `toml:"cluster"`
	Host    string `toml:"host"`

	// Resources is a map of additional default required resources attached to
	// every submission (e.g. scratch = "500GB"), the submit-side counterpart to
	// [*_runner.resources]. Overridable per-submit by --resource.
	Resources map[string]string `toml:"resources"`
}

type SimpleRunnerConfig struct {
	MaxProcs    int    `toml:"max_procs"`
	MaxMem      string `toml:"max_mem"`
	MaxWalltime string `toml:"max_walltime"`
	UseCgroupV1 bool   `toml:"use_cgroup_v1"`
	UseCgroupV2 bool   `toml:"use_cgroup_v2"`
	Shell       string `toml:"shell"`

	// MaxJobs caps how many jobs this runner runs concurrently. Zero / unset
	// means unlimited (bounded only by max_procs/max_mem). Mirrors the
	// --max-jobs flag.
	MaxJobs int `toml:"max_jobs"`

	// RunnerID is the stable identity this runner reports to the server, so the
	// Runners view shows one row per runner that updates in place across
	// restarts (rather than a new row per invocation). Empty / unset defaults
	// to the hostname. Mirrors the --runner-id flag.
	RunnerID string `toml:"runner_id"`

	// Host is the hostname this runner advertises to the server on each claim
	// (so a remote server's Runners view can show which machine ran a job).
	// Empty / unset defaults to the OS hostname.
	Host string `toml:"host"`

	// Cluster is advertised as a "cluster" resource on every claim — a
	// convenience for the common case of tagging a runner with the cluster it
	// runs on, so jobs requiring that cluster are routed here. Equivalent to
	// adding cluster = "..." under [simple_runner.resources].
	Cluster string `toml:"cluster"`

	// Resources advertises generic resources this runner provides, e.g.
	// [simple_runner.resources] with gpu = "4", cluster = "xyz_cluster".
	// Counts are integer strings; labels are plain or comma-separated sets.
	Resources map[string]string `toml:"resources"`
}

type SlurmRunnerConfig struct {
	User      string `toml:"user"`
	Account   string `toml:"account"`
	Partition string `toml:"partition"`

	// MaxJobs caps how many jobs a single `batchq run --slurm`
	// invocation submits before exiting. Each submission decrements the
	// available count. Zero / unset means unlimited.
	MaxJobs int `toml:"max_jobs"`

	// MaxSlurmJobs caps how many of this user's jobs may be in the SLURM
	// queue at once. The runner polls `squeue` and pauses submitting when
	// the live count reaches this limit. Zero / unset means unlimited.
	MaxSlurmJobs int `toml:"max_slurm_jobs"`

	// RunnerID is the stable identity this runner reports to the server.
	// Empty / unset defaults to the hostname. Mirrors the --runner-id flag.
	RunnerID string `toml:"runner_id"`

	// Host is the hostname this runner advertises to the server on each claim.
	// Empty / unset defaults to the OS hostname.
	Host string `toml:"host"`

	// Cluster is advertised as a "cluster" resource on every claim (convenience
	// for tagging this SLURM runner with its cluster). Equivalent to a
	// cluster = "..." entry under [slurm_runner.resources].
	Cluster string `toml:"cluster"`

	// Resources advertises generic resources this SLURM runner provides
	// (e.g. cluster = "xyz_cluster"), so resource-tagged jobs can be routed
	// to the right cluster. procs/mem/walltime are still enforced by SLURM.
	Resources map[string]string `toml:"resources"`
}

// LoadConfig parses a TOML config file from path. A missing file yields
// a zero-value Config (no error) so callers can rely on defaults when
// no config is installed. A malformed file is reported as an error.
//
// LoadConfig returns the raw parsed config (no defaults applied). To get
// a resolved config that call sites can read directly, follow up with
// cfg.ApplyDefaults(NewDefaults()).
func LoadConfig(path string) (*Config, error) {
	cfg := &Config{}
	if path == "" {
		return cfg, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	if _, err := toml.Decode(string(data), cfg); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	return cfg, nil
}

// Clone returns a deep copy of c. Used to retain the raw TOML-loaded
// values before defaults are layered on top, so later mutation of the
// resolved config can't corrupt the retained snapshot. The map-valued
// fields (the resource subtables) are copied explicitly; a plain struct
// copy would share them.
func (c *Config) Clone() *Config {
	if c == nil {
		return nil
	}
	cp := *c
	cp.JobDefaults.Resources = cloneStringMap(c.JobDefaults.Resources)
	cp.SimpleRunner.Resources = cloneStringMap(c.SimpleRunner.Resources)
	cp.SlurmRunner.Resources = cloneStringMap(c.SlurmRunner.Resources)
	return &cp
}

// cloneStringMap returns a shallow copy of m, or nil if m is nil.
func cloneStringMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// EnvOverrides is the set of config knobs that may be overridden by an
// environment variable. We deliberately keep this list short — env vars
// are useful for secrets and for CI / container deployments, not as a
// general substitute for the config file. The two wired up today are
// both secrets, so operators can keep them out of argv (where `ps` would
// expose them) and out of a checked-in config:
//   - BATCHQ_TOKEN        — the client's bearer token ([batchq] token)
//   - BATCHQ_SERVER_TOKEN — the server's required shared token ([server] token)
type EnvOverrides struct {
	Token       string // BATCHQ_TOKEN
	ServerToken string // BATCHQ_SERVER_TOKEN
}

// ReadEnvOverrides snapshots the env vars that Config consumes. Calling
// it twice in one process is fine — there's no caching.
func ReadEnvOverrides() EnvOverrides {
	return EnvOverrides{
		Token:       os.Getenv("BATCHQ_TOKEN"),
		ServerToken: os.Getenv("BATCHQ_SERVER_TOKEN"),
	}
}

// ApplyEnv layers env-var overrides onto c. An empty env var leaves the
// existing field (from TOML) alone — env "absent" is not the same as
// env "explicitly empty"; we treat both as no-override so that an
// unset env var can't accidentally blank out a config value.
func (c *Config) ApplyEnv(e EnvOverrides) {
	if e.Token != "" {
		c.Batchq.Token = e.Token
	}
	if e.ServerToken != "" {
		c.Server.Token = e.ServerToken
	}
}

// ApplyDefaults fills empty fields of c with values from d. After this
// call, call sites can read Config.Server.Listen and friends and get a
// resolved value without re-implementing the fallback chain.
//
// Only knobs that have a meaningful built-in default are touched —
// JobDefaults numeric/optional fields (procs, mem, walltime, hold, env,
// name) stay zero-valued because their absence is itself meaningful.
func (c *Config) ApplyDefaults(d Defaults) {
	if c.Batchq.Runner == "" {
		c.Batchq.Runner = "simple"
	}
	if c.Batchq.AutospawnWaitTimeout.AsDuration() == 0 {
		c.Batchq.AutospawnWaitTimeout = Duration(d.AutospawnWaitTimeout)
	}
	if c.Server.Listen == "" {
		c.Server.Listen = d.ServerListen
	}
	if c.Server.DB == "" {
		c.Server.DB = d.Backend
	}
	if c.Web.Socket == "" {
		c.Web.Socket = d.WebSocket
	}
	if c.JobDefaults.Stdout == "" {
		c.JobDefaults.Stdout = d.JobStdout
	}
	if c.JobDefaults.Stderr == "" {
		c.JobDefaults.Stderr = d.JobStderr
	}
	if c.JobDefaults.Wd == "" {
		c.JobDefaults.Wd = d.JobWd
	}
	if c.SimpleRunner.Shell == "" {
		c.SimpleRunner.Shell = d.Shell
	}
}

// Duration wraps time.Duration so TOML can decode strings like "1m".
type Duration time.Duration

// UnmarshalText accepts any Go-duration string (e.g. "1m", "30s", "1h30m").
// An empty string decodes to zero.
func (d *Duration) UnmarshalText(text []byte) error {
	s := string(text)
	if s == "" {
		*d = 0
		return nil
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

// AsDuration returns the underlying time.Duration value.
func (d Duration) AsDuration() time.Duration { return time.Duration(d) }
