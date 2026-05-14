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
// where the queue lives, and how to authenticate.
type BatchqConfig struct {
	// Runner selects the runner type when `batchq run` is invoked without
	// an explicit --slurm / runner flag. "simple" | "slurm".
	Runner string `toml:"runner"`

	// Backend is the queue location URL with a scheme. Exactly one form:
	//   sqlite3:///path/to/db                  — local SQLite
	//   postgres://user:pass@host:5432/dbname  — local server, Postgres (future)
	//   batchq-remote://host[:port]/path       — remote HTTPS REST API
	Backend string `toml:"backend"`

	// Token is the bearer token used when Backend is a batchq-remote://
	// URL. Ignored for local backends.
	Token string `toml:"token"`

	// Multiuser triggers display modes that surface job owners. Set on
	// shared deployments where many users share a batchq.
	Multiuser bool `toml:"multiuser"`
}

// ServerConfig holds server-runtime knobs. None of these apply when
// Backend is a batchq-remote:// URL (no local server is running).
type ServerConfig struct {
	Listen      string   `toml:"listen"`
	IdleTimeout Duration `toml:"idle_timeout"`
	SqliteWAL   bool     `toml:"sqlite_wal"`
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
}

type SimpleRunnerConfig struct {
	MaxProcs    int    `toml:"max_procs"`
	MaxMem      string `toml:"max_mem"`
	MaxWalltime string `toml:"max_walltime"`
	UseCgroupV1 bool   `toml:"use_cgroup_v1"`
	UseCgroupV2 bool   `toml:"use_cgroup_v2"`
	Shell       string `toml:"shell"`
}

type SlurmRunnerConfig struct {
	User      string `toml:"user"`
	Account   string `toml:"account"`
	Partition string `toml:"partition"`
	MaxJobs   int    `toml:"max_jobs"`
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
// values before defaults are layered on top.
func (c *Config) Clone() *Config {
	if c == nil {
		return nil
	}
	cp := *c
	return &cp
}

// EnvOverrides is the set of config knobs that may be overridden by an
// environment variable. We deliberately keep this list short — env vars
// are useful for secrets and for CI / container deployments, not as a
// general substitute for the config file. Today only BATCHQ_TOKEN is
// wired up, so operators can keep the bearer token out of argv (where
// `ps` would expose it) and out of a checked-in config.
type EnvOverrides struct {
	Token string // BATCHQ_TOKEN
}

// ReadEnvOverrides snapshots the env vars that Config consumes. Calling
// it twice in one process is fine — there's no caching.
func ReadEnvOverrides() EnvOverrides {
	return EnvOverrides{
		Token: os.Getenv("BATCHQ_TOKEN"),
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
}

// ApplyDefaults fills empty fields of c with values from d. After this
// call, call sites can read Config.Server.Listen and friends and get a
// resolved value without re-implementing the fallback chain.
//
// Only knobs that have a meaningful built-in default are touched —
// JobDefaults numeric/optional fields (procs, mem, walltime, hold, env,
// name) stay zero-valued because their absence is itself meaningful.
func (c *Config) ApplyDefaults(d Defaults) {
	if c.Batchq.Backend == "" {
		c.Batchq.Backend = d.Backend
	}
	if c.Batchq.Runner == "" {
		c.Batchq.Runner = "simple"
	}
	if c.Server.Listen == "" {
		c.Server.Listen = d.ServerListen
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
