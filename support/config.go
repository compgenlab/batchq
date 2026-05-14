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
	Lock        string   `toml:"lock"`
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
