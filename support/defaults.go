package support

// defaults.go centralises every built-in default that depends on
// $BATCHQ_HOME so call sites don't drift. Resolution order at every
// site stays: flag > env > config > default — this file owns just the
// last step.

import (
	"path/filepath"
	"time"
)

// Default file names under $BATCHQ_HOME.
const (
	DefaultSocketFile    = "batchq.sock"
	DefaultDBFile        = "batchq.db"
	DefaultWebSocketFile = "batchq-web.sock"
	DefaultConfigFile    = "config"
)

// Defaults bundles the home-relative defaults plus a few constants used
// across subcommands. Build it with NewDefaults; Home is resolved once
// so every dependent path stays consistent.
type Defaults struct {
	Home string

	Backend      string // sqlite3://$HOME/batchq.db
	ServerListen string // unix://$HOME/batchq.sock
	WebSocket    string // $HOME/batchq-web.sock
	ConfigFile   string // $HOME/config

	Shell                string        // job interpreter when none is configured
	AutospawnIdleTimeout time.Duration // server lifetime when forked by a CLI client
	AutospawnWaitTimeout time.Duration // how long a client waits for the spawned server to bind

	JobStdout string // submit-time default for the captured stdout path
	JobStderr string // submit-time default for the captured stderr path
	JobWd     string // submit-time default for the working directory
}

// NewDefaults captures the defaults for a freshly resolved $BATCHQ_HOME.
func NewDefaults() Defaults {
	home := GetBatchqHome()
	return Defaults{
		Home:                 home,
		Backend:              "sqlite3://" + filepath.Join(home, DefaultDBFile),
		ServerListen:         "unix://" + filepath.Join(home, DefaultSocketFile),
		WebSocket:            filepath.Join(home, DefaultWebSocketFile),
		ConfigFile:           filepath.Join(home, DefaultConfigFile),
		Shell:                "/bin/bash",
		AutospawnIdleTimeout: time.Minute,
		AutospawnWaitTimeout: 10 * time.Second,
		JobStdout:            "./batchq-%JOBID.stdout",
		JobStderr:            "./batchq-%JOBID.stderr",
		JobWd:                ".",
	}
}
