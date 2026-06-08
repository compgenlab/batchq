package cmd

// debug.go renders `batchq debug` — a dump of the resolved
// configuration along with where each value came from. Source labels:
//
//   flag    — set on the command line (persistent flag on the root cmd)
//   env     — set via an environment variable (currently BATCHQ_HOME)
//   config  — set in the TOML config file
//   default — built-in default from support.NewDefaults()
//   unset   — no value at any layer; field is genuinely empty
//
// Resolution order at every site is: flag > env > config > default. The
// debug command compares the raw (TOML-only) Config against the
// resolved Config and the Defaults snapshot to figure out which layer
// supplied the final value.

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/mbreese/batchq/client"
	"github.com/mbreese/batchq/support"
)

type debugRow struct {
	key    string
	value  string
	source string
}

func printDebugConfig(w io.Writer) {
	d := defaultsResolved
	raw := rawConfig
	if raw == nil {
		raw = &support.Config{}
	}

	fmt.Fprintln(w, "# batchq debug — resolved configuration")
	fmt.Fprintln(w)

	homeSource := "default"
	if v := os.Getenv("BATCHQ_HOME"); v != "" {
		homeSource = "env BATCHQ_HOME=" + v
	}
	fmt.Fprintf(w, "home:        %s   (%s)\n", batchqHome, homeSource)

	cfgSource := "default"
	if _, err := os.Stat(configFile); err == nil {
		cfgSource = "loaded"
	} else {
		cfgSource = "not found"
	}
	fmt.Fprintf(w, "config:      %s   (%s)\n", configFile, cfgSource)

	fmt.Fprintln(w)
	fmt.Fprintln(w, "env vars:")
	fmt.Fprintf(w, "  BATCHQ_HOME          = %s\n", envOrUnset("BATCHQ_HOME"))
	fmt.Fprintf(w, "  BATCHQ_TOKEN         = %s\n", envOrRedacted("BATCHQ_TOKEN"))
	fmt.Fprintf(w, "  BATCHQ_SERVER_TOKEN  = %s\n", envOrRedacted("BATCHQ_SERVER_TOKEN"))
	fmt.Fprintln(w)

	sections := []struct {
		title string
		rows  []debugRow
	}{
		{"[batchq]", batchqRows(raw, d)},
		{"[server]", serverRows(raw, d)},
		{"[web]", webRows(raw, d)},
		{"[job_defaults]", jobDefaultsRows(raw, d)},
		{"[simple_runner]", simpleRunnerRows(raw, d)},
		{"[slurm_runner]", slurmRunnerRows(raw)},
	}
	for _, sec := range sections {
		fmt.Fprintln(w, sec.title)
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		for _, r := range sec.rows {
			fmt.Fprintf(tw, "  %s\t= %s\t(%s)\n", r.key, displayValue(r.value), r.source)
		}
		tw.Flush()
		fmt.Fprintln(w)
	}

	printServerStatus(w)
}

// printServerStatus probes the configured server endpoint (never autospawning)
// and reports whether a server is running, its PID, and instance id — i.e. can
// it be reached / PINGed. Mirrors dialClient's endpoint resolution.
func printServerStatus(w io.Writer) {
	fmt.Fprintln(w, "server status:")

	remoteRaw := clientRemote
	if remoteRaw == "" {
		remoteRaw = Config.Batchq.Remote
	}

	dialURL := Config.Server.Listen
	mode := "local"
	if remoteRaw != "" {
		u, err := support.ParseRemote(remoteRaw)
		if err != nil {
			fmt.Fprintf(w, "  endpoint:    %s   (remote, invalid: %v)\n", remoteRaw, err)
			return
		}
		dialURL = u
		mode = "remote"
	}
	fmt.Fprintf(w, "  endpoint:    %s   (%s)\n", dialURL, mode)

	token := clientToken
	if token == "" {
		token = Config.Batchq.Token
	}

	c, err := client.DialWithOptions(client.Options{
		URL:     dialURL,
		Token:   token,
		Timeout: 3 * time.Second,
	})
	if err != nil {
		fmt.Fprintf(w, "  running:     unknown   (dial: %v)\n", err)
		return
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	hr, err := c.HealthStatus(ctx)
	if err != nil {
		fmt.Fprintf(w, "  running:     no   (ping: %v)\n", err)
		// For a local unix socket, distinguish "nothing there" from a stale
		// socket file left by a crashed server.
		if sock := c.SocketPath(); sock != "" {
			if _, serr := os.Stat(sock); serr == nil {
				fmt.Fprintf(w, "  socket file: %s   (present — likely stale)\n", sock)
			} else {
				fmt.Fprintf(w, "  socket file: %s   (absent)\n", sock)
			}
		}
		return
	}

	fmt.Fprintf(w, "  running:     yes   (ping ok)\n")
	if hr.PID != 0 {
		fmt.Fprintf(w, "  pid:         %d\n", hr.PID)
	} else {
		fmt.Fprintf(w, "  pid:         (not reported)\n")
	}
	if hr.InstanceID != "" {
		fmt.Fprintf(w, "  instance:    %s\n", hr.InstanceID)
	}
}

// stringRow picks a source for a string field. Flag override wins; then
// raw config; then default; else unset.
func stringRow(key string, flagVal, rawVal, defaultVal string) debugRow {
	switch {
	case flagVal != "":
		return debugRow{key, flagVal, "flag"}
	case rawVal != "":
		return debugRow{key, rawVal, "config"}
	case defaultVal != "":
		return debugRow{key, defaultVal, "default"}
	default:
		return debugRow{key, "", "unset"}
	}
}

func boolRow(key string, rawVal bool) debugRow {
	if rawVal {
		return debugRow{key, "true", "config"}
	}
	return debugRow{key, "false", "default"}
}

func intRow(key string, rawVal int) debugRow {
	if rawVal != 0 {
		return debugRow{key, strconv.Itoa(rawVal), "config"}
	}
	return debugRow{key, "", "unset"}
}

func durationRow(key string, rawVal time.Duration, defaultVal time.Duration) debugRow {
	if rawVal != 0 {
		return debugRow{key, rawVal.String(), "config"}
	}
	if defaultVal != 0 {
		return debugRow{key, defaultVal.String(), "default"}
	}
	return debugRow{key, "0s", "default"}
}

func batchqRows(raw *support.Config, d support.Defaults) []debugRow {
	return []debugRow{
		{
			key:    "runner",
			value:  firstNonEmpty(raw.Batchq.Runner, "simple"),
			source: source(raw.Batchq.Runner != "", "config", "default"),
		},
		stringRow("remote", clientRemote, raw.Batchq.Remote, ""),
		tokenRow(),
		boolRow("multiuser", raw.Batchq.Multiuser),
		durationRow("autospawn_wait_timeout", raw.Batchq.AutospawnWaitTimeout.AsDuration(), d.AutospawnWaitTimeout),
		stringRow("log", clientLogPath, raw.Batchq.Log, ""),
	}
}

// tokenRow picks a source for [batchq] token following the resolution
// order: --token flag > BATCHQ_TOKEN env > [batchq] token in config.
// The value itself is redacted so a casual `batchq debug` over a
// shared screen can't leak the secret.
func tokenRow() debugRow {
	switch {
	case clientToken != "":
		return debugRow{"token", "(set, redacted)", "flag"}
	case envOverrides.Token != "":
		return debugRow{"token", "(set, redacted)", "env BATCHQ_TOKEN"}
	case rawConfig != nil && rawConfig.Batchq.Token != "":
		return debugRow{"token", "(set, redacted)", "config"}
	default:
		return debugRow{"token", "", "unset"}
	}
}

func serverRows(raw *support.Config, d support.Defaults) []debugRow {
	return []debugRow{
		stringRow("listen", "", raw.Server.Listen, d.ServerListen),
		stringRow("db", serverDB, raw.Server.DB, d.Backend),
		durationRow("idle_timeout", raw.Server.IdleTimeout.AsDuration(), 0),
		boolRow("sqlite_wal", raw.Server.SqliteWAL),
		serverTokenRow(raw),
	}
}

// serverTokenRow picks a source for [server] token following the
// resolution order: BATCHQ_SERVER_TOKEN env > [server] token in config.
// The value is redacted so a casual `batchq debug` doesn't print the
// shared secret.
func serverTokenRow(raw *support.Config) debugRow {
	switch {
	case envOverrides.ServerToken != "":
		return debugRow{"token", "(set, redacted)", "env BATCHQ_SERVER_TOKEN"}
	case raw != nil && raw.Server.Token != "":
		return debugRow{"token", "(set, redacted)", "config"}
	default:
		return debugRow{"token", "", "unset"}
	}
}

func webRows(raw *support.Config, d support.Defaults) []debugRow {
	return []debugRow{
		stringRow("socket", "", raw.Web.Socket, d.WebSocket),
		stringRow("listen", "", raw.Web.Listen, ""),
	}
}

func jobDefaultsRows(raw *support.Config, d support.Defaults) []debugRow {
	return []debugRow{
		stringRow("name", "", raw.JobDefaults.Name, ""),
		intRow("procs", raw.JobDefaults.Procs),
		stringRow("mem", "", raw.JobDefaults.Mem, ""),
		stringRow("walltime", "", raw.JobDefaults.Walltime, ""),
		stringRow("wd", "", raw.JobDefaults.Wd, d.JobWd),
		stringRow("stdout", "", raw.JobDefaults.Stdout, d.JobStdout),
		stringRow("stderr", "", raw.JobDefaults.Stderr, d.JobStderr),
		boolRow("hold", raw.JobDefaults.Hold),
		boolRow("env", raw.JobDefaults.Env),
		stringRow("cluster", "", raw.JobDefaults.Cluster, ""),
		stringRow("host", "", raw.JobDefaults.Host, ""),
		resourcesRow(raw.JobDefaults.Resources),
	}
}

func simpleRunnerRows(raw *support.Config, d support.Defaults) []debugRow {
	return []debugRow{
		stringRow("shell", "", raw.SimpleRunner.Shell, d.Shell),
		intRow("max_procs", raw.SimpleRunner.MaxProcs),
		stringRow("max_mem", "", raw.SimpleRunner.MaxMem, ""),
		stringRow("max_walltime", "", raw.SimpleRunner.MaxWalltime, ""),
		boolRow("use_cgroup_v1", raw.SimpleRunner.UseCgroupV1),
		boolRow("use_cgroup_v2", raw.SimpleRunner.UseCgroupV2),
		intRow("max_jobs", raw.SimpleRunner.MaxJobs),
		stringRow("runner_id", "", raw.SimpleRunner.RunnerID, ""),
		stringRow("host", "", raw.SimpleRunner.Host, ""),
		stringRow("cluster", "", raw.SimpleRunner.Cluster, ""),
		resourcesRow(raw.SimpleRunner.Resources),
	}
}

func slurmRunnerRows(raw *support.Config) []debugRow {
	return []debugRow{
		stringRow("user", "", raw.SlurmRunner.User, ""),
		stringRow("account", "", raw.SlurmRunner.Account, ""),
		stringRow("partition", "", raw.SlurmRunner.Partition, ""),
		intRow("max_jobs", raw.SlurmRunner.MaxJobs),
		intRow("max_slurm_jobs", raw.SlurmRunner.MaxSlurmJobs),
		stringRow("runner_id", "", raw.SlurmRunner.RunnerID, ""),
		stringRow("host", "", raw.SlurmRunner.Host, ""),
		stringRow("cluster", "", raw.SlurmRunner.Cluster, ""),
		resourcesRow(raw.SlurmRunner.Resources),
	}
}

// resourcesRow renders an advertised-resources map as a sorted "k=v, k=v" list.
func resourcesRow(res map[string]string) debugRow {
	if len(res) == 0 {
		return debugRow{"resources", "", "unset"}
	}
	names := make([]string, 0, len(res))
	for k := range res {
		names = append(names, k)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, k := range names {
		parts = append(parts, k+"="+res[k])
	}
	return debugRow{"resources", strings.Join(parts, ", "), "config"}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func source(cond bool, ifTrue, ifFalse string) string {
	if cond {
		return ifTrue
	}
	return ifFalse
}

func displayValue(v string) string {
	if strings.TrimSpace(v) == "" {
		return "(unset)"
	}
	return v
}

func envOrUnset(name string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return "(unset)"
}

// envOrRedacted is for secret-bearing env vars: we want to confirm the
// var is set without printing the value.
func envOrRedacted(name string) string {
	if os.Getenv(name) != "" {
		return "(set, redacted)"
	}
	return "(unset)"
}
