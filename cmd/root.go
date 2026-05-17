package cmd

import (
	"fmt"
	"os"

	"github.com/mbreese/batchq/support"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "batchq",
	Short: "batchq - simple batch job queue",
}

// Config holds the resolved configuration: TOML values from
// ~/.batchq/config (or $BATCHQ_HOME/config) with built-in defaults
// layered onto any empty fields. Call sites can read fields directly —
// e.g. Config.Server.Listen — without re-implementing the fallback
// chain. Always non-nil after init even if no file is present.
var Config *support.Config

// rawConfig holds the TOML-loaded values *before* env vars or defaults
// were applied. Only the debug command needs this — it compares raw vs
// final to label each value's source (config / env / default).
var rawConfig *support.Config

// envOverrides snapshots the env vars Config consumes (BATCHQ_TOKEN).
// The debug command consults this to render `(env)` for any knob that
// came from an env var rather than the config file.
var envOverrides support.EnvOverrides

// defaultsResolved is the Defaults snapshot used during init. The debug
// command reuses it to label fallback sources.
var defaultsResolved support.Defaults

var debugCmd = &cobra.Command{
	Use:    "debug",
	Short:  "Show resolved configuration (sources: flag, env, config, default)",
	Hidden: true,
	Run: func(cmd *cobra.Command, args []string) {
		printDebugConfig(os.Stdout)
	},
}

var licenseCmd = &cobra.Command{
	Use:    "license",
	Short:  "Show the license",
	Hidden: true,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(licenseText)
	},
}

var licenseText string

func SetLicenseText(txt string) {
	licenseText = txt
}

var batchqHome string
var configFile string

func init() {
	rootCmd.CompletionOptions.DisableDefaultCmd = true

	rootCmd.AddCommand(debugCmd)
	rootCmd.AddCommand(licenseCmd)

	defaultsResolved = support.NewDefaults()
	batchqHome = defaultsResolved.Home
	if expanded, err := support.ExpandPathAbs(defaultsResolved.ConfigFile); err == nil {
		configFile = expanded
	} else {
		configFile = defaultsResolved.ConfigFile
	}
	cfg, loadErr := support.LoadConfig(configFile)
	if loadErr != nil {
		fmt.Fprintln(os.Stderr, loadErr)
		cfg = &support.Config{}
	}
	rawConfig = cfg.Clone()
	envOverrides = support.ReadEnvOverrides()
	cfg.ApplyEnv(envOverrides)
	cfg.ApplyDefaults(defaultsResolved)
	Config = cfg
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
