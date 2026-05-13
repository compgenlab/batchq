package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/mbreese/batchq/support"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "batchq",
	Short: "batchq - simple batch job queue",
}

// Config holds parsed `~/.batchq/config` (TOML) values. Always non-nil
// after init even if no file is present.
var Config *support.Config

var debugCmd = &cobra.Command{
	Use:    "debug",
	Short:  "Show some debug information",
	Hidden: true,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("batchq home: %s\n", batchqHome)
		fmt.Printf("     config: %s\n", configFile)
		fmt.Printf("    backend: %s\n", Config.Batchq.Backend)
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

	batchqHome = support.GetBatchqHome()
	var err error
	if configFile, err = support.ExpandPathAbs(filepath.Join(batchqHome, "config")); err != nil {
		configFile = ""
	}
	cfg, loadErr := support.LoadConfig(configFile)
	if loadErr != nil {
		fmt.Fprintln(os.Stderr, loadErr)
		cfg = &support.Config{}
	}
	Config = cfg
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
