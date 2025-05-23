package cmd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/mbreese/batchq/db"
	"github.com/mbreese/batchq/iniconfig"
	"github.com/mbreese/batchq/support"

	// "github.com/mbreese/iniconfig"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "batchq",
	Short: "batchq - simple batch job queue",
}

var Config *iniconfig.Config

var initdbCmd = &cobra.Command{
	Use:   "initdb",
	Short: "Initialize the job database",
	Run: func(cmd *cobra.Command, args []string) {
		if err := db.InitDB(dbpath, force); err != nil {
			fmt.Printf("Error initializing DB: %v\n", err)
		}
	},
}

var debugCmd = &cobra.Command{
	Use:    "debug",
	Short:  "Show some debug information",
	Hidden: true,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("batchq home: %s\n", batchqHome)
		fmt.Printf("     config: %s\n", configFile)
		fmt.Printf("     dbpath: %s\n", dbpath)

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
var dbpath string
var force bool

func init() {
	rootCmd.CompletionOptions.DisableDefaultCmd = true

	initdbCmd.Flags().BoolVar(&force, "force", false, "Force overwriting existing DB")

	rootCmd.AddCommand(debugCmd)
	rootCmd.AddCommand(licenseCmd)
	rootCmd.AddCommand(initdbCmd)

	batchqHome = os.Getenv("BATCHQ_HOME")
	if batchqHome == "" {
		batchqHome = "~/.batchq"
	}

	var err error
	if configFile, err = support.ExpandPathAbs(filepath.Join(batchqHome, "config")); err == nil {
		Config = iniconfig.LoadConfig(configFile, "batchq")
		defDB, _ := support.ExpandPathAbs(filepath.Join(batchqHome, "batchq.db"))
		dbpath, _ = Config.Get("", "dbpath", "sqlite3://"+defDB) // ok is always true with a defval
	}
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
