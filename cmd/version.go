package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// Version is the batchq version string. Set at build time via:
//
//	-ldflags "-X github.com/mbreese/batchq/cmd.Version=v0.2.0-dev-abc1234"
//
// When unset (plain `go run`, `go build` without -ldflags, IDE
// builds), defaults to "dev". The Makefile computes the real value
// from `git describe`: a commit that's on an exact tag becomes that
// tag's name; any other commit becomes "v0.2.0-dev-<short-sha>".
var Version = "dev"

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the batchq version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("batchq " + Version)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
	rootCmd.Version = Version
	rootCmd.SetVersionTemplate("batchq {{.Version}}\n")

	// Append a version footer to every command's help output.
	// SetHelpTemplate on rootCmd is inherited by subcommands.
	rootCmd.SetHelpTemplate(`{{with (or .Long .Short)}}{{. | trimTrailingWhitespaces}}

{{end}}{{if or .Runnable .HasSubCommands}}{{.UsageString}}{{end}}
batchq {{.Root.Version}}
`)
}
