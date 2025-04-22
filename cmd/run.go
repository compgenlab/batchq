package cmd

import (
	"log"

	"github.com/mbreese/batchq/db"
	"github.com/mbreese/batchq/jobs"
	"github.com/mbreese/batchq/runner"
	"github.com/spf13/cobra"
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run jobs",
	Run: func(cmd *cobra.Command, args []string) {
		if jobq, err := db.OpenDB(dbpath); err != nil {
			log.Fatalln(err)
		} else {
			defer jobq.Close()
			var runr runner.Runner
			if rtype, ok := Config.Get("batchq", "runner", "simple"); ok {
				if rtype == "simple" {

					if maxProcs < 0 {
						if val, ok := Config.GetInt("simple_runner", "max_procs"); ok {
							maxProcs = val
						}
					}
					if maxMemStr == "" {
						if val, ok := Config.Get("simple_runner", "max_mem"); ok {
							maxMemStr = val
						}
					}
					if maxTimeStr == "" {
						if val, ok := Config.Get("simple_runner", "max_walltime"); ok {
							maxTimeStr = val
						}
					}

					shell, _ := Config.Get("simple_runner", "shell", "/bin/bash")

					runr = runner.NewSimpleRunner(jobq).
						SetMaxProcs(maxProcs).
						SetMaxMemMB(jobs.ParseMemoryString(maxMemStr)).
						SetMaxWalltimeSec(jobs.ParseWalltimeString(maxTimeStr)).
						SetForever(forever).
						SetShell(shell)

					runr.Start()
				}
			}
		}
	},
}

var maxProcs int
var maxMemStr string
var maxTimeStr string
var forever bool

func init() {
	runCmd.Flags().IntVar(&maxProcs, "max-procs", -1, "Maximum processors to use")
	runCmd.Flags().StringVar(&maxMemStr, "max-mem", "", "Max-memory (MB,GB)")
	runCmd.Flags().StringVar(&maxTimeStr, "max-walltime", "", "Max-time (D-HH:MM:SS)")
	runCmd.Flags().BoolVar(&forever, "forever", false, "Run forever, waiting for new jobs")

	rootCmd.AddCommand(runCmd)
}
