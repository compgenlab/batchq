package cmd

import (
	"log"
	"os/exec"
	"os/user"
	"strings"

	"github.com/mbreese/batchq/jobs"
	"github.com/mbreese/batchq/runner"
	"github.com/spf13/cobra"
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run jobs",
	Run: func(cmd *cobra.Command, args []string) {
		c := mustDialClient()
		defer c.Close()

		var runr runner.Runner
		runnerType := "simple"
		if slurmRunner {
			runnerType = "slurm"
		} else if v := Config.Batchq.Runner; v != "" {
			runnerType = v
		}
		switch runnerType {
		case "slurm":
			if slurmMaxJobs < 0 && Config.SlurmRunner.MaxJobs > 0 {
				slurmMaxJobs = Config.SlurmRunner.MaxJobs
			}
			if slurmAcct == "" {
				slurmAcct = Config.SlurmRunner.Account
			}
			if slurmPartition == "" {
				slurmPartition = Config.SlurmRunner.Partition
			}
			if slurmUser == "" {
				slurmUser = Config.SlurmRunner.User
				if slurmUser == "" {
					if currentUser, err := user.Current(); err == nil {
						slurmUser = currentUser.Username
					} else {
						cmd := exec.Command("id", "-un")
						if out, err := cmd.Output(); err == nil {
							slurmUser = strings.TrimSpace(string(out))
						}
					}
				}
			}

			if slurmUser == "" {
				log.Fatalln("SLURM username must be specified via --slurm-user or in the config file")
			}

			runr = runner.NewSlurmRunner(c).
				SetSlurmMaxUserJobs(slurmMaxJobs).
				SetMaxJobCount(maxJobs).
				SetSlurmUsername(slurmUser).
				SetSlurmAccount(slurmAcct).
				SetSlurmPartition(slurmPartition)

		case "simple":
			if maxProcs < 0 && Config.SimpleRunner.MaxProcs > 0 {
				maxProcs = Config.SimpleRunner.MaxProcs
			}
			if maxMemStr == "" {
				maxMemStr = Config.SimpleRunner.MaxMem
			}
			if maxTimeStr == "" {
				maxTimeStr = Config.SimpleRunner.MaxWalltime
			}
			if !useCgroupV2 && Config.SimpleRunner.UseCgroupV2 {
				useCgroupV2 = true
			}
			if !useCgroupV1 && Config.SimpleRunner.UseCgroupV1 {
				useCgroupV1 = true
			}

			shell := Config.SimpleRunner.Shell

			if useCgroupV1 && useCgroupV2 {
				log.Fatalln("You cannot use cgroup v2 and v1 at the same time!")
			}

			runr = runner.NewSimpleRunner(c).
				SetMaxProcs(maxProcs).
				SetMaxMemMB(jobs.ParseMemoryString(maxMemStr)).
				SetMaxWalltimeSec(jobs.ParseWalltimeString(maxTimeStr)).
				SetForever(forever).
				SetShell(shell).
				SetCgroupV2(useCgroupV2).
				SetCgroupV1(useCgroupV1)
		}
		if runr != nil {
			runr.Start()
		}
	},
}

var maxProcs int
var maxJobs int
var maxMemStr string
var maxTimeStr string
var forever bool
var useCgroupV2 bool
var useCgroupV1 bool

var slurmRunner bool
var slurmUser string
var slurmAcct string
var slurmPartition string
var slurmMaxJobs int

func init() {
	runCmd.Flags().IntVar(&maxProcs, "max-procs", -1, "Maximum processors to use")
	runCmd.Flags().StringVar(&maxMemStr, "max-mem", "", "Max-memory (MB,GB)")
	runCmd.Flags().IntVar(&maxJobs, "max-jobs", -1, "Max number of jobs to run")
	runCmd.Flags().StringVar(&maxTimeStr, "max-walltime", "", "Max-time (D-HH:MM:SS)")
	runCmd.Flags().BoolVar(&forever, "forever", false, "Run forever, waiting for new jobs")
	runCmd.Flags().BoolVar(&useCgroupV2, "use-cgroupv2", false, "Use cgroup v2 to control resources (requires root)")
	runCmd.Flags().BoolVar(&useCgroupV1, "use-cgroupv1", false, "Use cgroup v1 to control resources (requires root)")
	runCmd.Flags().BoolVar(&slurmRunner, "slurm", false, "Use the SLURM runner to proxy jobs to a SLURM scheduler")
	runCmd.Flags().StringVar(&slurmAcct, "slurm-account", "", "Use this SLURM account number")
	runCmd.Flags().StringVar(&slurmPartition, "slurm-partition", "", "Use this SLURM partition")
	runCmd.Flags().IntVar(&slurmMaxJobs, "slurm-max-jobs", -1, "Max jobs allowed for this user account")
	runCmd.Flags().StringVar(&slurmUser, "slurm-user", "", "SLURM user (used for calculating job-count)")

	rootCmd.AddCommand(runCmd)
}
