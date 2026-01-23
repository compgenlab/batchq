package cmd

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/mbreese/batchq/db"
	"github.com/spf13/cobra"
)

var holdCmd = &cobra.Command{
	Use:   "hold job-id...",
	Short: "Hold job",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			cmd.Help()
			return
		}

		if jobq, err := db.OpenDB(dbpath); err != nil {
			log.Fatalln(err)
		} else {
			defer jobq.Close()
			jobIds, err := expandJobArgs(args)
			if err != nil {
				fmt.Printf("Bad job-id: %s\n", err.Error())
				return
			}
			for _, jobid := range jobIds {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				if jobq.HoldJob(ctx, jobid) {
					fmt.Printf("Job: %s held\n", jobid)
				} else {
					fmt.Printf("Error holding job: %s\n", jobid)
				}
			}
		}
	},
}

var releaseCmd = &cobra.Command{
	Use:   "release job-id...",
	Short: "Release user-hold on job",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			cmd.Help()
			return
		}

		if jobq, err := db.OpenDB(dbpath); err != nil {
			log.Fatalln(err)
		} else {
			defer jobq.Close()
			jobIds, err := expandJobArgs(args)
			if err != nil {
				fmt.Printf("Bad job-id: %s\n", err.Error())
				return
			}
			for _, jobid := range jobIds {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				if jobq.ReleaseJob(ctx, jobid) {
					fmt.Printf("Job: %s released\n", jobid)
				} else {
					fmt.Printf("Error releasing job: %s\n", jobid)
				}
			}
		}
	},
}

var cancelCmd = &cobra.Command{
	Use:   "cancel job1_id-job2_id ...",
	Short: "Cancel a job (running or queued)",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			cmd.Help()
			return
		}

		if jobq, err := db.OpenDB(dbpath); err != nil {
			log.Fatalln(err)
		} else {
			defer jobq.Close()
			jobIds, err := expandJobArgs(args)
			if err != nil {
				fmt.Printf("Bad job-id: %s\n", err.Error())
				return
			}
			for _, jobid := range jobIds {
				// this can propagate, so it can take a while...
				ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
				defer cancel()

				if job := jobq.GetJob(ctx, jobid); job != nil {
					if job.GetRunningDetail("slurm_job_id", "") != "" {
						cmd := exec.Command("scancel", job.GetRunningDetail("slurm_job_id", ""))
						if err := cmd.Run(); err != nil {
							fmt.Printf("Error canceling slurm job: %v\n", err)
						} else {
							fmt.Printf("Canceled slurm job: %s\n", job.GetRunningDetail("slurm_job_id", ""))
						}
					}
					if jobq.CancelJob(ctx, jobid, cancelReason) {
						fmt.Printf("Job: %s canceled\n", jobid)
					} else {
						fmt.Printf("Error canceling job: %s\n", jobid)
					}
				}
			}
		}
	},
}

var topCmd = &cobra.Command{
	Use:   "top job-id...",
	Short: "Move job to the top of priority queue",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			cmd.Help()
			return
		}

		if jobq, err := db.OpenDB(dbpath); err != nil {
			log.Fatalln(err)
		} else {
			defer jobq.Close()
			jobIds, err := expandJobArgs(args)
			if err != nil {
				fmt.Printf("Bad job-id: %s\n", err.Error())
				return
			}
			for _, jobid := range jobIds {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				if jobq.TopJob(ctx, jobid) {
					fmt.Printf("Job: %s prioritized\n", jobid)
				} else {
					fmt.Printf("Error prioritizing job: %s\n", jobid)
				}
			}
		}
	},
}

var niceCmd = &cobra.Command{
	Use:   "nice job-id...",
	Short: "Move job lower in priority",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			cmd.Help()
			return
		}

		if jobq, err := db.OpenDB(dbpath); err != nil {
			log.Fatalln(err)
		} else {
			defer jobq.Close()
			jobIds, err := expandJobArgs(args)
			if err != nil {
				fmt.Printf("Bad job-id: %s\n", err.Error())
				return
			}
			for _, jobid := range jobIds {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				if jobq.NiceJob(ctx, jobid) {
					fmt.Printf("Job: %s de-prioritized\n", jobid)
				} else {
					fmt.Printf("Error de-prioritizing job: %s\n", jobid)
				}
			}
		}
	},
}

var cancelReason string

func expandJobArgs(args []string) ([]string, error) {
	var jobIds []string
	for _, arg := range args {
		if start, end, ok := parseNumericRange(arg); ok {
			for jobid := start; jobid <= end; jobid++ {
				jobIds = append(jobIds, strconv.Itoa(jobid))
			}
		} else {
			jobIds = append(jobIds, arg)
		}
	}
	return jobIds, nil
}

func parseNumericRange(arg string) (int, int, bool) {
	if strings.Count(arg, "-") != 1 {
		return 0, 0, false
	}
	spl := strings.Split(arg, "-")
	if len(spl) != 2 || spl[0] == "" || spl[1] == "" {
		return 0, 0, false
	}
	for _, part := range spl {
		for _, r := range part {
			if r < '0' || r > '9' {
				return 0, 0, false
			}
		}
	}
	start, err := strconv.Atoi(spl[0])
	if err != nil {
		return 0, 0, false
	}
	end, err := strconv.Atoi(spl[1])
	if err != nil {
		return 0, 0, false
	}
	return start, end, true
}

func init() {
	cancelCmd.Flags().StringVar(&cancelReason, "reason", "Canceled by user", "Reason for canceling")

	rootCmd.AddCommand(holdCmd)
	rootCmd.AddCommand(releaseCmd)
	rootCmd.AddCommand(cancelCmd)
	rootCmd.AddCommand(topCmd)
	rootCmd.AddCommand(niceCmd)
}
