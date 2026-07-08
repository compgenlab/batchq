package cmd

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/compgenlab/batchq/client"
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

		c := mustDialClient()
		defer c.Close()
		forEachTarget(c, args, func(ctx context.Context, t *jobTarget) {
			if t.isArray {
				if n, err := c.HoldArray(ctx, t.arrayID); err == nil {
					fmt.Printf("Array: %s — held %d task(s)\n", t.arrayID, n)
				} else {
					fmt.Printf("Error holding array %s: %v\n", t.arrayID, err)
				}
				return
			}
			if err := c.HoldJob(ctx, t.jobID); err == nil {
				fmt.Printf("Job: %s held\n", t.jobID)
			} else {
				fmt.Printf("Error holding job: %s\n", t.jobID)
			}
		})
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

		c := mustDialClient()
		defer c.Close()
		forEachTarget(c, args, func(ctx context.Context, t *jobTarget) {
			if t.isArray {
				if n, err := c.ReleaseArray(ctx, t.arrayID); err == nil {
					fmt.Printf("Array: %s — released %d task(s)\n", t.arrayID, n)
				} else {
					fmt.Printf("Error releasing array %s: %v\n", t.arrayID, err)
				}
				return
			}
			if err := c.ReleaseJob(ctx, t.jobID); err == nil {
				fmt.Printf("Job: %s released\n", t.jobID)
			} else {
				fmt.Printf("Error releasing job: %s\n", t.jobID)
			}
		})
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

		c := mustDialClient()
		defer c.Close()
		forEachTarget(c, args, func(ctx context.Context, t *jobTarget) {
			if t.isArray {
				scancelArray(t.members)
				if n, err := c.CancelArray(ctx, t.arrayID, cancelReason); err == nil {
					fmt.Printf("Array: %s — canceled %d task(s)\n", t.arrayID, n)
				} else {
					fmt.Printf("Error canceling array %s: %v\n", t.arrayID, err)
				}
				return
			}
			scancelJob(t.dto)
			if err := c.CancelJob(ctx, t.jobID, cancelReason); err == nil {
				fmt.Printf("Job: %s canceled\n", t.jobID)
			} else {
				fmt.Printf("Error canceling job: %s\n", t.jobID)
			}
		})
	},
}

var topCmd = &cobra.Command{
	Use:   "top job-id...",
	Short: "Move job (or whole array) to the top of priority queue",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			cmd.Help()
			return
		}

		c := mustDialClient()
		defer c.Close()
		adjustPriority(c, args, 1, "prioritized")
	},
}

var niceCmd = &cobra.Command{
	Use:   "nice job-id...",
	Short: "Move job (or whole array) lower in priority",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			cmd.Help()
			return
		}

		c := mustDialClient()
		defer c.Close()
		adjustPriority(c, args, -1, "de-prioritized")
	},
}

// adjustPriority applies a priority delta to each target, dispatching whole
// arrays to the atomic array endpoint and single jobs/tasks to the per-job one.
// verb is the past-tense word used in the success message ("prioritized").
func adjustPriority(c *client.Client, args []string, delta int, verb string) {
	forEachTarget(c, args, func(ctx context.Context, t *jobTarget) {
		if t.isArray {
			if n, err := c.AdjustArrayPriority(ctx, t.arrayID, delta); err == nil {
				fmt.Printf("Array: %s — %s %d task(s)\n", t.arrayID, verb, n)
			} else {
				fmt.Printf("Error %s array %s: %v\n", verb, t.arrayID, err)
			}
			return
		}
		if err := c.AdjustJobPriority(ctx, t.jobID, delta); err == nil {
			fmt.Printf("Job: %s %s\n", t.jobID, verb)
		} else {
			fmt.Printf("Error %s job: %s\n", verb, t.jobID)
		}
	})
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
