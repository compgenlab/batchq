package cmd

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/mbreese/batchq/db"
	"github.com/mbreese/batchq/jobs"
	"github.com/spf13/cobra"
)

var cleanupCmd = &cobra.Command{
	Use:   "cleanup",
	Short: "Remove completed jobs from the database",
	Run: func(cmd *cobra.Command, args []string) {
		if !cleanupCanceled && !cleanupFailed && !cleanupSuccess && !cleanupAll {
			cmd.Help()
			return
		}

		if cleanupAll {
			cleanupCanceled = true
			cleanupFailed = true
			cleanupSuccess = true
		}

		statuses := make([]jobs.StatusCode, 0, 3)
		if cleanupCanceled {
			statuses = append(statuses, jobs.CANCELED)
		}
		if cleanupFailed {
			statuses = append(statuses, jobs.FAILED)
		}
		if cleanupSuccess {
			statuses = append(statuses, jobs.SUCCESS)
		}

		if jobq, err := db.OpenDB(dbpath); err != nil {
			log.Fatalln(err)
		} else {
			defer jobq.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			for _, job := range jobq.GetJobsByStatus(ctx, statuses, false) {
				if job.Status == jobs.SUCCESS {
					if dependents := jobq.GetJobDependents(ctx, job.JobId); len(dependents) > 0 {
						fmt.Printf("Skipping job %s: has dependents\n", job.JobId)
						continue
					}
				}
				if jobq.CleanupJob(ctx, job.JobId) {
					fmt.Printf("Removed job: %s\n", job.JobId)
				} else {
					fmt.Printf("Error removing job: %s\n", job.JobId)
				}
			}
		}
	},
}

var cleanupCanceled bool
var cleanupFailed bool
var cleanupSuccess bool
var cleanupAll bool

func init() {
	cleanupCmd.Flags().BoolVar(&cleanupCanceled, "canceled", false, "Remove canceled jobs")
	cleanupCmd.Flags().BoolVar(&cleanupFailed, "failed", false, "Remove failed jobs")
	cleanupCmd.Flags().BoolVar(&cleanupSuccess, "success", false, "Remove successful jobs")
	cleanupCmd.Flags().BoolVar(&cleanupAll, "all", false, "Remove canceled, failed, and successful jobs")
	rootCmd.AddCommand(cleanupCmd)
}
