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

			plan := buildCleanupPlan(ctx, jobq, statuses)
			for _, job := range plan.candidates {
				if !plan.blocked[job.JobId] {
					continue
				}
				dependents, ok := plan.dependents[job.JobId]
				if !ok {
					dependents = jobq.GetJobDependents(ctx, job.JobId)
				}
				if len(dependents) > 0 {
					fmt.Printf("Skipping job %s: dependents not eligible for removal\n", job.JobId)
				}
			}

			for _, jobId := range plan.removalOrder {
				if jobq.CleanupJob(ctx, jobId) {
					fmt.Printf("Removed job: %s\n", jobId)
				} else {
					fmt.Printf("Error removing job: %s\n", jobId)
				}
			}
		}
	},
}

type cleanupJobReader interface {
	GetJobsByStatus(ctx context.Context, statuses []jobs.StatusCode, sortByStatus bool) []*jobs.JobDef
	GetJob(ctx context.Context, jobId string) *jobs.JobDef
	GetJobDependents(ctx context.Context, jobId string) []string
}

type cleanupPlan struct {
	candidates   []*jobs.JobDef
	removalOrder []string
	dependents   map[string][]string
	blocked      map[string]bool
}

func buildCleanupPlan(ctx context.Context, jobq cleanupJobReader, statuses []jobs.StatusCode) cleanupPlan {
	// Plan removals by recursively verifying that all dependents are eligible for removal,
	// then emit a post-order deletion list so children are removed before parents.
	candidates := jobq.GetJobsByStatus(ctx, statuses, false)
	allowedStatuses := map[jobs.StatusCode]bool{}
	for _, status := range statuses {
		allowedStatuses[status] = true
	}

	depCache := map[string][]string{}
	getDependents := func(jobId string) []string {
		if deps, ok := depCache[jobId]; ok {
			return deps
		}
		deps := jobq.GetJobDependents(ctx, jobId)
		depCache[jobId] = deps
		return deps
	}

	const (
		unknownState = iota
		visitingState
		removableState
		blockedState
	)
	state := map[string]int{}

	var canRemove func(jobId string) bool
	canRemove = func(jobId string) bool {
		if st, ok := state[jobId]; ok {
			if st == visitingState {
				state[jobId] = blockedState
				return false
			}
			return st == removableState
		}
		state[jobId] = visitingState
		job := jobq.GetJob(ctx, jobId)
		if job == nil || !allowedStatuses[job.Status] {
			state[jobId] = blockedState
			return false
		}
		for _, childId := range getDependents(jobId) {
			if !canRemove(childId) {
				state[jobId] = blockedState
				return false
			}
		}
		state[jobId] = removableState
		return true
	}

	removalOrder := []string{}
	added := map[string]bool{}
	var addRemovals func(jobId string)
	addRemovals = func(jobId string) {
		if added[jobId] || !canRemove(jobId) {
			return
		}
		for _, childId := range getDependents(jobId) {
			addRemovals(childId)
		}
		if !added[jobId] {
			removalOrder = append(removalOrder, jobId)
			added[jobId] = true
		}
	}

	for _, job := range candidates {
		addRemovals(job.JobId)
	}

	blocked := map[string]bool{}
	for _, job := range candidates {
		if !canRemove(job.JobId) {
			blocked[job.JobId] = true
		}
	}

	return cleanupPlan{
		candidates:   candidates,
		removalOrder: removalOrder,
		dependents:   depCache,
		blocked:      blocked,
	}
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
