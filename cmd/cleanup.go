package cmd

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/mbreese/batchq/api"
	"github.com/mbreese/batchq/client"
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

		c := mustDialClient()
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		reader := &clientCleanupReader{c: c}
		plan := buildCleanupPlan(ctx, reader, statuses)
		for _, job := range plan.candidates {
			if !plan.blocked[job.JobID] {
				continue
			}
			dependents, ok := plan.dependents[job.JobID]
			if !ok {
				dependents, _ = reader.GetJobDependents(ctx, job.JobID)
			}
			if len(dependents) > 0 {
				fmt.Printf("Skipping job %s: dependents not eligible for removal\n", job.JobID)
			}
		}

		for _, jobId := range plan.removalOrder {
			if err := c.CleanupJob(ctx, jobId); err == nil {
				fmt.Printf("Removed job: %s\n", jobId)
			} else {
				fmt.Printf("Error removing job: %s\n", jobId)
			}
		}
	},
}

// cleanupJobReader abstracts the data access the cleanup planner needs.
// The CLI wires it to a REST client; tests wire it to an in-memory fake.
type cleanupJobReader interface {
	GetJobsByStatus(ctx context.Context, statuses []jobs.StatusCode, sortByStatus bool) []*api.JobDTO
	GetJob(ctx context.Context, jobId string) *api.JobDTO
	GetJobDependents(ctx context.Context, jobId string) ([]string, error)
}

// clientCleanupReader adapts a *client.Client to cleanupJobReader. It
// converts client errors into the "missing"/empty values the planner
// expects so the planner stays REST-agnostic.
type clientCleanupReader struct {
	c *client.Client
}

func (r *clientCleanupReader) GetJobsByStatus(ctx context.Context, statuses []jobs.StatusCode, sortByStatus bool) []*api.JobDTO {
	names := make([]string, 0, len(statuses))
	for _, s := range statuses {
		names = append(names, s.String())
	}
	dtos, err := r.c.ListJobs(ctx, client.ListJobsOptions{
		Statuses:     names,
		SortByStatus: sortByStatus,
	})
	if err != nil {
		log.Fatalln(err)
	}
	return dtos
}

func (r *clientCleanupReader) GetJob(ctx context.Context, jobId string) *api.JobDTO {
	dto, err := r.c.GetJob(ctx, jobId)
	if err != nil {
		if errors.Is(err, client.ErrNotFound) {
			return nil
		}
		log.Fatalln(err)
	}
	return dto
}

func (r *clientCleanupReader) GetJobDependents(ctx context.Context, jobId string) ([]string, error) {
	return r.c.GetJobDependents(ctx, jobId)
}

type cleanupPlan struct {
	candidates   []*api.JobDTO
	removalOrder []string
	dependents   map[string][]string
	blocked      map[string]bool
}

func buildCleanupPlan(ctx context.Context, jobq cleanupJobReader, statuses []jobs.StatusCode) cleanupPlan {
	// Plan removals by recursively verifying that all dependents are eligible for removal,
	// then emit a post-order deletion list so children are removed before parents.
	candidates := jobq.GetJobsByStatus(ctx, statuses, false)
	allowedStatuses := map[string]bool{}
	for _, status := range statuses {
		allowedStatuses[status.String()] = true
	}

	depCache := map[string][]string{}
	getDependents := func(jobId string) []string {
		if deps, ok := depCache[jobId]; ok {
			return deps
		}
		deps, _ := jobq.GetJobDependents(ctx, jobId)
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
		addRemovals(job.JobID)
	}

	blocked := map[string]bool{}
	for _, job := range candidates {
		if !canRemove(job.JobID) {
			blocked[job.JobID] = true
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
