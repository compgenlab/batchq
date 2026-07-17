package cmd

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/compgenlab/batchq/api"
	"github.com/compgenlab/batchq/client"
	"github.com/compgenlab/batchq/jobs"
	"github.com/spf13/cobra"
)

// archiveAutoName is the sentinel value the --archive flag takes when given
// with no explicit name (NoOptDefVal), meaning "let the server pick a
// timestamped archive filename".
const archiveAutoName = "\x00auto"

var cleanupCmd = &cobra.Command{
	Use:   "cleanup",
	Short: "Remove or archive completed jobs, and/or vacuum the database",
	Long: `Remove or archive terminal jobs (canceled/failed/successful) from the
database. You must choose an action explicitly:

  --delete     permanently remove the selected jobs
  --archive    move the selected jobs into a new read-only archive DB, then
               remove them from the live DB. Use --archive=NAME to name the
               archive file (default: a timestamped name). Each run writes one
               new, immutable archive under [server] archive_dir.

Select which jobs with --canceled / --failed / --success / --all, optionally
narrowed by --older-than.

After a --delete or --archive the database is VACUUMed automatically to reclaim
the freed space; pass --no-vacuum to skip that. Run 'cleanup --vacuum' on its
own to just compact the live DB without removing anything.`,
	Args: cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		if cleanupAll {
			cleanupCanceled = true
			cleanupFailed = true
			cleanupSuccess = true
		}
		purgeRequested := cleanupCanceled || cleanupFailed || cleanupSuccess
		archiveSet := cmd.Flags().Changed("archive")

		// Validate the action selection.
		if cleanupVacuum && cleanupNoVacuum {
			log.Fatal("--vacuum and --no-vacuum are mutually exclusive")
		}
		if cleanupDelete && archiveSet {
			log.Fatal("choose exactly one of --delete or --archive, not both")
		}
		if purgeRequested && !cleanupDelete && !archiveSet {
			log.Fatal("select an action for the chosen jobs: --delete or --archive")
		}
		if !purgeRequested && (cleanupDelete || archiveSet) {
			log.Fatal("no jobs selected: use --canceled / --failed / --success / --all (optionally --older-than)")
		}
		if !purgeRequested && !cleanupVacuum {
			// Nothing to do — no jobs selected and no standalone vacuum.
			cmd.Help()
			return
		}

		// Vacuum runs automatically after a purge (unless --no-vacuum); with no
		// purge it runs only when --vacuum was given explicitly.
		doVacuum := cleanupVacuum
		if purgeRequested {
			doVacuum = !cleanupNoVacuum
		}

		var minAge time.Duration
		if cleanupOlderThan != "" {
			d, err := parseAgeDuration(cleanupOlderThan)
			if err != nil {
				log.Fatalf("invalid --older-than %q: %v", cleanupOlderThan, err)
			}
			minAge = d
		}

		// Long-running client: a large archive batch plus a VACUUM can far
		// exceed the 30s per-request default; the server decouples cancellation
		// so it finishes regardless — wait rather than hard-fail at 30s.
		c := mustDialClientLongRunning()
		defer c.Close()

		ctx, cancel := cmdContextLong()
		defer cancel()

		if purgeRequested {
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

			reader := &clientCleanupReader{c: c}
			plan := buildCleanupPlan(ctx, reader, statuses)
			if minAge > 0 {
				plan = filterPlanByAge(plan, minAge, time.Now())
			}

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

			if archiveSet {
				name := cleanupArchive
				if name == archiveAutoName {
					name = "" // let the server pick a timestamped name
				}
				if len(plan.removalOrder) == 0 {
					fmt.Println("No jobs to archive")
				} else {
					path, count, err := c.ArchiveJobs(ctx, plan.removalOrder, name)
					if err != nil {
						fmt.Fprintf(os.Stderr, "batchq cleanup: archive failed: %v\n", err)
						os.Exit(1)
					}
					fmt.Printf("Archived %d job(s) to %s\n", count, path)
				}
			} else { // --delete
				for _, jobId := range plan.removalOrder {
					if err := c.CleanupJob(ctx, jobId); err == nil {
						fmt.Printf("Removed job: %s\n", jobId)
					} else {
						fmt.Printf("Error removing job: %s\n", jobId)
					}
				}
			}
		}

		if doVacuum {
			fmt.Println("Vacuuming database...")
			if err := c.Vacuum(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "batchq cleanup: vacuum failed: %v\n", err)
				os.Exit(1)
			}
			fmt.Println("Database vacuumed")
		}
	},
}

// filterPlanByAge drops any job from the removal order whose end_time is
// unset or younger than minAge. Jobs are kept in plan.candidates (so the
// "blocked" reporting above still works), but the removalOrder set is the
// authoritative list of what actually gets deleted.
func filterPlanByAge(plan cleanupPlan, minAge time.Duration, now time.Time) cleanupPlan {
	eligible := make(map[string]bool, len(plan.removalOrder))
	byID := make(map[string]*api.JobDTO, len(plan.candidates))
	for _, c := range plan.candidates {
		byID[c.JobID] = c
	}
	for _, id := range plan.removalOrder {
		c, ok := byID[id]
		if !ok {
			continue
		}
		if c.EndTime == nil || c.EndTime.IsZero() {
			continue
		}
		if now.Sub(*c.EndTime) <= minAge {
			continue
		}
		eligible[id] = true
	}
	filteredOrder := make([]string, 0, len(eligible))
	for _, id := range plan.removalOrder {
		if eligible[id] {
			filteredOrder = append(filteredOrder, id)
		}
	}
	plan.removalOrder = filteredOrder
	return plan
}

// parseAgeDuration parses a duration string accepting `s`, `m`, `h`, `d`,
// or `w` suffixes (e.g. "30d", "12h", "1w", "90m"). Bare numbers are not
// accepted — the unit is required to avoid ambiguity.
func parseAgeDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}
	last := s[len(s)-1]
	switch last {
	case 'd', 'w':
		n, err := strconv.Atoi(s[:len(s)-1])
		if err != nil || n < 0 {
			return 0, fmt.Errorf("bad number before %q", string(last))
		}
		mult := 24 * time.Hour
		if last == 'w' {
			mult = 7 * 24 * time.Hour
		}
		return time.Duration(n) * mult, nil
	case 's', 'm', 'h':
		// Go's ParseDuration handles these natively.
		return time.ParseDuration(s)
	default:
		return 0, fmt.Errorf("unrecognized unit %q (expected s, m, h, d, or w)", string(last))
	}
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
var cleanupOlderThan string
var cleanupDelete bool
var cleanupArchive string
var cleanupVacuum bool
var cleanupNoVacuum bool

func init() {
	cleanupCmd.Flags().BoolVar(&cleanupCanceled, "canceled", false, "Select canceled jobs")
	cleanupCmd.Flags().BoolVar(&cleanupFailed, "failed", false, "Select failed jobs")
	cleanupCmd.Flags().BoolVar(&cleanupSuccess, "success", false, "Select successful jobs")
	cleanupCmd.Flags().BoolVar(&cleanupAll, "all", false, "Select canceled, failed, and successful jobs")
	cleanupCmd.Flags().StringVar(&cleanupOlderThan, "older-than", "", "Only select jobs whose end_time is older than this duration (e.g. 30d, 12h, 1w)")

	cleanupCmd.Flags().BoolVar(&cleanupDelete, "delete", false, "Permanently remove the selected jobs")
	cleanupCmd.Flags().StringVar(&cleanupArchive, "archive", "", "Archive the selected jobs into a new read-only archive DB, then remove them (use --archive=NAME to name the file)")
	// Allow bare --archive (auto-named) as well as --archive=NAME.
	cleanupCmd.Flags().Lookup("archive").NoOptDefVal = archiveAutoName
	cleanupCmd.Flags().BoolVar(&cleanupVacuum, "vacuum", false, "VACUUM the live DB (runs automatically after a purge; use alone to just compact)")
	cleanupCmd.Flags().BoolVar(&cleanupNoVacuum, "no-vacuum", false, "Skip the automatic VACUUM after a --delete/--archive")

	rootCmd.AddCommand(cleanupCmd)
}
