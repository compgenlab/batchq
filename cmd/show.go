package cmd

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/compgenlab/batchq/api"
	"github.com/compgenlab/batchq/client"
	"github.com/compgenlab/batchq/jobs"
	"github.com/spf13/cobra"
)

var detailsCmd = &cobra.Command{
	Use:   "details jobid",
	Short: "Show details for a job",
	Run: func(cmd *cobra.Command, args []string) {
		c := mustDialClient()
		defer c.Close()

		if statusJSON {
			emitJSON(c, args)
			return
		}

		for _, ids := range args {
			for _, spl := range strings.Split(ids, ",") {
				jobid := strings.TrimSpace(spl)
				if jobid == "" {
					continue
				}
				ctx, cancel := cmdContext()
				target, err := resolveTarget(ctx, c, jobid)
				cancel()
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					continue
				}
				if target.isArray {
					printArraySummary(target.arrayID, target.members, true)
				} else {
					printJobDetails(target.dto)
				}
			}
		}
	},
}

// emitJSON resolves each id argument and prints a single JSON array of the
// per-job DTOs to stdout — one element per resolved job, in argument order.
// An array id contributes all its member task DTOs (in array-index order).
// Unknown ids are omitted (no error, exit 0), matching --porcelain.
func emitJSON(c *client.Client, args []string) {
	out := make([]*api.JobDTO, 0, len(args))
	for _, ids := range args {
		for _, spl := range strings.Split(ids, ",") {
			jobid := strings.TrimSpace(spl)
			if jobid == "" {
				continue
			}
			ctx, cancel := cmdContext()
			target, err := resolveTarget(ctx, c, jobid)
			cancel()
			if err != nil {
				continue // unknown id -> omit (matches --porcelain)
			}
			if target.isArray {
				members := append([]*api.JobDTO{}, target.members...)
				sort.Slice(members, func(i, j int) bool {
					return arrayIndexOf(members[i]) < arrayIndexOf(members[j])
				})
				out = append(out, members...)
			} else {
				out = append(out, target.dto)
			}
		}
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		log.Fatalln(err)
	}
}

// printJobDetails renders the long-form view of a single job (used by
// `details` and by `search` when the query has exactly one hit).
func printJobDetails(dto *api.JobDTO) {
	job := api.JobToDef(dto)
	if job == nil {
		return
	}
	// Status string → StatusCode for the v1 printer.
	if s, perr := api.ParseStatus(dto.Status); perr == nil {
		job.Status = s
	}
	job.Print()
	fmt.Println("")
}

var queueCmd = &cobra.Command{
	Use:   "queue",
	Short: "Show the job queue",
	Run: func(cmd *cobra.Command, args []string) {
		c := mustDialClient()
		defer c.Close()

		ctx, cancel := cmdContext()
		defer cancel()
		var since, before time.Time
		if queueSince != "" {
			var perr error
			if since, perr = resolveTimeBound(queueSince, time.Now()); perr != nil {
				log.Fatalln(perr)
			}
		}
		if queueBefore != "" {
			var perr error
			if before, perr = resolveTimeBound(queueBefore, time.Now()); perr != nil {
				log.Fatalln(perr)
			}
		}

		var dtos []*api.JobDTO
		var err error
		// Detail-based filters (run-id/array-id/output/input) need the
		// general /jobs listing, which loads full relations to match them.
		// A plain queue view — even with --since/--before, now bounded in
		// SQL — uses the fast single-query /queue path.
		if queueRunID != "" || queueArrayID != "" || queueOutput != "" || queueInput != "" {
			dtos, err = c.ListJobs(ctx, client.ListJobsOptions{
				ShowAll:      jobShowAll,
				SortByStatus: !queueSortTime,
				RunID:        queueRunID,
				ArrayID:      queueArrayID,
				Output:       queueOutput,
				Input:        queueInput,
				Since:        since,
				Before:       before,
			})
		} else {
			dtos, err = c.GetQueueJobs(ctx, jobShowAll, !queueSortTime, since, before)
		}
		if err != nil {
			log.Fatalln(err)
		}
		if queueSortTime {
			sort.SliceStable(dtos, func(i, j int) bool {
				ti := timeOrZero(dtos[i].SubmitTime)
				tj := timeOrZero(dtos[j].SubmitTime)
				if queueSortReverse {
					return ti.Before(tj)
				}
				return ti.After(tj)
			})
		}
		printQueueTable(dtos)
	},
}

// printQueueTable renders the standard tabular queue view for a slice
// of jobs. Shared by `queue` and `search` (when a query has multiple
// hits).
func printQueueTable(dtos []*api.JobDTO) {
	fmt.Printf("| %-36.36s ", "jobid")
	fmt.Printf("| %-8.8s ", "status")
	fmt.Printf("| %-20.20s ", "job-name")
	if Config.Batchq.Multiuser {
		fmt.Printf("| %-12.12s ", "username")
	}
	fmt.Printf("|%-5.5s", "procs")
	fmt.Printf("| %-8.8s ", "mem")
	fmt.Printf("| %-11.11s ", "walltime")
	fmt.Printf("| %-6.6s ", "submit")
	fmt.Println("|")
	if Config.Batchq.Multiuser {
		fmt.Println("|--------------------------------------|----------|----------------------|--------------|-----|----------|-------------|--------|")
	} else {
		fmt.Println("|--------------------------------------|----------|----------------------|-----|----------|-------------|--------|")
	}

	for _, dto := range dtos {
		job := api.JobToDef(dto)
		if s, perr := api.ParseStatus(dto.Status); perr == nil {
			job.Status = s
		}
		fmt.Printf("| %-36.36s ", job.JobId)
		fmt.Printf("| %-8.8s ", job.Status.String())
		fmt.Printf("| %-20.20s ", job.Name)
		if Config.Batchq.Multiuser {
			fmt.Printf("| %-12.12s ", job.GetDetail("user", ""))
		}
		fmt.Printf("| %-3.3s ", job.GetDetail("procs", ""))
		fmt.Printf("| %-8.8s ", jobs.PrintMemoryString(job.GetDetail("mem", "")))

		var walltimeStr string
		switch job.Status {
		case jobs.CANCELED:
			walltimeStr = ""
		case jobs.SUCCESS, jobs.FAILED:
			walltimeStr = jobs.WalltimeToString(int(job.EndTime.Sub(job.StartTime).Seconds()))
		case jobs.RUNNING:
			walltimeStr = jobs.WalltimeToString(int(time.Now().UTC().Sub(job.StartTime).Seconds()))
		default:
			walltimeStr = jobs.WalltimeStringToString(job.GetDetail("walltime", ""))
		}
		fmt.Printf("| %-11.11s ", walltimeStr)
		fmt.Printf("| %-6.6s ", relativeAge(dto.SubmitTime))

		switch job.Status {
		case jobs.CANCELED:
			fmt.Printf("| %-20.20s\n", job.Notes)
		case jobs.SUCCESS:
			fmt.Println("|")
		case jobs.FAILED:
			fmt.Printf("| %-20.20s\n", fmt.Sprintf("exit:%d", job.ReturnCode))
		case jobs.RUNNING:
			fmt.Printf("| %-20.20s\n", fmt.Sprintf("pid:%s", job.GetRunningDetail("pid", "")))
		case jobs.PROXYQUEUED:
			fmt.Print("|")
			if job.GetRunningDetail("slurm_job_id", "") != "" {
				fmt.Printf(" %s", fmt.Sprintf("slurm:%s %s;", job.GetRunningDetail("slurm_status", ""), job.GetRunningDetail("slurm_job_id", "")))
			}
			if len(job.AfterOk) > 0 {
				depStr := fmt.Sprintf("deps:%s", strings.Join(job.AfterOk, ","))
				if len(depStr) > 20 {
					fmt.Printf(" %-17.17s...", depStr)
				} else {
					fmt.Printf(" %-20s", depStr)
				}
			}
			fmt.Println("")
		default:
			fmt.Print("|")
			if len(job.AfterOk) > 0 {
				depStr := fmt.Sprintf("deps:%s", strings.Join(job.AfterOk, ","))
				if len(depStr) > 20 {
					fmt.Printf(" %-17.17s...", depStr)
				} else {
					fmt.Printf(" %-20s", depStr)
				}
			}
			fmt.Println("")
		}
	}
}

// resolveTimeBound turns a user-supplied --since/--before value into an
// absolute time. It accepts an age-ago duration (1d/12h/1w/30m/90s, resolved
// as now minus that duration), a local calendar date (2006-01-02, midnight
// local), a local datetime (2006-01-02 15:04:05), or an RFC3339 timestamp.
func resolveTimeBound(s string, now time.Time) (time.Time, error) {
	s = strings.TrimSpace(s)
	if d, err := parseAgeDuration(s); err == nil {
		return now.Add(-d), nil
	}
	for _, layout := range []string{"2006-01-02", "2006-01-02 15:04:05", time.RFC3339} {
		if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse time %q (want a date, RFC3339, or age like 1d/12h)", s)
}

func timeOrZero(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

func relativeAge(t *time.Time) string {
	if t == nil || t.IsZero() {
		return ""
	}
	d := time.Since(*t)
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	case d < 7*24*time.Hour:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	default:
		return fmt.Sprintf("%dw", int(d.Hours()/(24*7)))
	}
}

func statusTimeSuffix(dto *api.JobDTO) string {
	var b strings.Builder
	fmtTime := func(t *time.Time) string {
		tv := timeOrZero(t)
		if tv.IsZero() {
			return ""
		}
		return tv.UTC().Format(time.RFC3339)
	}
	if statusShowSubmit {
		fmt.Fprintf(&b, " SubmitTime:%s", fmtTime(dto.SubmitTime))
	}
	if statusShowStart {
		fmt.Fprintf(&b, " StartTime:%s", fmtTime(dto.StartTime))
	}
	if statusShowEnd {
		fmt.Fprintf(&b, " EndTime:%s", fmtTime(dto.EndTime))
	}
	if statusShowWall {
		b.WriteString(" WallTime:" + statusWallString(dto))
	}
	return b.String()
}

func statusWallString(dto *api.JobDTO) string {
	start := timeOrZero(dto.StartTime)
	if start.IsZero() {
		return ""
	}
	end := timeOrZero(dto.EndTime)
	if end.IsZero() {
		if dto.Status == jobs.RUNNING.String() {
			return jobs.WalltimeToString(int(time.Now().UTC().Sub(start).Seconds()))
		}
		return ""
	}
	return jobs.WalltimeToString(int(end.Sub(start).Seconds()))
}

var statusCmd = &cobra.Command{
	Use:   "status {job1 job2...}",
	Short: "Status for a job",
	Run: func(cmd *cobra.Command, args []string) {
		c := mustDialClient()
		defer c.Close()

		if statusJSON {
			emitJSON(c, args)
			return
		}

		if len(args) == 0 {
			ctx, cancel := cmdContext()
			defer cancel()
			dtos, err := c.ListJobs(ctx, client.ListJobsOptions{ShowAll: jobShowAll})
			if err != nil {
				log.Fatalln(err)
			}
			for _, dto := range dtos {
				if statusPorcelain {
					fmt.Printf("%s\t%s\n", dto.JobID, dto.Status)
				} else {
					fmt.Printf("%s %s\n", dto.JobID, dto.Status)
				}
			}
			return
		}
		for _, ids := range args {
			for _, spl := range strings.Split(ids, ",") {
				jobid := strings.TrimSpace(spl)
				if jobid == "" {
					continue
				}
				ctx, cancel := cmdContext()
				target, err := resolveTarget(ctx, c, jobid)
				cancel()
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					continue
				}
				// Porcelain: one stable, machine-readable line per queried id,
				// "<queried-id>\t<STATUS>". The queried token is echoed verbatim in
				// field 0 (so a task address or array id is unambiguous), and an
				// array collapses to a single aggregate status.
				if statusPorcelain {
					if target.isArray {
						fmt.Printf("%s\t%s\n", jobid, aggregateArrayStatus(target.members))
					} else {
						fmt.Printf("%s\t%s\n", jobid, target.dto.Status)
					}
					continue
				}
				if target.isArray {
					printArraySummary(target.arrayID, target.members, false)
				} else {
					fmt.Printf("%s %s%s\n", target.dto.JobID, target.dto.Status, statusTimeSuffix(target.dto))
				}
			}
		}
	},
}

// aggregateArrayStatus rolls an array's per-task statuses into one word. It
// reports the most-active non-terminal state if any task is still live (so an
// external "is this array still active?" check sees activity), otherwise the
// terminal state: FAILED/CANCELED if any task failed, else SUCCESS.
func aggregateArrayStatus(members []*api.JobDTO) string {
	counts := map[string]int{}
	for _, m := range members {
		counts[m.Status]++
	}
	for _, st := range []jobs.StatusCode{jobs.RUNNING, jobs.PROXYQUEUED, jobs.QUEUED, jobs.WAITING, jobs.USERHOLD} {
		if counts[st.String()] > 0 {
			return st.String()
		}
	}
	if counts[jobs.FAILED.String()] > 0 {
		return jobs.FAILED.String()
	}
	if counts[jobs.CANCELED.String()] > 0 {
		return jobs.CANCELED.String()
	}
	return jobs.SUCCESS.String()
}

var summaryCmd = &cobra.Command{
	Use:   "summary",
	Short: "Summary of the job queue",
	Run: func(cmd *cobra.Command, args []string) {
		c := mustDialClient()
		defer c.Close()

		if len(args) == 0 {
			ctx, cancel := cmdContext()
			defer cancel()
			counts, err := c.GetJobStatusCounts(ctx, jobShowAll)
			if err != nil {
				log.Fatalln(err)
			}

			for _, status := range []jobs.StatusCode{jobs.USERHOLD, jobs.WAITING, jobs.QUEUED, jobs.PROXYQUEUED, jobs.RUNNING, jobs.SUCCESS, jobs.FAILED, jobs.CANCELED} {
				name := status.String()
				if jobShowAll || counts[name] > 0 {
					fmt.Printf("%-12s: %d\n", name, counts[name])
				}
			}
		}
	},
}

var jobShowAll bool
var statusShowSubmit bool
var statusShowStart bool
var statusShowEnd bool
var statusShowWall bool
var statusPorcelain bool
var statusJSON bool
var queueRunID string
var queueArrayID string
var queueOutput string
var queueInput string
var queueSince string
var queueBefore string
var queueSortTime bool
var queueSortReverse bool

func init() {
	queueCmd.Flags().BoolVar(&jobShowAll, "all", false, "Show all jobs (including completed)")
	queueCmd.Flags().StringVar(&queueRunID, "run-id", "", "Only show jobs in this workflow run")
	queueCmd.Flags().StringVar(&queueArrayID, "array-id", "", "Only show tasks in this job array")
	queueCmd.Flags().StringVar(&queueOutput, "output", "", "Only show jobs that list this file as an output")
	queueCmd.Flags().StringVar(&queueInput, "input", "", "Only show jobs that list this file as an input")
	queueCmd.Flags().StringVar(&queueSince, "since", "", "Only show jobs submitted at/after this time (date, RFC3339, or age like 1d/12h)")
	queueCmd.Flags().StringVar(&queueBefore, "before", "", "Only show jobs submitted before this time (date, RFC3339, or age like 1d/12h)")
	queueCmd.Flags().BoolVarP(&queueSortTime, "time", "t", false, "Sort by submit time (newest first)")
	queueCmd.Flags().BoolVarP(&queueSortReverse, "reverse", "r", false, "Reverse sort order (use with -t)")
	summaryCmd.Flags().BoolVar(&jobShowAll, "all", false, "Show all jobs (including completed)")
	statusCmd.Flags().BoolVarP(&statusShowSubmit, "submit", "s", false, "Show submit time")
	statusCmd.Flags().BoolVarP(&statusShowStart, "begin", "b", false, "Show start/begin time")
	statusCmd.Flags().BoolVarP(&statusShowEnd, "end", "e", false, "Show end time")
	statusCmd.Flags().BoolVarP(&statusShowWall, "walltime", "t", false, "Show wall time (end-start)")
	statusCmd.Flags().BoolVar(&statusPorcelain, "porcelain", false, "Machine-readable: one '<id>\\t<status>' line per queried id (arrays collapse to one status)")
	statusCmd.Flags().BoolVar(&statusJSON, "json", false, "Machine-readable: a JSON array of job objects")
	detailsCmd.Flags().BoolVar(&statusJSON, "json", false, "Machine-readable: a JSON array of job objects")

	rootCmd.AddCommand(detailsCmd)
	rootCmd.AddCommand(queueCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(summaryCmd)
}
