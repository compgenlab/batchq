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
				target, err := resolveTarget(ctx, c, jobid, showIncludeArchives)
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
			target, err := resolveTarget(ctx, c, jobid, showIncludeArchives)
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

		// Normalize + validate --state (case-insensitive, comma-separated).
		states, perr := normalizeStates(queueStates)
		if perr != nil {
			log.Fatalln(perr)
		}

		var dtos []*api.JobDTO
		var err error
		// A --state filter or a detail filter (run-id/array-id/output/input) uses
		// the general /jobs listing (it can filter by status / load relations). A
		// plain queue view — even with --since/--before, bounded in SQL — uses the
		// fast single-query /queue path.
		if len(states) > 0 || queueRunID != "" || queueArrayID != "" || queueOutput != "" || queueInput != "" {
			dtos, err = c.ListJobs(ctx, client.ListJobsOptions{
				ShowAll:      jobShowAll,
				SortByStatus: !queueSortTime,
				Statuses:     states,
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
		// Collapse arrays into one row by default; --expand-arrays shows every
		// task, and --array-id (drilling into one array) implies expanded.
		collapse := !queueExpandArrays && queueArrayID == ""
		printQueueTable(dtos, collapse)
	},
}

// slurmDisplayID returns the SLURM id to show for a proxied job: a plain job's
// slurm_job_id, or an array task's "<slurm_array_id>_<slurm_task_index>" (falling
// back to the bare array id). Empty when nothing has been recorded yet. Array
// tasks record slurm_array_id/slurm_task_index rather than slurm_job_id, so the
// queue must check both — otherwise proxied array tasks show no SLURM id.
func slurmDisplayID(job *jobs.JobDef) string {
	if id := job.GetRunningDetail("slurm_job_id", ""); id != "" {
		return id
	}
	if aid := job.GetRunningDetail("slurm_array_id", ""); aid != "" {
		if idx := job.GetRunningDetail("slurm_task_index", ""); idx != "" {
			return aid + "_" + idx
		}
		return aid
	}
	return ""
}

// normalizeStates upper-cases, trims, drops blanks, and validates the --state
// values into wire status names, returning a clear error on an unknown state.
func normalizeStates(raw []string) ([]string, error) {
	var out []string
	for _, s := range raw {
		s = strings.ToUpper(strings.TrimSpace(s))
		if s == "" {
			continue
		}
		if _, err := api.ParseStatus(s); err != nil {
			return nil, fmt.Errorf("invalid --state %q (valid: QUEUED, RUNNING, PROXYQUEUED, WAITING, USERHOLD, SUCCESS, FAILED, CANCELED)", s)
		}
		out = append(out, s)
	}
	return out, nil
}

// printQueueTable renders the standard tabular queue view for a slice of jobs.
// Shared by `queue` and `search`. When collapseArrays is set, tasks that share
// an array_id are folded into a single summary row (in the position of the
// array's first task), so a large array doesn't flood the queue.
func printQueueTable(dtos []*api.JobDTO, collapseArrays bool) {
	printQueueHeader()

	if !collapseArrays {
		for _, dto := range dtos {
			printJobRow(dto)
		}
		return
	}

	groups := map[string][]*api.JobDTO{}
	for _, dto := range dtos {
		if aid := dto.Details["array_id"]; aid != "" {
			groups[aid] = append(groups[aid], dto)
		}
	}
	printed := map[string]bool{}
	for _, dto := range dtos {
		aid := dto.Details["array_id"]
		if aid == "" {
			printJobRow(dto)
			continue
		}
		if printed[aid] {
			continue
		}
		printed[aid] = true
		printArraySummaryRow(aid, groups[aid])
	}
}

func printQueueHeader() {
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
}

// printArraySummaryRow renders one collapsed row for a whole array: the array id,
// an aggregate status, the shared template fields, and a per-status progress
// breakdown in the trailing column.
func printArraySummaryRow(arrayID string, members []*api.JobDTO) {
	m0 := members[0]
	tmpl := api.JobToDef(m0)
	fmt.Printf("| %-36.36s ", arrayID)
	fmt.Printf("| %-8.8s ", arrayDisplayStatus(members))
	fmt.Printf("| %-20.20s ", tmpl.Name)
	if Config.Batchq.Multiuser {
		fmt.Printf("| %-12.12s ", tmpl.GetDetail("user", ""))
	}
	fmt.Printf("| %-3.3s ", tmpl.GetDetail("procs", ""))
	fmt.Printf("| %-8.8s ", jobs.PrintMemoryString(tmpl.GetDetail("mem", "")))
	fmt.Printf("| %-11.11s ", jobs.WalltimeStringToString(tmpl.GetDetail("walltime", "")))
	fmt.Printf("| %-6.6s ", relativeAge(m0.SubmitTime))
	fmt.Printf("| %s\n", arrayProgress(members))
}

// effectiveStatus is a task's status for the collapsed array view. For a
// PROXYQUEUED task, its recorded SLURM state (PENDING/RUNNING/…) is shown instead
// of the bare "PROXYQUEUED", since that's what's actually happening on the
// cluster — otherwise a whole array that SLURM is actively running just reads as
// PROXYQUEUED in the queue.
func effectiveStatus(m *api.JobDTO) string {
	if m.Status == jobs.PROXYQUEUED.String() {
		if ss := m.RunningDetails["slurm_status"]; ss != "" {
			return ss
		}
	}
	return m.Status
}

// arrayDisplayStatus is the single aggregate status for a collapsed array row,
// over effectiveStatus — so a proxied array with SLURM-running tasks shows
// RUNNING, not PROXYQUEUED. Returns the most "active" state present.
func arrayDisplayStatus(members []*api.JobDTO) string {
	present := map[string]bool{}
	for _, m := range members {
		present[effectiveStatus(m)] = true
	}
	for _, s := range []string{"RUNNING", "COMPLETING", "SUSPENDED", "PENDING", "PROXYQUEUED", "QUEUED", "WAITING", "USERHOLD", "FAILED", "CANCELED"} {
		if present[s] {
			return s
		}
	}
	return jobs.SUCCESS.String()
}

// isDoneStatus reports whether an effective status is terminal (batchq or SLURM).
func isDoneStatus(s string) bool {
	switch s {
	case "SUCCESS", "FAILED", "CANCELED",
		"COMPLETED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY", "BOOT_FAIL", "NODE_FAIL", "DEADLINE":
		return true
	}
	return false
}

// arrayProgress summarizes an array's tasks as "<done>/<total> · <status counts>"
// using effectiveStatus, e.g. "5/33 · PENDING 26 RUNNING 2 SUCCESS 5". Known
// states are shown in a readable progression; any other SLURM states are
// appended (sorted) so nothing is dropped.
func arrayProgress(members []*api.JobDTO) string {
	counts := map[string]int{}
	done := 0
	for _, m := range members {
		es := effectiveStatus(m)
		counts[es]++
		if isDoneStatus(es) {
			done++
		}
	}
	order := []string{"USERHOLD", "WAITING", "QUEUED", "PENDING", "PROXYQUEUED", "RUNNING", "SUCCESS", "FAILED", "CANCELED"}
	seen := map[string]bool{}
	parts := make([]string, 0, len(counts))
	for _, st := range order {
		if counts[st] > 0 {
			parts = append(parts, fmt.Sprintf("%s %d", st, counts[st]))
			seen[st] = true
		}
	}
	extra := make([]string, 0)
	for st := range counts {
		if !seen[st] {
			extra = append(extra, st)
		}
	}
	sort.Strings(extra)
	for _, st := range extra {
		parts = append(parts, fmt.Sprintf("%s %d", st, counts[st]))
	}
	return fmt.Sprintf("array %d/%d · %s", done, len(members), strings.Join(parts, " "))
}

// printJobRow renders a single job as one queue-table row.
func printJobRow(dto *api.JobDTO) {
	job := api.JobToDef(dto)
	if s, perr := api.ParseStatus(dto.Status); perr == nil {
		job.Status = s
	}
	fmt.Printf("| %-36.36s ", job.JobId)
	// Show the effective status: a proxied job displays its SLURM state
	// (PENDING/RUNNING/…) rather than the bare PROXYQUEUED, matching the collapsed
	// array row. The batchq status (job.Status) still drives the layout below.
	fmt.Printf("| %-8.8s ", effectiveStatus(dto))
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
		if sid := slurmDisplayID(job); sid != "" {
			// The SLURM status is now shown in the status column; show just the
			// SLURM id here.
			fmt.Printf(" slurm:%s;", sid)
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
				target, err := resolveTarget(ctx, c, jobid, showIncludeArchives)
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

// showIncludeArchives is the --archives flag on status/details: fall back to the
// server's archive DBs for a job/array not found in the live DB.
var showIncludeArchives bool
var queueRunID string
var queueArrayID string
var queueOutput string
var queueInput string
var queueSince string
var queueBefore string
var queueSortTime bool
var queueSortReverse bool
var queueStates []string
var queueExpandArrays bool

func init() {
	queueCmd.Flags().BoolVar(&jobShowAll, "all", false, "Show all jobs (including completed)")
	queueCmd.Flags().StringSliceVarP(&queueStates, "state", "s", nil, "Only show jobs in these states (comma-separated, case-insensitive; e.g. RUNNING,QUEUED)")
	queueCmd.Flags().BoolVar(&queueExpandArrays, "expand-arrays", false, "Show every array task as its own row (default: collapse each array into one summary row)")
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
	statusCmd.Flags().BoolVar(&showIncludeArchives, "archives", false, "Also look in archived jobs when an id isn't in the live DB")
	detailsCmd.Flags().BoolVar(&statusJSON, "json", false, "Machine-readable: a JSON array of job objects")
	detailsCmd.Flags().BoolVar(&showIncludeArchives, "archives", false, "Also look in archived jobs when an id isn't in the live DB")

	rootCmd.AddCommand(detailsCmd)
	rootCmd.AddCommand(queueCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(summaryCmd)
}
