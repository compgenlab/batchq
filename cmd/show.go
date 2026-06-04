package cmd

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/mbreese/batchq/api"
	"github.com/mbreese/batchq/client"
	"github.com/mbreese/batchq/jobs"
	"github.com/spf13/cobra"
)

var detailsCmd = &cobra.Command{
	Use:   "details jobid",
	Short: "Show details for a job",
	Run: func(cmd *cobra.Command, args []string) {
		c := mustDialClient()
		defer c.Close()

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
		var dtos []*api.JobDTO
		var err error
		if queueRunID != "" || queueArrayID != "" || queueOutput != "" || queueInput != "" {
			dtos, err = c.ListJobs(ctx, client.ListJobsOptions{
				ShowAll:      jobShowAll,
				SortByStatus: !queueSortTime,
				RunID:        queueRunID,
				ArrayID:      queueArrayID,
				Output:       queueOutput,
				Input:        queueInput,
			})
		} else {
			dtos, err = c.GetQueueJobs(ctx, jobShowAll, !queueSortTime)
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

var statusCmd = &cobra.Command{
	Use:   "status {job1 job2...}",
	Short: "Status for a job",
	Run: func(cmd *cobra.Command, args []string) {
		c := mustDialClient()
		defer c.Close()

		if len(args) == 0 {
			ctx, cancel := cmdContext()
			defer cancel()
			dtos, err := c.ListJobs(ctx, client.ListJobsOptions{ShowAll: jobShowAll})
			if err != nil {
				log.Fatalln(err)
			}
			for _, dto := range dtos {
				fmt.Printf("%s %s\n", dto.JobID, dto.Status)
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
				if target.isArray {
					printArraySummary(target.arrayID, target.members, false)
				} else {
					fmt.Printf("%s %s\n", target.dto.JobID, target.dto.Status)
				}
			}
		}
	},
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
var queueRunID string
var queueArrayID string
var queueOutput string
var queueInput string
var queueSortTime bool
var queueSortReverse bool

func init() {
	queueCmd.Flags().BoolVar(&jobShowAll, "all", false, "Show all jobs (including completed)")
	queueCmd.Flags().StringVar(&queueRunID, "run-id", "", "Only show jobs in this workflow run")
	queueCmd.Flags().StringVar(&queueArrayID, "array-id", "", "Only show tasks in this job array")
	queueCmd.Flags().StringVar(&queueOutput, "output", "", "Only show jobs that list this file as an output")
	queueCmd.Flags().StringVar(&queueInput, "input", "", "Only show jobs that list this file as an input")
	queueCmd.Flags().BoolVarP(&queueSortTime, "time", "t", false, "Sort by submit time (newest first)")
	queueCmd.Flags().BoolVarP(&queueSortReverse, "reverse", "r", false, "Reverse sort order (use with -t)")
	summaryCmd.Flags().BoolVar(&jobShowAll, "all", false, "Show all jobs (including completed)")

	rootCmd.AddCommand(detailsCmd)
	rootCmd.AddCommand(queueCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(summaryCmd)
}
