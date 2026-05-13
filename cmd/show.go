package cmd

import (
	"errors"
	"fmt"
	"log"
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
				dto, err := c.GetJob(ctx, jobid)
				cancel()
				if err != nil {
					if errors.Is(err, client.ErrNotFound) {
						continue
					}
					log.Fatalln(err)
				}
				if job := api.JobToDef(dto); job != nil {
					// Status string → StatusCode for the v1 printer.
					if s, perr := api.ParseStatus(dto.Status); perr == nil {
						job.Status = s
					}
					job.Print()
					fmt.Println("")
				}
			}
		}
	},
}

var queueCmd = &cobra.Command{
	Use:   "queue",
	Short: "Show the job queue",
	Run: func(cmd *cobra.Command, args []string) {
		c := mustDialClient()
		defer c.Close()

		fmt.Printf("| %-36.36s ", "jobid")
		fmt.Printf("| %-8.8s ", "status")
		fmt.Printf("| %-20.20s ", "job-name")
		if Config.Batchq.Multiuser {
			fmt.Printf("| %-12.12s ", "username")
		}
		fmt.Printf("|%-5.5s", "procs")
		fmt.Printf("| %-8.8s ", "mem")
		fmt.Printf("| %-11.11s ", "walltime")
		fmt.Println("|")
		if Config.Batchq.Multiuser {
			fmt.Println("|--------------------------------------|----------|----------------------|--------------|-----|----------|-------------|")
		} else {
			fmt.Println("|--------------------------------------|----------|----------------------|-----|----------|-------------|")
		}

		ctx, cancel := cmdContext()
		defer cancel()
		dtos, err := c.GetQueueJobs(ctx, jobShowAll, true)
		if err != nil {
			log.Fatalln(err)
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
			switch job.Status {
			case jobs.CANCELED:
				fmt.Printf("| %-11.11s ", "")
				fmt.Printf("| %-20.20s\n", job.Notes)
			case jobs.SUCCESS:
				elapsed := job.EndTime.Sub(job.StartTime)
				fmt.Printf("| %-11.11s ", jobs.WalltimeToString(int(elapsed.Seconds())))
				fmt.Println("|")
			case jobs.FAILED:
				elapsed := job.EndTime.Sub(job.StartTime)
				fmt.Printf("| %-11.11s ", jobs.WalltimeToString(int(elapsed.Seconds())))
				fmt.Printf("| %-20.20s\n", fmt.Sprintf("exit:%d", job.ReturnCode))
			case jobs.RUNNING:
				elapsed := time.Now().UTC().Sub(job.StartTime)
				fmt.Printf("| %-11.11s ", jobs.WalltimeToString(int(elapsed.Seconds())))
				fmt.Printf("| %-20.20s\n", fmt.Sprintf("pid:%s", job.GetRunningDetail("pid", "")))
			case jobs.PROXYQUEUED:
				fmt.Printf("| %-11.11s ", jobs.WalltimeStringToString(job.GetDetail("walltime", "")))
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
				fmt.Printf("| %-11.11s ", jobs.WalltimeStringToString(job.GetDetail("walltime", "")))
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
	},
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
				dto, err := c.GetJob(ctx, jobid)
				cancel()
				if err != nil {
					if errors.Is(err, client.ErrNotFound) {
						continue
					}
					log.Fatalln(err)
				}
				fmt.Printf("%s %s\n", dto.JobID, dto.Status)
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

func init() {
	queueCmd.Flags().BoolVar(&jobShowAll, "all", false, "Show all jobs (including completed)")
	summaryCmd.Flags().BoolVar(&jobShowAll, "all", false, "Show all jobs (including completed)")

	rootCmd.AddCommand(detailsCmd)
	rootCmd.AddCommand(queueCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(summaryCmd)
}
