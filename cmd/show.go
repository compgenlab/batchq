package cmd

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/mbreese/batchq/db"
	"github.com/mbreese/batchq/jobs"
	"github.com/mbreese/batchq/support"
	"github.com/spf13/cobra"
)

var detailsCmd = &cobra.Command{
	Use:   "details jobid",
	Short: "Show details for a job",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if jobq, err := db.OpenDB(dbpath); err != nil {
			log.Fatalln(err)
		} else {
			defer jobq.Close()
			for _, ids := range args {
				for _, spl := range strings.Split(ids, ",") {
					if jobid, err := strconv.Atoi(spl); err != nil {
						log.Fatal(err)
					} else {
						if job := jobq.GetJob(ctx, jobid); job != nil {
							job.Print()
							fmt.Println("")
						}
					}
				}
			}
		}
	},
}

var queueCmd = &cobra.Command{
	Use:   "queue",
	Short: "Show the job queue",
	Run: func(cmd *cobra.Command, args []string) {
		if jobq, err := db.OpenDB(dbpath); err != nil {
			log.Fatalln(err)
		} else {
			defer jobq.Close()
			fmt.Printf("| %-6.6s ", "jobid")
			fmt.Printf("| %-8.8s ", "status")
			fmt.Printf("| %-20.20s ", "job-name")
			if val, _ := Config.GetBool("batchq", "multiuser", false); val {
				fmt.Printf("| %-12.12s ", "username")
			}
			fmt.Printf("|%-5.5s", "procs")
			fmt.Printf("| %-8.8s ", "mem")
			fmt.Printf("| %-11.11s ", "walltime")
			fmt.Println("|")
			if val, _ := Config.GetBool("batchq", "multiuser", false); val {
				fmt.Println("|--------|----------|----------------------|--------------|-----|----------|-------------|")
			} else {
				fmt.Println("|--------|----------|----------------------|-----|----------|-------------|")
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			for _, job := range jobq.GetJobs(ctx, jobShowAll, true) {
				// job.Print()
				fmt.Printf("| %-6.d ", job.JobId)
				fmt.Printf("| %-8.8s ", job.Status.String())
				fmt.Printf("| %-20.20s ", job.Name)
				if val, _ := Config.GetBool("batchq", "multiuser", false); val {
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
					//elapsed := time.Since(job.StartTime)
					fmt.Printf("| %-11.11s ", jobs.WalltimeToString(int(elapsed.Seconds())))
					fmt.Printf("| %-20.20s\n", fmt.Sprintf("pid:%s", job.GetRunningDetail("pid", "")))
				case jobs.PROXYQUEUED:
					fmt.Printf("| %-11.11s ", jobs.WalltimeStringToString(job.GetDetail("walltime", "")))
					fmt.Print("|")
					if job.GetRunningDetail("slurm_job_id", "") != "" {
						fmt.Printf(" %s", fmt.Sprintf("slurm:%s %s;", job.GetRunningDetail("slurm_status", ""), job.GetRunningDetail("slurm_job_id", "")))
					}
					if len(job.AfterOk) > 0 {
						depStr := fmt.Sprintf("deps:%s", support.JoinInt(job.AfterOk, ","))
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
						depStr := fmt.Sprintf("deps:%s", support.JoinInt(job.AfterOk, ","))
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
	},
}

var statusCmd = &cobra.Command{
	Use:   "status {job1 job2...}",
	Short: "Status for a job",
	Run: func(cmd *cobra.Command, args []string) {
		if jobq, err := db.OpenDB(dbpath); err != nil {
			log.Fatalln(err)
		} else {
			defer jobq.Close()
			if len(args) == 0 {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				for _, job := range jobq.GetJobs(ctx, jobShowAll, false) {
					fmt.Printf("%d %s\n", job.JobId, job.Status.String())
				}
			} else {
				for _, ids := range args {
					for _, spl := range strings.Split(ids, ",") {
						if jobid, err := strconv.Atoi(spl); err != nil {
							log.Fatal(err)
						} else {
							ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
							defer cancel()

							if job := jobq.GetJob(ctx, jobid); job != nil {
								fmt.Printf("%d %s\n", job.JobId, job.Status.String())
							}
						}
					}
				}
			}
		}
	},
}

var summaryCmd = &cobra.Command{
	Use:   "summary",
	Short: "Summary of the job queue",
	Run: func(cmd *cobra.Command, args []string) {
		if jobq, err := db.OpenDB(dbpath); err != nil {
			log.Fatalln(err)
		} else {
			defer jobq.Close()
			if len(args) == 0 {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				var counts map[jobs.StatusCode]int = make(map[jobs.StatusCode]int)
				for _, status := range []jobs.StatusCode{jobs.USERHOLD, jobs.WAITING, jobs.QUEUED, jobs.PROXYQUEUED, jobs.RUNNING, jobs.SUCCESS, jobs.FAILED, jobs.CANCELED} {
					counts[status] = 0
				}

				for _, job := range jobq.GetJobs(ctx, jobShowAll, false) {
					if _, ok := counts[job.Status]; ok {
						counts[job.Status]++
					}
				}

				for _, status := range []jobs.StatusCode{jobs.USERHOLD, jobs.WAITING, jobs.QUEUED, jobs.PROXYQUEUED, jobs.RUNNING, jobs.SUCCESS, jobs.FAILED, jobs.CANCELED} {
					if jobShowAll || counts[status] > 0 {
						fmt.Printf("%-12s: %d\n", status.String(), counts[status])
					}
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
