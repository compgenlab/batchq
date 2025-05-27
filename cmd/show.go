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
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if jobq, err := db.OpenDB(dbpath); err != nil {
			log.Fatalln(err)
		} else {
			defer jobq.Close()
			fmt.Printf("| %-6.6s ", "jobid")
			fmt.Printf("| %-10.10s ", "status")
			fmt.Printf("| %-20.20s ", "job-name")
			if val, _ := Config.GetBool("batchq", "multiuser", false); val {
				fmt.Printf("| %-12.12s ", "username")
			}
			fmt.Printf("|%-5.5s", "procs")
			fmt.Printf("| %-8.8s ", "mem")
			fmt.Printf("| %-11.11s ", "walltime")
			fmt.Println("|")
			if val, _ := Config.GetBool("batchq", "multiuser", false); val {
				fmt.Println("|--------|------------|----------------------|--------------|-----|----------|-------------|")
			} else {
				fmt.Println("|--------|------------|----------------------|-----|----------|-------------|")
			}

			for _, job := range jobq.GetJobs(ctx, jobShowAll, true) {
				// job.Print()
				fmt.Printf("| %-6.d ", job.JobId)
				fmt.Printf("| %-10.10s ", job.Status.String())
				fmt.Printf("| %-20.20s ", job.Name)
				if val, _ := Config.GetBool("batchq", "multiuser", false); val {
					fmt.Printf("| %-12.12s ", job.GetDetail("user", ""))
				}
				fmt.Printf("| %-3.3s ", job.GetDetail("procs", ""))
				fmt.Printf("| %-8.8s ", jobs.PrintMemoryString(job.GetDetail("mem", "")))
				if job.Status == jobs.FAILED || job.Status == jobs.CANCELLED || job.Status == jobs.SUCCESS {
					elapsed := job.EndTime.Sub(job.StartTime)
					fmt.Printf("| %-11.11s ", jobs.PrintWalltime(int(elapsed.Seconds())))
					if job.Status == jobs.CANCELLED {
						fmt.Printf("| %-20.20s\n", job.Notes)
					} else if job.Status == jobs.FAILED {
						fmt.Printf("| %-20.20s\n", fmt.Sprintf("exit:%d", job.ReturnCode))
					} else {
						fmt.Println("|")
					}
				} else if job.Status == jobs.RUNNING {
					elapsed := time.Now().UTC().Sub(job.StartTime)
					//elapsed := time.Since(job.StartTime)
					fmt.Printf("| %-11.11s ", jobs.PrintWalltime(int(elapsed.Seconds())))
					fmt.Printf("| %-20.20s\n", fmt.Sprintf("pid:%s", job.GetRunningDetail("pid", "")))
				} else {
					fmt.Printf("| %-11.11s ", jobs.PrintWalltimeString(job.GetDetail("walltime", "")))
					if len(job.AfterOk) > 0 {
						fmt.Printf("| %-20.20s\n", fmt.Sprintf("deps:%s", support.JoinInt(job.AfterOk, ",")))
					} else {
						fmt.Println("|")
					}
				}
			}
		}
	},
}

var statusCmd = &cobra.Command{
	Use:   "status {job1 job2...}",
	Short: "Status for a job",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if jobq, err := db.OpenDB(dbpath); err != nil {
			log.Fatalln(err)
		} else {
			defer jobq.Close()
			if len(args) == 0 {
				for _, job := range jobq.GetJobs(ctx, jobShowAll, false) {
					fmt.Printf("%d %s\n", job.JobId, job.Status.String())
				}
			} else {
				for _, ids := range args {
					for _, spl := range strings.Split(ids, ",") {
						if jobid, err := strconv.Atoi(spl); err != nil {
							log.Fatal(err)
						} else {
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

var jobShowAll bool

func init() {
	queueCmd.Flags().BoolVar(&jobShowAll, "all", false, "Show all jobs (including completed)")

	rootCmd.AddCommand(detailsCmd)
	rootCmd.AddCommand(queueCmd)
	rootCmd.AddCommand(statusCmd)
}
