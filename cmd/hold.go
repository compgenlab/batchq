package cmd

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/mbreese/batchq/db"
	"github.com/spf13/cobra"
)

var holdCmd = &cobra.Command{
	Use:   "hold job-id...",
	Short: "Hold job",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			cmd.Help()
			return
		}

		if jobq, err := db.OpenDB(dbpath); err != nil {
			log.Fatalln(err)
		} else {
			defer jobq.Close()
			for _, arg := range args {
				if strings.Count(arg, "-") == 1 {
					spl := strings.Split(arg, "-")
					if jobid1, err := strconv.Atoi(spl[0]); err != nil {
						fmt.Printf("Bad job-id: %s\n", spl[0])
					} else {
						if jobid2, err := strconv.Atoi(spl[1]); err != nil {
							fmt.Printf("Bad job-id: %s\n", spl[1])
						} else {
							for jobid := jobid1; jobid <= jobid2; jobid++ {
								ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
								defer cancel()

								if jobq.HoldJob(ctx, jobid) {
									fmt.Printf("Job: %d held\n", jobid)
								} else {
									fmt.Printf("Error holding job: %d\n", jobid)
								}
							}
						}
					}
				} else {
					if jobid, err := strconv.Atoi(arg); err != nil {
						fmt.Printf("Bad job-id: %s\n", arg)
					} else {
						ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
						defer cancel()

						if jobq.HoldJob(ctx, jobid) {
							fmt.Printf("Job: %d held\n", jobid)
						} else {
							fmt.Printf("Error holding job: %d\n", jobid)
						}
					}
				}
			}
		}
	},
}

var releaseCmd = &cobra.Command{
	Use:   "release job-id...",
	Short: "Release user-hold on job",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			cmd.Help()
			return
		}

		if jobq, err := db.OpenDB(dbpath); err != nil {
			log.Fatalln(err)
		} else {
			defer jobq.Close()
			for _, arg := range args {
				if strings.Count(arg, "-") == 1 {
					spl := strings.Split(arg, "-")
					if jobid1, err := strconv.Atoi(spl[0]); err != nil {
						fmt.Printf("Bad job-id: %s\n", spl[0])
					} else {
						if jobid2, err := strconv.Atoi(spl[1]); err != nil {
							fmt.Printf("Bad job-id: %s\n", spl[1])
						} else {
							for jobid := jobid1; jobid <= jobid2; jobid++ {
								ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
								defer cancel()

								if jobq.ReleaseJob(ctx, jobid) {
									fmt.Printf("Job: %d released\n", jobid)
								} else {
									fmt.Printf("Error releasing job: %d\n", jobid)
								}
							}
						}
					}
				} else {
					if jobid, err := strconv.Atoi(arg); err != nil {
						fmt.Printf("Bad job-id: %s\n", arg)
					} else {
						ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
						defer cancel()

						if jobq.ReleaseJob(ctx, jobid) {
							fmt.Printf("Job: %d released\n", jobid)
						} else {
							fmt.Printf("Error releasing job: %d\n", jobid)
						}
					}
				}
			}
		}
	},
}

var cancelCmd = &cobra.Command{
	Use:   "cancel job1_id-job2_id ...",
	Short: "Cancel a job (running or queued)",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			cmd.Help()
			return
		}

		if jobq, err := db.OpenDB(dbpath); err != nil {
			log.Fatalln(err)
		} else {
			defer jobq.Close()
			for _, arg := range args {
				if strings.Count(arg, "-") == 1 {
					spl := strings.Split(arg, "-")
					if jobid1, err := strconv.Atoi(spl[0]); err != nil {
						fmt.Printf("Bad job-id: %s\n", spl[0])
					} else {
						if jobid2, err := strconv.Atoi(spl[1]); err != nil {
							fmt.Printf("Bad job-id: %s\n", spl[1])
						} else {
							for jobid := jobid1; jobid <= jobid2; jobid++ {
								// this can propagate, so it can take a while...
								ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
								defer cancel()

								if job := jobq.GetJob(ctx, jobid); job != nil {
									if job.GetRunningDetail("slurm_job_id", "") != "" {
										cmd := exec.Command("scancel", job.GetRunningDetail("slurm_job_id", ""))
										if err := cmd.Run(); err != nil {
											fmt.Printf("Error canceling slurm job: %v\n", err)
										} else {
											fmt.Printf("Canceled slurm job: %s\n", job.GetRunningDetail("slurm_job_id", ""))
										}
									}
									if jobq.CancelJob(ctx, jobid, cancelReason) {
										fmt.Printf("Job: %d canceled\n", jobid)
									} else {
										fmt.Printf("Error canceling job: %d\n", jobid)
									}
								}
							}
						}
					}
				} else {
					if jobid, err := strconv.Atoi(arg); err != nil {
						fmt.Printf("Bad job-id: %s\n", arg)
					} else {
						// this can propagate, so it can take a while...
						ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
						defer cancel()

						if job := jobq.GetJob(ctx, jobid); job != nil {
							if job.GetRunningDetail("slurm_job_id", "") != "" {
								cmd := exec.Command("scancel", job.GetRunningDetail("slurm_job_id", ""))
								if err := cmd.Run(); err != nil {
									fmt.Printf("Error canceling slurm job: %v\n", err)
								} else {
									fmt.Printf("Canceled slurm job: %s\n", job.GetRunningDetail("slurm_job_id", ""))
								}
							}
							if jobq.CancelJob(ctx, jobid, cancelReason) {
								fmt.Printf("Job: %d canceled\n", jobid)
							} else {
								fmt.Printf("Error canceling job: %d\n", jobid)
							}
						}
					}
				}
			}
		}
	},
}

var topCmd = &cobra.Command{
	Use:   "top job-id...",
	Short: "Move job to top of queue",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			cmd.Help()
			return
		}

		if jobq, err := db.OpenDB(dbpath); err != nil {
			log.Fatalln(err)
		} else {
			defer jobq.Close()
			for _, arg := range args {
				if strings.Count(arg, "-") == 1 {
					spl := strings.Split(arg, "-")
					if jobid1, err := strconv.Atoi(spl[0]); err != nil {
						fmt.Printf("Bad job-id: %s\n", spl[0])
					} else {
						if jobid2, err := strconv.Atoi(spl[1]); err != nil {
							fmt.Printf("Bad job-id: %s\n", spl[1])
						} else {
							for jobid := jobid1; jobid <= jobid2; jobid++ {
								ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
								defer cancel()

								if jobq.TopJob(ctx, jobid) {
									fmt.Printf("Job: %d prioritized\n", jobid)
								} else {
									fmt.Printf("Error prioritizing job: %d\n", jobid)
								}
							}
						}
					}
				} else {
					if jobid, err := strconv.Atoi(arg); err != nil {
						fmt.Printf("Bad job-id: %s\n", arg)
					} else {
						ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
						defer cancel()

						if jobq.TopJob(ctx, jobid) {
							fmt.Printf("Job: %d prioritized\n", jobid)
						} else {
							fmt.Printf("Error prioritizing job: %d\n", jobid)
						}
					}
				}
			}
		}
	},
}

var cancelReason string

func init() {
	cancelCmd.Flags().StringVar(&cancelReason, "reason", "Canceled by user", "Reason for canceling")

	rootCmd.AddCommand(holdCmd)
	rootCmd.AddCommand(releaseCmd)
	rootCmd.AddCommand(cancelCmd)
	rootCmd.AddCommand(topCmd)
}
