package cmd

import (
	"context"
	"fmt"
	"log"
	"strconv"
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

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if jobq, err := db.OpenDB(dbpath); err != nil {
			log.Fatalln(err)
		} else {
			defer jobq.Close()
			for _, arg := range args {
				if jobid, err := strconv.Atoi(arg); err != nil {
					fmt.Printf("Bad job-id: %s\n", arg)
				} else {
					if jobq.HoldJob(ctx, jobid) {
						fmt.Printf("Job: %d held\n", jobid)
					} else {
						fmt.Printf("Error holding job: %d\n", jobid)
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

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if jobq, err := db.OpenDB(dbpath); err != nil {
			log.Fatalln(err)
		} else {
			defer jobq.Close()
			for _, arg := range args {
				if jobid, err := strconv.Atoi(arg); err != nil {
					fmt.Printf("Bad job-id: %s\n", arg)
				} else {
					if jobq.ReleaseJob(ctx, jobid) {
						fmt.Printf("Job: %d released\n", jobid)
					} else {
						fmt.Printf("Error releasing job: %d\n", jobid)
					}
				}
			}
		}
	},
}

var cancelCmd = &cobra.Command{
	Use:   "cancel job-id...",
	Short: "Cancel a job (running or queued)",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			cmd.Help()
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if jobq, err := db.OpenDB(dbpath); err != nil {
			log.Fatalln(err)
		} else {
			defer jobq.Close()
			for _, arg := range args {
				if jobid, err := strconv.Atoi(arg); err != nil {
					fmt.Printf("Bad job-id: %s\n", arg)
				} else {
					if jobq.CancelJob(ctx, jobid) {
						fmt.Printf("Job: %d cancelled\n", jobid)
					} else {
						fmt.Printf("Error cancelling job: %d\n", jobid)
					}
				}
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(holdCmd)
	rootCmd.AddCommand(releaseCmd)
	rootCmd.AddCommand(cancelCmd)
}
