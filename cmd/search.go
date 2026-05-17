package cmd

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/mbreese/batchq/client"
	"github.com/spf13/cobra"
)

// searchCmd queries the server for jobs whose id, name, or script
// content matches the given term. The output shape depends on the hit
// count: a single match prints the full details (like `batchq details`)
// since that's almost always what you want; multiple matches fall back
// to the standard queue table (like `batchq queue`). Zero matches
// prints a message to stderr and exits 1 so scripts can detect it.
//
// Search is across ALL jobs (completed included) — the use case is
// "find that one job," not "filter the live queue."
var searchCmd = &cobra.Command{
	Use:   "search term",
	Short: "Find jobs matching a substring of id, name, or script",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		term := strings.TrimSpace(args[0])
		if term == "" {
			fmt.Fprintln(os.Stderr, "search: empty term")
			os.Exit(2)
		}

		c := mustDialClient()
		defer c.Close()

		ctx, cancel := cmdContext()
		defer cancel()
		dtos, err := c.ListJobs(ctx, client.ListJobsOptions{
			Query:        term,
			ShowAll:      true,
			SortByStatus: true,
		})
		if err != nil {
			log.Fatalln(err)
		}

		switch len(dtos) {
		case 0:
			fmt.Fprintf(os.Stderr, "no jobs match %q\n", term)
			os.Exit(1)
		case 1:
			printJobDetails(dtos[0])
		default:
			printQueueTable(dtos)
		}
	},
}

func init() {
	rootCmd.AddCommand(searchCmd)
}
