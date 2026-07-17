package cmd

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/compgenlab/batchq/api"
	"github.com/compgenlab/batchq/jobs"
	"github.com/spf13/cobra"
)

var cleanupCmd = &cobra.Command{
	Use:   "cleanup [archive-name]",
	Short: "Remove or archive completed jobs, and/or vacuum the database",
	Long: `Remove or archive terminal jobs (canceled/failed/successful) from the
database. You must choose an action explicitly:

  --delete     permanently remove the selected jobs
  --archive    move the selected jobs into a new read-only archive DB, then
               remove them from the live DB. Pass an optional name argument
               ("batchq cleanup --success --archive 2025q4") to name the archive
               file; the default is a timestamped name. Each run writes one new,
               immutable archive under [server] archive_dir.

Select which jobs with --canceled / --failed / --success / --all, optionally
narrowed by --older-than.

After a --delete or --archive the database is VACUUMed automatically to reclaim
the freed space; pass --no-vacuum to skip that. Run 'cleanup --vacuum' on its
own to just compact the live DB without removing anything.

The selection, dependency-safe planning, and delete/archive all run server-side
in one request — so it scales to a large backlog without per-job round-trips.`,
	Args: cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		if cleanupAll {
			cleanupCanceled = true
			cleanupFailed = true
			cleanupSuccess = true
		}
		purgeRequested := cleanupCanceled || cleanupFailed || cleanupSuccess

		archiveName := ""
		if len(args) == 1 {
			archiveName = args[0]
		}

		// Validate the action selection.
		if cleanupVacuum && cleanupNoVacuum {
			log.Fatal("--vacuum and --no-vacuum are mutually exclusive")
		}
		if cleanupDelete && cleanupArchive {
			log.Fatal("choose exactly one of --delete or --archive, not both")
		}
		if archiveName != "" && !cleanupArchive {
			log.Fatalf("archive name %q given without --archive", archiveName)
		}
		if purgeRequested && !cleanupDelete && !cleanupArchive {
			log.Fatal("select an action for the chosen jobs: --delete or --archive")
		}
		if !purgeRequested && (cleanupDelete || cleanupArchive) {
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

		var olderThanSecs int64
		if cleanupOlderThan != "" {
			d, err := parseAgeDuration(cleanupOlderThan)
			if err != nil {
				log.Fatalf("invalid --older-than %q: %v", cleanupOlderThan, err)
			}
			olderThanSecs = int64(d / time.Second)
		}

		var statuses []string
		if purgeRequested {
			if cleanupCanceled {
				statuses = append(statuses, jobs.CANCELED.String())
			}
			if cleanupFailed {
				statuses = append(statuses, jobs.FAILED.String())
			}
			if cleanupSuccess {
				statuses = append(statuses, jobs.SUCCESS.String())
			}
		}

		// Long-running client: server-side selection + dependency-safe
		// archive/delete + VACUUM on a large DB can far exceed 30s, and the
		// server decouples cancellation so it finishes regardless — so use a
		// client with no per-request cap (the caller's context bounds it).
		c := mustDialClientLongRunning()
		defer c.Close()

		ctx, cancel := cmdContextLong()
		defer cancel()

		resp, err := c.Cleanup(ctx, api.CleanupRequest{
			Statuses:      statuses,
			OlderThanSecs: olderThanSecs,
			Archive:       cleanupArchive,
			ArchiveName:   archiveName,
			Vacuum:        doVacuum,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "batchq cleanup: %v\n", err)
			os.Exit(1)
		}

		if purgeRequested {
			if cleanupArchive {
				if resp.Removed == 0 {
					fmt.Println("No jobs to archive")
				} else {
					fmt.Printf("Archived %d job(s) to %s\n", resp.Removed, resp.ArchivePath)
				}
			} else {
				fmt.Printf("Removed %d job(s)\n", resp.Removed)
			}
			if resp.Blocked > 0 {
				fmt.Printf("Left %d job(s) in place (a dependent was not eligible for removal)\n", resp.Blocked)
			}
		}
		if resp.Vacuumed {
			fmt.Println("Database vacuumed")
		}
	},
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

var cleanupCanceled bool
var cleanupFailed bool
var cleanupSuccess bool
var cleanupAll bool
var cleanupOlderThan string
var cleanupDelete bool
var cleanupArchive bool
var cleanupVacuum bool
var cleanupNoVacuum bool

func init() {
	cleanupCmd.Flags().BoolVar(&cleanupCanceled, "canceled", false, "Select canceled jobs")
	cleanupCmd.Flags().BoolVar(&cleanupFailed, "failed", false, "Select failed jobs")
	cleanupCmd.Flags().BoolVar(&cleanupSuccess, "success", false, "Select successful jobs")
	cleanupCmd.Flags().BoolVar(&cleanupAll, "all", false, "Select canceled, failed, and successful jobs")
	cleanupCmd.Flags().StringVar(&cleanupOlderThan, "older-than", "", "Only select jobs whose end_time is older than this duration (e.g. 30d, 12h, 1w)")

	cleanupCmd.Flags().BoolVar(&cleanupDelete, "delete", false, "Permanently remove the selected jobs")
	cleanupCmd.Flags().BoolVar(&cleanupArchive, "archive", false, "Archive the selected jobs into a new read-only archive DB, then remove them (pass an optional name argument)")
	cleanupCmd.Flags().BoolVar(&cleanupVacuum, "vacuum", false, "VACUUM the live DB (runs automatically after a purge; use alone to just compact)")
	cleanupCmd.Flags().BoolVar(&cleanupNoVacuum, "no-vacuum", false, "Skip the automatic VACUUM after a --delete/--archive")

	rootCmd.AddCommand(cleanupCmd)
}
