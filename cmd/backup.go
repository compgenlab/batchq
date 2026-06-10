package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// backupCmd asks the running server to snapshot its database via SQLite's
// VACUUM INTO. The snapshot is taken by the server's own connection, so no
// second process ever opens the live DB file — the property that keeps it
// safe on NFS/Lustre, unlike running `sqlite3 .backup` externally.
var backupCmd = &cobra.Command{
	Use:   "backup [destination]",
	Short: "Write a consistent snapshot of the database",
	Long: `Ask the running batchq server to snapshot its database to a file using
SQLite's VACUUM INTO. The snapshot is a fully consistent, defragmented copy.

The server takes the snapshot on its own connection, so no second process
opens the live database file. This is the safe way to back up a DB that lives
on NFS/Lustre — running 'sqlite3 .backup' or '.dump' externally would open a
second connection to the file, the exact cross-process case the single-server
model avoids.

IMPORTANT: the destination path resolves against the SERVER's filesystem, not
this client's. For a local server (unix socket) that's the same host, so a
local path works. For a --remote server the snapshot is written on the remote
host. The path actually written is printed on success.

With no destination, the server picks a timestamped default under its
$BATCHQ_HOME/backups/. The destination must not already exist.`,
	Args: cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		dest := ""
		if len(args) == 1 {
			dest = args[0]
		}

		c := mustDialClient()
		defer c.Close()

		// Generous budget: VACUUM INTO of a large DB can run a while, and
		// because the server decouples cancellation it finishes regardless of
		// a client timeout — so wait rather than give up early.
		ctx, cancel := cmdContextRetryable()
		defer cancel()

		path, err := c.Backup(ctx, dest)
		if err != nil {
			fmt.Fprintf(os.Stderr, "batchq backup: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "backup written to %s\n", path)
	},
}

func init() {
	rootCmd.AddCommand(backupCmd)
}
