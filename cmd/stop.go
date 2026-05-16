package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/mbreese/batchq/client"
	"github.com/spf13/cobra"
)

// stopCmd asks the local batchq server to drain in-flight requests and
// exit. It's the explicit form of what `--restart-server` does inline
// before any other command.
var stopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Shut down the running local batchq server",
	Long: `Ask the running local batchq server to drain in-flight requests and exit.

This is the explicit form of the --restart-server flag. Use it when you
want to stop the server without immediately running another command —
e.g. before swapping in a new binary, or to free the unix socket.

Has no effect when [batchq] remote is set: 'batchq stop' will not shut
down remote servers from a client.`,
	Run: func(cmd *cobra.Command, args []string) {
		remoteRaw := clientRemote
		if remoteRaw == "" {
			remoteRaw = Config.Batchq.Remote
		}
		if remoteRaw != "" {
			fmt.Fprintln(os.Stderr, "batchq stop: refusing to shut down a remote server")
			os.Exit(1)
		}
		dialURL := Config.Server.Listen
		if !strings.HasPrefix(dialURL, "unix://") {
			fmt.Fprintf(os.Stderr, "batchq stop: server listen URL %q is not a unix socket\n", dialURL)
			os.Exit(1)
		}
		opts := client.Options{URL: dialURL}
		if err := shutdownLocalServer(opts); err != nil {
			fmt.Fprintf(os.Stderr, "batchq stop: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintln(os.Stderr, "batchq stop: server shut down")
	},
}

func init() {
	rootCmd.AddCommand(stopCmd)
}
