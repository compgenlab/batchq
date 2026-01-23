package cmd

import (
	"log"
	"os"

	"github.com/mbreese/batchq/web"
	"github.com/spf13/cobra"
)

var webSocket string
var webListen string
var webForce bool
var webVerbose bool

var webCmd = &cobra.Command{
	Use:   "web",
	Short: "Start a local web UI",
	Run: func(cmd *cobra.Command, args []string) {
		opts := web.Options{
			Config:     Config,
			DBPath:     dbpath,
			SocketPath: webSocket,
			ListenAddr: webListen,
			Force:      webForce,
			Verbose:    webVerbose,
		}
		if err := web.StartServer(opts); err != nil {
			log.SetOutput(os.Stderr)
			log.Fatal(err)
		}
	},
}

func init() {
	webCmd.Flags().StringVar(&webSocket, "socket", "", "Unix socket path for the web UI")
	webCmd.Flags().StringVar(&webListen, "listen", "", "TCP listen address (host:port) for the web UI")
	webCmd.Flags().BoolVar(&webForce, "force", false, "Remove existing socket before binding")
	webCmd.Flags().BoolVarP(&webVerbose, "verbose", "v", false, "Verbose output")
	rootCmd.AddCommand(webCmd)
}
