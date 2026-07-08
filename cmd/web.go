package cmd

import (
	"fmt"
	"log"
	"os"

	"github.com/compgenlab/batchq/web"
	"github.com/spf13/cobra"
)

var webSocket string
var webListen string
var webForce bool
var webVerbose bool
var webAPI bool

var webCmd = &cobra.Command{
	Use:   "web",
	Short: "Start a local web UI",
	Run: func(cmd *cobra.Command, args []string) {
		c := mustDialClient()
		defer c.Close()

		apiEnabled := webAPI || Config.Web.API
		// The /api gate uses the resolved client token (same chain as
		// dialClient), and the proxy forwards that same token to the backend.
		apiToken := clientToken
		if apiToken == "" {
			apiToken = Config.Batchq.Token
		}

		// On a public TCP listener, warn loudly when a surface is unauthenticated
		// (mirrors cmd/server.go's TCP-without-token warning).
		if webListen != "" || Config.Web.Listen != "" {
			if Config.Web.Password == "" {
				fmt.Fprintln(os.Stderr, "WARNING: web UI is exposed on a TCP port without a password (set BATCHQ_PASSWORD or [web] password)")
			}
			if apiEnabled && apiToken == "" {
				fmt.Fprintln(os.Stderr, "WARNING: --api exposes the REST API on a TCP port without a token (set BATCHQ_TOKEN or [batchq] token)")
			}
		}

		opts := web.Options{
			Config:     Config,
			Client:     c,
			SocketPath: webSocket,
			ListenAddr: webListen,
			Force:      webForce,
			Verbose:    webVerbose,
			APIEnabled: apiEnabled,
			APIToken:   apiToken,
			Username:   Config.Web.Username,
			Password:   Config.Web.Password,
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
	webCmd.Flags().BoolVar(&webAPI, "api", false, "Also serve the REST API under /api (bearer-token protected)")
	rootCmd.AddCommand(webCmd)
}
