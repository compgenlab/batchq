package cmd

import (
	"fmt"
	"log"
	"os"

	"github.com/compgenlab/batchq/support"
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
		password := Config.Web.Password

		// Secure-by-default on a public TCP listener: rather than serve an
		// unauthenticated surface, generate any missing credential, print it to
		// stderr, and persist it to the config file so restarts are stable.
		if webListen != "" || Config.Web.Listen != "" {
			toSave := rawConfig.Clone() // on-disk state; avoids baking defaults in
			save := false
			if password == "" {
				p, err := support.RandomString(16)
				if err != nil {
					log.SetOutput(os.Stderr)
					log.Fatalf("generate web password: %v", err)
				}
				password = p
				toSave.Web.Password = p
				save = true
				fmt.Fprintf(os.Stderr, "Generated web UI password (user %q): %s\n", Config.Web.Username, p)
			}
			if apiEnabled && apiToken == "" {
				tok, err := support.RandomToken()
				if err != nil {
					log.SetOutput(os.Stderr)
					log.Fatalf("generate API token: %v", err)
				}
				apiToken = tok
				toSave.Batchq.Token = tok
				save = true
				fmt.Fprintf(os.Stderr, "Generated REST API token: %s\n", tok)
			}
			if save {
				if err := support.SaveConfig(configFile, toSave); err != nil {
					fmt.Fprintf(os.Stderr, "warning: could not save generated credentials to %s: %v\n", configFile, err)
				} else {
					fmt.Fprintf(os.Stderr, "Saved generated credentials to %s\n", configFile)
				}
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
			Password:   password,
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
