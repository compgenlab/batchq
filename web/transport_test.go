package web

import (
	"strings"
	"testing"

	"github.com/compgenlab/batchq/support"
)

// chooseTransport must let a listen address (flag or config) win over the
// defaulted socket, while still erroring when a socket and listen are set at
// the same layer. Guards the pre-existing "configure only one" footgun where
// the always-defaulted [web] socket conflicted with --listen.
func TestChooseTransport(t *testing.T) {
	defSock := support.NewDefaults().WebSocket

	// A config as it looks after ApplyDefaults for a plain (no-listen) setup.
	defaultedCfg := func() *support.Config {
		c := &support.Config{}
		c.Web.Socket = defSock
		return c
	}

	tests := []struct {
		name       string
		opts       Options
		wantListen string
		wantSocket bool // true => expect a socket path, no listen
		wantErr    bool
	}{
		{
			name:       "listen flag beats defaulted socket",
			opts:       Options{ListenAddr: "127.0.0.1:9000", Config: defaultedCfg()},
			wantListen: "127.0.0.1:9000",
		},
		{
			name:       "socket flag",
			opts:       Options{SocketPath: "/tmp/x.sock", Config: defaultedCfg()},
			wantSocket: true,
		},
		{
			name:    "both flags conflict",
			opts:    Options{SocketPath: "/tmp/x.sock", ListenAddr: ":9000"},
			wantErr: true,
		},
		{
			name:       "config listen (socket not defaulted when listen set)",
			opts:       Options{Config: &support.Config{Web: support.WebConfig{Listen: ":9001"}}},
			wantListen: ":9001",
		},
		{
			name:    "config socket and listen both set conflict",
			opts:    Options{Config: &support.Config{Web: support.WebConfig{Socket: "/tmp/x.sock", Listen: ":9002"}}},
			wantErr: true,
		},
		{
			name:       "default socket when nothing set",
			opts:       Options{Config: defaultedCfg()},
			wantSocket: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sock, listen, err := chooseTransport(tc.opts)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got sock=%q listen=%q", sock, listen)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantListen != "" {
				if listen != tc.wantListen || sock != "" {
					t.Fatalf("want listen %q (no socket), got sock=%q listen=%q", tc.wantListen, sock, listen)
				}
				return
			}
			if tc.wantSocket {
				if sock == "" || listen != "" {
					t.Fatalf("want a socket (no listen), got sock=%q listen=%q", sock, listen)
				}
				if !strings.HasPrefix(sock, "/") {
					t.Fatalf("socket path not absolute: %q", sock)
				}
			}
		})
	}
}
