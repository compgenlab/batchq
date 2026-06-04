package support

import "testing"

func TestApplyEnvTokens(t *testing.T) {
	c := &Config{}
	c.Batchq.Token = "config-client"
	c.Server.Token = "config-server"

	// Empty env leaves the config values alone.
	c.ApplyEnv(EnvOverrides{})
	if c.Batchq.Token != "config-client" {
		t.Fatalf("client token clobbered by empty env: %q", c.Batchq.Token)
	}
	if c.Server.Token != "config-server" {
		t.Fatalf("server token clobbered by empty env: %q", c.Server.Token)
	}

	// Non-empty env overrides both.
	c.ApplyEnv(EnvOverrides{Token: "env-client", ServerToken: "env-server"})
	if c.Batchq.Token != "env-client" {
		t.Fatalf("client token: got %q, want env-client", c.Batchq.Token)
	}
	if c.Server.Token != "env-server" {
		t.Fatalf("server token: got %q, want env-server", c.Server.Token)
	}
}
