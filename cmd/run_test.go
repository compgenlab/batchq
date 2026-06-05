package cmd

import "testing"

func TestResolveRunnerID(t *testing.T) {
	cases := []struct{ flag, cfg, host, want string }{
		{"explicit", "cfg", "h", "explicit"}, // flag wins
		{"", "cfg", "h", "cfg"},              // then config
		{"", "", "host01", "host01"},         // then host
		{"", "", "", ""},                     // nothing -> runner keeps its UUID
	}
	for _, c := range cases {
		if got := resolveRunnerID(c.flag, c.cfg, c.host); got != c.want {
			t.Errorf("resolveRunnerID(%q,%q,%q) = %q, want %q", c.flag, c.cfg, c.host, got, c.want)
		}
	}
}

func TestResolveHostPrefersFlagThenConfig(t *testing.T) {
	if got := resolveHost("flaghost", "cfghost"); got != "flaghost" {
		t.Errorf("flag should win: %q", got)
	}
	if got := resolveHost("", "cfghost"); got != "cfghost" {
		t.Errorf("config should be used: %q", got)
	}
	// With neither set it falls back to the OS hostname (non-empty on any host).
	if got := resolveHost("", ""); got == "" {
		t.Errorf("expected an OS hostname fallback")
	}
}

func TestWithResource(t *testing.T) {
	if got := withResource(nil, "cluster", ""); got != nil {
		t.Errorf("empty value should leave resources nil, got %v", got)
	}
	if got := withResource(nil, "cluster", "hpc"); got["cluster"] != "hpc" {
		t.Errorf("resource not advertised: %v", got)
	}
	got := withResource(map[string]string{"cluster": "old", "gpu": "4"}, "cluster", "new")
	if got["cluster"] != "new" || got["gpu"] != "4" {
		t.Errorf("cluster should override, gpu should survive: %v", got)
	}
	got = withResource(map[string]string{"cluster": "x"}, "host", "node01")
	if got["host"] != "node01" || got["cluster"] != "x" {
		t.Errorf("host should be added alongside cluster: %v", got)
	}
}
