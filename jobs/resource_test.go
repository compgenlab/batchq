package jobs

import "testing"

func TestIsReservedResourceName(t *testing.T) {
	for _, name := range []string{"procs", "mem", "walltime"} {
		if !IsReservedResourceName(name) {
			t.Errorf("IsReservedResourceName(%q) = false, want true", name)
		}
	}
	for _, name := range []string{"gpu", "cluster", "host", "feature", ""} {
		if IsReservedResourceName(name) {
			t.Errorf("IsReservedResourceName(%q) = true, want false", name)
		}
	}
}

func TestParseResourceEntry(t *testing.T) {
	tests := []struct {
		entry     string
		wantName  string
		wantValue string
		wantErr   bool
	}{
		// countable: integer value
		{"gpu=2", "gpu", "2", false},
		{"gpu:a100=2", "gpu:a100", "2", false},
		// label: non-integer value
		{"cluster=xyz", "cluster", "xyz", false},
		// feature flag: bare name, empty value
		{"highmem", "highmem", "", false},
		// whitespace is trimmed on both halves
		{"  gpu = 2 ", "gpu", "2", false},
		// reserved names rejected
		{"procs=4", "", "", true},
		{"mem=8G", "", "", true},
		{"walltime=1:00", "", "", true},
		// bad names rejected
		{"", "", "", true},
		{"=2", "", "", true},
		{"a b=2", "", "", true},
		{"a\tb", "", "", true},
	}
	for _, tt := range tests {
		name, value, err := ParseResourceEntry(tt.entry)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParseResourceEntry(%q): expected error, got name=%q value=%q", tt.entry, name, value)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseResourceEntry(%q): unexpected error %v", tt.entry, err)
			continue
		}
		if name != tt.wantName || value != tt.wantValue {
			t.Errorf("ParseResourceEntry(%q) = (%q, %q), want (%q, %q)",
				tt.entry, name, value, tt.wantName, tt.wantValue)
		}
	}
}
