package jobs

import "testing"

// TestNormalizeName pins the job-name canonicalization: every character
// outside [a-zA-Z0-9-_] becomes '_', then runs of underscores collapse to one.
// batchq stores only normalized names so a name never carries whitespace that
// would, e.g., make sbatch parse a later word as a stray `#SBATCH` directive.
func TestNormalizeName(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"Count reads per sample (extended walltime)", "Count_reads_per_sample_extended_walltime_"},
		{"my   bad  job!!", "my_bad_job_"},
		{"line1\nline2\tx", "line1_line2_x"},
		{"build-step_1", "build-step_1"},   // already safe: unchanged
		{"align.v2.step", "align.v2.step"}, // dots are preserved
		{"run 1.2 final", "run_1.2_final"}, // dot kept, spaces collapsed
		{"a___b", "a_b"},                 // existing underscore runs collapse
		{"", ""},
		{"plain", "plain"},
	}
	for _, tt := range tests {
		if got := NormalizeName(tt.in); got != tt.want {
			t.Errorf("NormalizeName(%q) = %q, want %q", tt.in, got, tt.want)
		}
		// Idempotent: normalizing a normalized name is a no-op.
		if got := NormalizeName(NormalizeName(tt.in)); got != tt.want {
			t.Errorf("NormalizeName not idempotent for %q: %q", tt.in, got)
		}
	}
}
