package runner

import (
	"strings"
	"testing"
)

// TestSlurmJobName guards against unsafe `#SBATCH -J` values: a job name with
// whitespace ("Count reads per sample") made sbatch parse the second word
// ("reads") as a new directive and reject the script with "Invalid directive
// found in batch script: reads". SLURM directives aren't quoted, so the name
// is sanitized to a bare token — every run of characters outside
// [a-zA-Z0-9-_] is collapsed to a single underscore.
func TestSlurmJobName(t *testing.T) {
	tests := []struct {
		name string
		id   string
		jobN string
		want string
	}{
		{
			"spaces and parens collapse to underscores",
			"4e6db5cc-d15d-4f4a-bfb0-c6182f97f8c4",
			"Count reads per sample (extended walltime)",
			"bq-4e6db5cc-d15d-4f4a-bfb0-c6182f97f8c4.Count_reads_per_sample_extended_walltime_",
		},
		{
			"runs of unsafe chars collapse to one underscore",
			"abc",
			"my   bad  job!!",
			"bq-abc.my_bad_job_",
		},
		{
			"quotes and newlines become underscores",
			"abc",
			"line1\nline2\"x\"",
			"bq-abc.line1_line2_x_",
		},
		{
			"already-safe name is unchanged",
			"abc",
			"build-step_1",
			"bq-abc.build-step_1",
		},
	}
	for _, tt := range tests {
		got := slurmJobName(tt.id, tt.jobN)
		if got != tt.want {
			t.Errorf("%s: slurmJobName(%q, %q) = %q, want %q", tt.name, tt.id, tt.jobN, got, tt.want)
		}
		// The result must be a single bare token: no whitespace anywhere.
		if strings.ContainsAny(got, " \t\n\r") {
			t.Errorf("%s: result contains whitespace: %q", tt.name, got)
		}
	}
}
