package runner

import "testing"

// TestCountSqueueJobs guards the PR #30 panic fix: blank and short lines in
// squeue output must be tolerated (HasPrefix, not a fixed-width slice), and
// only the header line is excluded from the job count.
func TestCountSqueueJobs(t *testing.T) {
	tests := []struct {
		name string
		out  string
		want int
	}{
		{"empty", "", 0},
		{"header only", "JOBID PARTITION NAME USER ST TIME NODES NODELIST\n", 0},
		{
			"two jobs",
			"JOBID PARTITION NAME USER ST TIME NODES NODELIST\n" +
				"123 debug job1 alice R 0:10 1 node1\n" +
				"124 debug job2 alice PD 0:00 1 (Resources)\n",
			2,
		},
		{
			"blank and short lines tolerated",
			"\nJOBID ...\n\n12 a b c\n   \nx\n",
			2, // "12 a b c" and "x" count; blank/whitespace lines skipped
		},
	}
	for _, tt := range tests {
		got, err := countSqueueJobs([]byte(tt.out))
		if err != nil {
			t.Errorf("%s: unexpected error %v", tt.name, err)
			continue
		}
		if got != tt.want {
			t.Errorf("%s: countSqueueJobs = %d, want %d", tt.name, got, tt.want)
		}
	}
}
