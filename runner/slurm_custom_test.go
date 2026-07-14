package runner

import (
	"strings"
	"testing"

	"github.com/compgenlab/batchq/api"
	"github.com/compgenlab/batchq/jobs"
)

// TestSlurmResourceDirectivesCustom checks the account/partition passthrough
// precedence in slurmResourceDirectives: a job's own custom.account /
// custom.partition wins over the runner's configured default, the runner default
// is used when the job omits them, and neither set emits no -A/-p line.
func TestSlurmResourceDirectivesCustom(t *testing.T) {
	line := func(src, flag string) string {
		for _, l := range strings.Split(src, "\n") {
			if strings.HasPrefix(l, "#SBATCH "+flag+" ") {
				return strings.TrimSpace(strings.TrimPrefix(l, "#SBATCH "+flag+" "))
			}
		}
		return ""
	}

	tests := []struct {
		name          string
		runnerAcct    string
		runnerPart    string
		details       map[string]string
		wantAccount   string
		wantPartition string
	}{
		{
			name:          "job overrides runner default",
			runnerAcct:    "runner-acct",
			runnerPart:    "runner-part",
			details:       map[string]string{jobs.CustomPrefix + "account": "job-acct", jobs.CustomPrefix + "partition": "job-part"},
			wantAccount:   "job-acct",
			wantPartition: "job-part",
		},
		{
			name:          "runner default used when job omits",
			runnerAcct:    "runner-acct",
			runnerPart:    "runner-part",
			details:       map[string]string{},
			wantAccount:   "runner-acct",
			wantPartition: "runner-part",
		},
		{
			name:          "neither set emits nothing",
			details:       map[string]string{},
			wantAccount:   "",
			wantPartition: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &slurmRunner{account: tt.runnerAcct, partition: tt.runnerPart}
			job := &api.JobDTO{Details: tt.details}
			src := r.slurmResourceDirectives(job)
			if got := line(src, "-A"); got != tt.wantAccount {
				t.Errorf("account: got %q, want %q\n%s", got, tt.wantAccount, src)
			}
			if got := line(src, "-p"); got != tt.wantPartition {
				t.Errorf("partition: got %q, want %q\n%s", got, tt.wantPartition, src)
			}
		})
	}
}
