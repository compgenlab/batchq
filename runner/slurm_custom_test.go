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

// longDirective returns the value of a `#SBATCH --<flag>=<value>` line, or "".
func longDirective(src, flag string) string {
	for _, l := range strings.Split(src, "\n") {
		if v, ok := strings.CutPrefix(l, "#SBATCH --"+flag+"="); ok {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// TestSlurmResourceRequests covers translation of generic resource.* details
// into --gres / --constraint / --nodelist, including the value-type inference
// default and the allowlist/denylist/constraint/nodelist config overrides.
func TestSlurmResourceRequests(t *testing.T) {
	tests := []struct {
		name           string
		mapping        func(*slurmRunner)
		details        map[string]string
		wantGres       string
		wantConstraint string
		wantNodelist   string
	}{
		{
			name: "infer: integer->gres, empty->constraint, label skipped",
			details: map[string]string{
				jobs.ResourcePrefix + "gpu":     "2",
				jobs.ResourcePrefix + "avx512":  "",
				jobs.ResourcePrefix + "cluster": "xyz",
				jobs.ResourcePrefix + "host":    "node01",
			},
			wantGres:       "gpu:2",
			wantConstraint: "avx512",
			wantNodelist:   "",
		},
		{
			name:     "typed gres keeps its type and combines, sorted",
			details:  map[string]string{jobs.ResourcePrefix + "gpu:a100": "2", jobs.ResourcePrefix + "mps": "50"},
			wantGres: "gpu:a100:2,mps:50",
		},
		{
			name:     "gres allowlist restricts which countables become gres",
			mapping:  func(r *slurmRunner) { r.gresAllow = map[string]bool{"gpu": true} },
			details:  map[string]string{jobs.ResourcePrefix + "gpu": "2", jobs.ResourcePrefix + "slots": "1"},
			wantGres: "gpu:2", // slots is integer but not allowed -> routing only
		},
		{
			name:     "gres denylist excludes a countable",
			mapping:  func(r *slurmRunner) { r.gresExclude = map[string]bool{"slots": true} },
			details:  map[string]string{jobs.ResourcePrefix + "gpu": "2", jobs.ResourcePrefix + "slots": "1"},
			wantGres: "gpu:2",
		},
		{
			name:           "constraint_resources emits a label's value as a feature",
			mapping:        func(r *slurmRunner) { r.constraintNames = map[string]bool{"arch": true} },
			details:        map[string]string{jobs.ResourcePrefix + "arch": "haswell"},
			wantConstraint: "haswell",
		},
		{
			name:         "nodelist_resources emits host as --nodelist",
			mapping:      func(r *slurmRunner) { r.nodelistNames = map[string]bool{"host": true} },
			details:      map[string]string{jobs.ResourcePrefix + "host": "node01"},
			wantNodelist: "node01",
		},
		{
			name:    "no resources emits nothing",
			details: map[string]string{"procs": "4"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &slurmRunner{}
			if tt.mapping != nil {
				tt.mapping(r)
			}
			src := r.slurmResourceRequests(&api.JobDTO{Details: tt.details})
			if got := longDirective(src, "gres"); got != tt.wantGres {
				t.Errorf("gres: got %q, want %q\n%s", got, tt.wantGres, src)
			}
			if got := longDirective(src, "constraint"); got != tt.wantConstraint {
				t.Errorf("constraint: got %q, want %q\n%s", got, tt.wantConstraint, src)
			}
			if got := longDirective(src, "nodelist"); got != tt.wantNodelist {
				t.Errorf("nodelist: got %q, want %q\n%s", got, tt.wantNodelist, src)
			}
		})
	}
}
