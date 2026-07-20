package cmd

import (
	"reflect"
	"testing"

	"github.com/compgenlab/batchq/api"
	"github.com/compgenlab/batchq/jobs"
)

func TestNormalizeStates(t *testing.T) {
	// Case-insensitive, trims whitespace, drops blanks (comma-splitting is done
	// by cobra's StringSlice before we see it).
	got, err := normalizeStates([]string{"running", " Queued ", "", "PROXYQUEUED"})
	if err != nil {
		t.Fatalf("normalizeStates: %v", err)
	}
	want := []string{"RUNNING", "QUEUED", "PROXYQUEUED"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	if got, err := normalizeStates(nil); err != nil || got != nil {
		t.Fatalf("empty input: got %v, %v", got, err)
	}

	if _, err := normalizeStates([]string{"RUNNING", "BOGUS"}); err == nil {
		t.Fatal("expected an error for an unknown state")
	}
}

func TestSlurmDisplayID(t *testing.T) {
	mk := func(kv map[string]string) *jobs.JobDef {
		j := &jobs.JobDef{}
		for k, v := range kv {
			j.RunningDetails = append(j.RunningDetails, jobs.JobRunningDetail{Key: k, Value: v})
		}
		return j
	}
	cases := []struct {
		name string
		rd   map[string]string
		want string
	}{
		{"plain job", map[string]string{"slurm_job_id": "123"}, "123"},
		{"array task", map[string]string{"slurm_array_id": "999", "slurm_task_index": "4"}, "999_4"},
		{"array id only", map[string]string{"slurm_array_id": "999"}, "999"},
		{"job_id wins", map[string]string{"slurm_job_id": "123", "slurm_array_id": "999", "slurm_task_index": "4"}, "123"},
		{"none", nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := slurmDisplayID(mk(tc.rd)); got != tc.want {
				t.Fatalf("slurmDisplayID = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestArrayProgress(t *testing.T) {
	mk := func(s string) *api.JobDTO { return &api.JobDTO{Status: s} }
	members := []*api.JobDTO{mk("RUNNING"), mk("RUNNING"), mk("QUEUED"), mk("SUCCESS")}
	got := arrayProgress(members)
	want := "array 1/4 · QUEUED 1 RUNNING 2 SUCCESS 1"
	if got != want {
		t.Fatalf("arrayProgress = %q, want %q", got, want)
	}
}

// A proxied array shows its SLURM state, not just PROXYQUEUED: effectiveStatus
// surfaces slurm_status for PROXYQUEUED tasks, so a running SLURM array reads as
// RUNNING with a PENDING/RUNNING breakdown.
func TestArrayProgressUsesSlurmStatus(t *testing.T) {
	proxied := func(slurm string) *api.JobDTO {
		return &api.JobDTO{Status: "PROXYQUEUED", RunningDetails: map[string]string{"slurm_status": slurm}}
	}
	members := []*api.JobDTO{proxied("RUNNING"), proxied("RUNNING"), proxied("PENDING"), proxied("PENDING")}

	if got := arrayProgress(members); got != "array 0/4 · PENDING 2 RUNNING 2" {
		t.Fatalf("arrayProgress = %q, want 'array 0/4 · PENDING 2 RUNNING 2'", got)
	}
	if got := arrayDisplayStatus(members); got != "RUNNING" {
		t.Fatalf("arrayDisplayStatus = %q, want RUNNING", got)
	}
}
