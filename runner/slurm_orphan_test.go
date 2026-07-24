package runner

import (
	"testing"
	"time"

	"github.com/compgenlab/batchq/api"
)

// isOrphanedClaim classifies a RUNNING job as recoverable by the recovery pass.
// A job already carrying a slurm id, or too recently claimed, is never an
// orphan. Attribution to this runner is by matching "host" (--slurm-recover-
// orphans) or, for a job with no host, by the --slurm-recover-hostless opt-in.
func TestIsOrphanedClaim(t *testing.T) {
	old := time.Now().Add(-2 * orphanRecoveryMinAge).UTC()
	recent := time.Now().UTC()

	dto := func(host string, start *time.Time, extra map[string]string) *api.JobDTO {
		rd := map[string]string{}
		if host != "" {
			rd["host"] = host
		}
		for k, v := range extra {
			rd[k] = v
		}
		return &api.JobDTO{RunningDetails: rd, StartTime: start}
	}

	cases := []struct {
		name            string
		runnerHost      string
		recoverOrphans  bool
		recoverHostless bool
		job             *api.JobDTO
		want            bool
	}{
		{
			name:           "host-keyed: our host, aged, no slurm id",
			runnerHost:     "node1",
			recoverOrphans: true,
			job:            dto("node1", &old, nil),
			want:           true,
		},
		{
			name:           "host-keyed: too recent is left alone",
			runnerHost:     "node1",
			recoverOrphans: true,
			job:            dto("node1", &recent, nil),
			want:           false,
		},
		{
			name:           "host-keyed: another host's claim is not ours",
			runnerHost:     "node1",
			recoverOrphans: true,
			job:            dto("node2", &old, nil),
			want:           false,
		},
		{
			name:           "host-keyed alone does NOT touch a hostless orphan",
			runnerHost:     "node1",
			recoverOrphans: true,
			job:            dto("", &old, nil),
			want:           false,
		},
		{
			name:            "hostless: aged orphan with no host is recovered",
			runnerHost:      "", // the old-binary case: runner never recorded a host either
			recoverHostless: true,
			job:             dto("", &old, nil),
			want:            true,
		},
		{
			name:            "hostless: still respects the age gate",
			runnerHost:      "",
			recoverHostless: true,
			job:             dto("", &recent, nil),
			want:            false,
		},
		{
			name:            "hostless is a superset: still recovers our host-matched orphan",
			runnerHost:      "node1",
			recoverHostless: true,
			job:             dto("node1", &old, nil),
			want:            true,
		},
		{
			name:            "hostless never touches another runner's host-tagged claim",
			runnerHost:      "node1",
			recoverHostless: true,
			job:             dto("node2", &old, nil),
			want:            false,
		},
		{
			name:            "already handed to SLURM is never an orphan",
			runnerHost:      "node1",
			recoverHostless: true,
			job:             dto("", &old, map[string]string{"slurm_array_id": "555"}),
			want:            false,
		},
		{
			name:       "recovery disabled: nothing qualifies",
			runnerHost: "node1",
			job:        dto("node1", &old, nil),
			want:       false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &slurmRunner{
				host:            tc.runnerHost,
				recoverOrphans:  tc.recoverOrphans,
				recoverHostless: tc.recoverHostless,
			}
			if got := r.isOrphanedClaim(tc.job); got != tc.want {
				t.Fatalf("isOrphanedClaim = %v, want %v", got, tc.want)
			}
		})
	}
}
