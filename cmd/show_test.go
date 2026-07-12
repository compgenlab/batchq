package cmd

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/compgenlab/batchq/api"
	"github.com/compgenlab/batchq/jobs"
)

func TestAggregateArrayStatus(t *testing.T) {
	dtos := func(statuses ...jobs.StatusCode) []*api.JobDTO {
		out := make([]*api.JobDTO, 0, len(statuses))
		for _, s := range statuses {
			out = append(out, &api.JobDTO{Status: s.String()})
		}
		return out
	}
	cases := []struct {
		name    string
		members []*api.JobDTO
		want    jobs.StatusCode
	}{
		{"any running wins", dtos(jobs.SUCCESS, jobs.RUNNING, jobs.PROXYQUEUED), jobs.RUNNING},
		{"proxyqueued over queued", dtos(jobs.QUEUED, jobs.PROXYQUEUED, jobs.SUCCESS), jobs.PROXYQUEUED},
		{"all done success", dtos(jobs.SUCCESS, jobs.SUCCESS), jobs.SUCCESS},
		{"any failed once terminal", dtos(jobs.SUCCESS, jobs.FAILED), jobs.FAILED},
		{"canceled when no failure", dtos(jobs.SUCCESS, jobs.CANCELED), jobs.CANCELED},
		{"failed beats canceled", dtos(jobs.FAILED, jobs.CANCELED), jobs.FAILED},
	}
	for _, c := range cases {
		if got := aggregateArrayStatus(c.members); got != c.want.String() {
			t.Errorf("%s: aggregateArrayStatus = %q, want %q", c.name, got, c.want)
		}
	}
}

// TestEmitJSON seeds a job through its full lifecycle (submit → claim → end) so
// it carries times, a return code, and running-detail keys, then asserts the
// --json emit round-trips into []api.JobDTO with those fields intact and that an
// unknown id is omitted rather than erroring.
func TestEmitJSON(t *testing.T) {
	c := startCompatServer(t)
	ctx := context.Background()

	// Seed a job and drive it to a terminal state.
	dto, err := c.SubmitJob(ctx, &api.SubmitJobRequest{
		Name:    "align.bam",
		Details: map[string]string{"script": "echo hi", "procs": "8", "mem": "16G"},
	})
	if err != nil {
		t.Fatalf("SubmitJob: %v", err)
	}
	jobID := dto.JobID

	const runnerID = "runner-test"
	claim, err := c.ClaimNextJob(ctx, runnerID, "simple", "gpu-04", 0, 0, 0, nil)
	if err != nil {
		t.Fatalf("ClaimNextJob: %v", err)
	}
	if claim.Job == nil || claim.Job.JobID != jobID {
		t.Fatalf("ClaimNextJob did not claim the seeded job: %+v", claim)
	}
	if err := c.UpdateRunningDetails(ctx, runnerID, jobID, map[string]string{"slurm_status": "COMPLETED"}); err != nil {
		t.Fatalf("UpdateRunningDetails: %v", err)
	}
	if err := c.EndJob(ctx, runnerID, jobID, 0, ""); err != nil {
		t.Fatalf("EndJob: %v", err)
	}

	// Query the real id plus an unknown one; the unknown should be omitted.
	out := capture(t, func() {
		emitJSON(c, []string{jobID, "does-not-exist"})
	})

	var got []api.JobDTO
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("unmarshal emitJSON output: %v\noutput was:\n%s", err, out)
	}
	if len(got) != 1 {
		t.Fatalf("got %d jobs, want 1 (unknown id should be omitted); output:\n%s", len(got), out)
	}

	j := got[0]
	if j.JobID != jobID {
		t.Errorf("job_id = %q, want %q", j.JobID, jobID)
	}
	if j.Status != jobs.SUCCESS.String() {
		t.Errorf("status = %q, want %q", j.Status, jobs.SUCCESS.String())
	}
	if j.ReturnCode != 0 {
		t.Errorf("return_code = %d, want 0", j.ReturnCode)
	}
	if j.SubmitTime == nil || j.StartTime == nil || j.EndTime == nil {
		t.Fatalf("expected submit/start/end times to be set, got submit=%v start=%v end=%v",
			j.SubmitTime, j.StartTime, j.EndTime)
	}
	if loc := j.EndTime.Location(); loc != time.UTC {
		t.Errorf("end_time location = %v, want UTC", loc)
	}
	if j.RunningDetails["slurm_status"] != "COMPLETED" {
		t.Errorf("running_details[slurm_status] = %q, want COMPLETED", j.RunningDetails["slurm_status"])
	}
	if j.RunningDetails["host"] != "gpu-04" {
		t.Errorf("running_details[host] = %q, want gpu-04", j.RunningDetails["host"])
	}
}
