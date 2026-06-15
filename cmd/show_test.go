package cmd

import (
	"testing"

	"github.com/mbreese/batchq/api"
	"github.com/mbreese/batchq/jobs"
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
