package api

import (
	"fmt"

	"github.com/mbreese/batchq/jobs"
)

// statusByName lets the wire format use the readable status name. Kept in
// one place so the wire vocabulary cannot drift from jobs.StatusCode.
var statusByName = map[string]jobs.StatusCode{
	"UNKNOWN":     jobs.UNKNOWN,
	"USERHOLD":    jobs.USERHOLD,
	"WAITING":     jobs.WAITING,
	"QUEUED":      jobs.QUEUED,
	"PROXYQUEUED": jobs.PROXYQUEUED,
	"RUNNING":     jobs.RUNNING,
	"CANCELED":    jobs.CANCELED,
	"SUCCESS":     jobs.SUCCESS,
	"FAILED":      jobs.FAILED,
}

// ParseStatus turns a wire status string into a StatusCode. Returns an
// error on an unknown name so the server can return 400 instead of
// silently treating it as UNKNOWN.
func ParseStatus(name string) (jobs.StatusCode, error) {
	if s, ok := statusByName[name]; ok {
		return s, nil
	}
	return jobs.UNKNOWN, fmt.Errorf("api: unknown status %q", name)
}

// ParseStatusList turns a slice of wire status strings into StatusCodes.
func ParseStatusList(names []string) ([]jobs.StatusCode, error) {
	out := make([]jobs.StatusCode, 0, len(names))
	for _, n := range names {
		s, err := ParseStatus(n)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}
