package api

import (
	"testing"
	"time"

	"github.com/mbreese/batchq/jobs"
)

// TestParseStatusRoundTrip asserts every StatusCode's String() name parses back
// to the same code, so the wire vocabulary can't drift from jobs.StatusCode.
func TestParseStatusRoundTrip(t *testing.T) {
	codes := []jobs.StatusCode{
		jobs.UNKNOWN, jobs.USERHOLD, jobs.WAITING, jobs.QUEUED,
		jobs.PROXYQUEUED, jobs.RUNNING, jobs.CANCELED, jobs.SUCCESS, jobs.FAILED,
	}
	for _, c := range codes {
		got, err := ParseStatus(c.String())
		if err != nil {
			t.Errorf("ParseStatus(%q): unexpected error %v", c.String(), err)
			continue
		}
		if got != c {
			t.Errorf("ParseStatus(%q) = %v, want %v", c.String(), got, c)
		}
	}
}

func TestParseStatusUnknown(t *testing.T) {
	if _, err := ParseStatus("NOPE"); err == nil {
		t.Error("ParseStatus(\"NOPE\"): expected error, got nil")
	}
}

// TestJobDefDTORoundTrip asserts JobFromDef -> JobToDef preserves the fields
// that cross the wire. Status is intentionally dropped by JobToDef (the server
// recomputes it), so it is checked only on the outbound DTO.
func TestJobDefDTORoundTrip(t *testing.T) {
	submit := time.Date(2026, 6, 5, 10, 0, 0, 0, time.UTC)
	start := time.Date(2026, 6, 5, 10, 1, 0, 0, time.UTC)
	end := time.Date(2026, 6, 5, 10, 2, 0, 0, time.UTC)

	orig := &jobs.JobDef{
		JobId:      "job-1",
		Status:     jobs.RUNNING,
		Priority:   7,
		Name:       "myjob",
		Notes:      "a note",
		SubmitTime: submit,
		StartTime:  start,
		EndTime:    end,
		ReturnCode: 3,
		AfterOk:    []string{"dep-1", "dep-2"},
		Details: []jobs.JobDefDetail{
			{Key: "procs", Value: "4"},
			{Key: "mem", Value: "8000"},
			{Key: jobs.ResourcePrefix + "gpu", Value: "2"},
		},
		RunningDetails: []jobs.JobRunningDetail{
			{Key: "host", Value: "node1"},
		},
		InputFiles:  []string{"/in/a"},
		OutputFiles: []string{"/out/b"},
	}

	dto := JobFromDef(orig)
	if dto.Status != "RUNNING" {
		t.Errorf("dto.Status = %q, want RUNNING", dto.Status)
	}

	back := JobToDef(dto)

	if back.JobId != orig.JobId || back.Priority != orig.Priority ||
		back.Name != orig.Name || back.Notes != orig.Notes || back.ReturnCode != orig.ReturnCode {
		t.Errorf("scalar fields not preserved: %+v", back)
	}
	if !back.SubmitTime.Equal(submit) || !back.StartTime.Equal(start) || !back.EndTime.Equal(end) {
		t.Errorf("times not preserved: submit=%v start=%v end=%v", back.SubmitTime, back.StartTime, back.EndTime)
	}
	if !equalStrings(back.AfterOk, orig.AfterOk) {
		t.Errorf("AfterOk = %v, want %v", back.AfterOk, orig.AfterOk)
	}
	if !equalStrings(back.InputFiles, orig.InputFiles) {
		t.Errorf("InputFiles = %v, want %v", back.InputFiles, orig.InputFiles)
	}
	if !equalStrings(back.OutputFiles, orig.OutputFiles) {
		t.Errorf("OutputFiles = %v, want %v", back.OutputFiles, orig.OutputFiles)
	}

	// Details survive the list<->map<->list trip; order is not guaranteed, so
	// compare via lookup.
	for _, d := range orig.Details {
		if got := back.GetDetail(d.Key, "\x00missing"); got != d.Value {
			t.Errorf("detail %q = %q, want %q", d.Key, got, d.Value)
		}
	}
	if got := back.GetRunningDetail("host", ""); got != "node1" {
		t.Errorf("running detail host = %q, want node1", got)
	}
}

func TestJobFromDefNil(t *testing.T) {
	if JobFromDef(nil) != nil {
		t.Error("JobFromDef(nil) should be nil")
	}
	if JobToDef(nil) != nil {
		t.Error("JobToDef(nil) should be nil")
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
