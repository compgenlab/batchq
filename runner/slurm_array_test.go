package runner

import "testing"

// parseSacctArrayOutput must map only the real per-task rows, skipping the
// header, step rows, and the compressed pending placeholder.
func TestParseSacctArrayOutput(t *testing.T) {
	out := []byte(`JobID|State|Start|End|ExitCode|NodeList
12345_0|COMPLETED|2026-06-04T00:00:00|2026-06-04T00:01:00|0:0|node1
12345_0.batch|COMPLETED|2026-06-04T00:00:00|2026-06-04T00:01:00|0:0|node1
12345_0.extern|COMPLETED|2026-06-04T00:00:00|2026-06-04T00:01:00|0:0|node1
12345_1|FAILED|2026-06-04T00:00:00|2026-06-04T00:02:00|1:0|node2
12345_2|RUNNING|2026-06-04T00:00:00|Unknown|0:0|node3
12345_[3-9]|PENDING|Unknown|Unknown|0:0|None assigned
99999_0|COMPLETED|2026-06-04T00:00:00|2026-06-04T00:01:00|0:0|nodeX
`)
	states := parseSacctArrayOutput(out, 12345)

	if len(states) != 3 {
		t.Fatalf("expected 3 task states, got %d: %+v", len(states), states)
	}
	if s, ok := states[0]; !ok || s.State != "COMPLETED" || s.ExitCodeInt() != 0 {
		t.Fatalf("task 0 = %+v (ok=%v)", s, ok)
	}
	if s, ok := states[1]; !ok || s.State != "FAILED" || s.ExitCodeInt() != 1 {
		t.Fatalf("task 1 = %+v (ok=%v)", s, ok)
	}
	if s, ok := states[2]; !ok || s.State != "RUNNING" {
		t.Fatalf("task 2 = %+v (ok=%v)", s, ok)
	}
	// The pending placeholder must NOT have produced entries for 3..9.
	if _, ok := states[3]; ok {
		t.Fatalf("pending placeholder 12345_[3-9] should not yield task states")
	}
	// A different array id must be ignored.
	if _, ok := states[0]; ok && states[0].JobId == "99999_0" {
		t.Fatalf("row for a different array leaked in")
	}
}
