package runner

import "testing"

// clampMinArray bounds the min-array gate to the smaller of the set job caps so
// a minimum larger than any achievable per-pass batch can't defer an array
// forever. Caps of -1 mean "unset".
func TestClampMinArray(t *testing.T) {
	cases := []struct {
		name                           string
		minArray, maxUserJobs, maxJobs int
		wantMin                        int
		wantClamped                    bool
	}{
		{"disabled", -1, 500, 100, -1, false},
		{"under both caps", 50, 500, 100, 50, false},
		{"over the smaller cap (maxJobs)", 200, 500, 100, 100, true},
		{"over the smaller cap (maxUserJobs)", 200, 100, 500, 100, true},
		{"equal to cap is not clamped", 100, 100, -1, 100, false},
		{"no caps set: unbounded, never clamps", 1000, -1, -1, 1000, false},
		{"only maxUserJobs set", 300, 250, -1, 250, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _, clamped := clampMinArray(tc.minArray, tc.maxUserJobs, tc.maxJobs)
			if got != tc.wantMin || clamped != tc.wantClamped {
				t.Fatalf("clampMinArray(%d, %d, %d) = (min=%d, clamped=%v), want (min=%d, clamped=%v)",
					tc.minArray, tc.maxUserJobs, tc.maxJobs, got, clamped, tc.wantMin, tc.wantClamped)
			}
		})
	}
}
