package support

import "testing"

func TestSplitTaskAddr(t *testing.T) {
	cases := []struct {
		in      string
		arrayID string
		index   string
		ok      bool
	}{
		{"6ac96846-575a-43da-a774-cf840d5069fc_1", "6ac96846-575a-43da-a774-cf840d5069fc", "1", true},
		{"6ac96846-575a-43da-a774-cf840d5069fc_24", "6ac96846-575a-43da-a774-cf840d5069fc", "24", true},
		{"6ac96846-575a-43da-a774-cf840d5069fc", "", "", false}, // plain id, no underscore
		{"abc_", "", "", false},    // trailing underscore
		{"_1", "", "", false},      // leading underscore
		{"abc_def", "", "", false}, // non-numeric suffix
		{"", "", "", false},        // empty
	}
	for _, c := range cases {
		gotID, gotIdx, gotOk := SplitTaskAddr(c.in)
		if gotOk != c.ok || gotID != c.arrayID || gotIdx != c.index {
			t.Errorf("SplitTaskAddr(%q) = (%q, %q, %v), want (%q, %q, %v)",
				c.in, gotID, gotIdx, gotOk, c.arrayID, c.index, c.ok)
		}
	}
}
