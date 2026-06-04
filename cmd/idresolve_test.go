package cmd

import "testing"

func TestSplitTaskAddr(t *testing.T) {
	const uuid = "a1b2c3d4-e5f6-4789-abcd-0123456789ab"
	cases := []struct {
		in        string
		wantArr   string
		wantIndex string
		wantOK    bool
	}{
		{uuid + "_3", uuid, "3", true},
		{uuid + "_0", uuid, "0", true},
		{uuid, "", "", false},          // a plain UUID has no underscore
		{uuid + "_x", "", "", false},   // non-integer suffix
		{uuid + "_", "", "", false},    // empty suffix
		{"_3", "", "", false},          // empty prefix
		{"arr_1_5", "arr_1", "5", true}, // split on the LAST underscore
	}
	for _, tc := range cases {
		arr, idx, ok := splitTaskAddr(tc.in)
		if ok != tc.wantOK || arr != tc.wantArr || idx != tc.wantIndex {
			t.Errorf("splitTaskAddr(%q) = (%q,%q,%v), want (%q,%q,%v)",
				tc.in, arr, idx, ok, tc.wantArr, tc.wantIndex, tc.wantOK)
		}
	}
}
