package jobs

import (
	"reflect"
	"testing"
)

func TestParseArraySpec(t *testing.T) {
	cases := []struct {
		in       string
		indices  []int
		throttle int
		wantErr  bool
	}{
		{in: "0-3", indices: []int{0, 1, 2, 3}},
		{in: "5", indices: []int{5}},
		{in: "1-10:2", indices: []int{1, 3, 5, 7, 9}},
		{in: "1,3,5-7", indices: []int{1, 3, 5, 6, 7}},
		{in: "0-99%4", indices: rangeInts(0, 99), throttle: 4},
		{in: " 2 , 1 , 1 ", indices: []int{1, 2}}, // dedup + sort + trim
		{in: "10-12 % 3", indices: []int{10, 11, 12}, throttle: 3},
		{in: "", wantErr: true},
		{in: "5-1", wantErr: true},
		{in: "-1-3", wantErr: true},
		{in: "abc", wantErr: true},
		{in: "0-9%0", wantErr: true},
		{in: "0-9:0", wantErr: true},
		{in: "1,,2", wantErr: true},
		{in: "0-1000000", wantErr: true}, // exceeds maxArrayTasks
	}
	for _, tc := range cases {
		got, err := ParseArraySpec(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ParseArraySpec(%q): expected error, got %+v", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseArraySpec(%q): unexpected error: %v", tc.in, err)
			continue
		}
		if !reflect.DeepEqual(got.Indices, tc.indices) {
			t.Errorf("ParseArraySpec(%q).Indices = %v, want %v", tc.in, got.Indices, tc.indices)
		}
		if got.Throttle != tc.throttle {
			t.Errorf("ParseArraySpec(%q).Throttle = %d, want %d", tc.in, got.Throttle, tc.throttle)
		}
	}
}

func rangeInts(lo, hi int) []int {
	var out []int
	for i := lo; i <= hi; i++ {
		out = append(out, i)
	}
	return out
}
