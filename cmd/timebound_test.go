package cmd

import (
	"testing"
	"time"
)

func TestResolveTimeBound(t *testing.T) {
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.Local)

	tests := []struct {
		name    string
		in      string
		want    time.Time
		wantErr bool
	}{
		{name: "age hours", in: "12h", want: now.Add(-12 * time.Hour)},
		{name: "age days", in: "1d", want: now.Add(-24 * time.Hour)},
		{name: "age weeks", in: "1w", want: now.Add(-7 * 24 * time.Hour)},
		{name: "age minutes", in: "90m", want: now.Add(-90 * time.Minute)},
		{name: "age seconds", in: "30s", want: now.Add(-30 * time.Second)},
		{name: "age whitespace", in: "  2d  ", want: now.Add(-48 * time.Hour)},
		{name: "date only", in: "2026-06-30", want: time.Date(2026, 6, 30, 0, 0, 0, 0, time.Local)},
		{name: "datetime", in: "2026-06-30 15:04:05", want: time.Date(2026, 6, 30, 15, 4, 5, 0, time.Local)},
		{name: "rfc3339 utc", in: "2026-06-30T15:04:05Z", want: time.Date(2026, 6, 30, 15, 4, 5, 0, time.UTC)},
		{name: "empty", in: "", wantErr: true},
		{name: "garbage", in: "not-a-time", wantErr: true},
		{name: "bare number", in: "20260630", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveTimeBound(tc.in, now)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("resolveTimeBound(%q) = %v, want error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveTimeBound(%q) unexpected error: %v", tc.in, err)
			}
			if !got.Equal(tc.want) {
				t.Errorf("resolveTimeBound(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}
