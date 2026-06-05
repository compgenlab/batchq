package support

import "testing"

// TestExpandPathAbsEmpty guards the regression where ExpandPathAbs("")
// panicked on path[len(path)-1]. An empty path must return empty, no error.
func TestExpandPathAbsEmpty(t *testing.T) {
	got, err := ExpandPathAbs("")
	if err != nil {
		t.Fatalf("ExpandPathAbs(\"\"): unexpected error %v", err)
	}
	if got != "" {
		t.Fatalf("ExpandPathAbs(\"\") = %q, want \"\"", got)
	}
}
