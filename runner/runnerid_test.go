package runner

import "testing"

// SetRunnerID overrides the generated id, but an empty value is ignored so the
// runner keeps a usable default.
func TestSetRunnerID(t *testing.T) {
	r := NewSimpleRunner(nil)
	def := r.runnerId
	if def == "" {
		t.Fatal("expected a generated default runner id")
	}
	r.SetRunnerID("")
	if r.runnerId != def {
		t.Fatalf("empty SetRunnerID should be ignored; got %q", r.runnerId)
	}
	r.SetRunnerID("node01")
	if r.runnerId != "node01" {
		t.Fatalf("runner id = %q, want node01", r.runnerId)
	}

	s := NewSlurmRunner(nil)
	s.SetRunnerID("slurm-head")
	if s.runnerId != "slurm-head" {
		t.Fatalf("slurm runner id = %q, want slurm-head", s.runnerId)
	}
}
