package runner

import (
	"encoding/json"
	"strings"
	"testing"
)

// The --comment directive must carry a JSON back-reference that a rescue can
// read from the SLURM job record (scontrol/sacct Comment) and parse back to the
// batchq id.
func TestSlurmCommentDirective(t *testing.T) {
	line := slurmCommentDirective(map[string]string{"batchq_id": "abc-123-uuid"})
	want := `#SBATCH --comment="{\"batchq_id\":\"abc-123-uuid\"}"` + "\n"
	if line != want {
		t.Fatalf("directive = %q, want %q", line, want)
	}

	// Round-trip: SLURM stores the Comment as the unescaped value; a rescue
	// unmarshals it to recover the id.
	val := strings.TrimSuffix(strings.TrimPrefix(line, `#SBATCH --comment="`), "\"\n")
	unescaped := strings.ReplaceAll(val, `\"`, `"`)
	var m map[string]string
	if err := json.Unmarshal([]byte(unescaped), &m); err != nil {
		t.Fatalf("comment value is not valid JSON: %v (%q)", err, unescaped)
	}
	if m["batchq_id"] != "abc-123-uuid" {
		t.Fatalf("batchq_id = %q, want abc-123-uuid", m["batchq_id"])
	}
}

func TestSlurmCommentDirectiveArray(t *testing.T) {
	line := slurmCommentDirective(map[string]string{"batchq_array_id": "arr-9"})
	if !strings.Contains(line, `--comment="{\"batchq_array_id\":\"arr-9\"}"`) {
		t.Fatalf("array comment directive = %q", line)
	}
}
