package support

import (
	"strconv"
	"strings"
)

// SplitTaskAddr parses a SLURM-style task address "<array_id>_<index>" into its
// array id and index. Job/array ids are UUIDs (no underscores), so the last '_'
// unambiguously separates the array id from the integer index. Returns ok=false
// when arg is not a task address (a plain job/array id).
func SplitTaskAddr(arg string) (arrayID, index string, ok bool) {
	i := strings.LastIndex(arg, "_")
	if i <= 0 || i == len(arg)-1 {
		return "", "", false
	}
	suffix := arg[i+1:]
	if _, err := strconv.Atoi(suffix); err != nil {
		return "", "", false
	}
	return arg[:i], suffix, true
}
