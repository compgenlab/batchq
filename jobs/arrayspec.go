package jobs

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// maxArrayTasks bounds how many tasks a single array spec may expand to. It
// guards against a runaway expansion (e.g. "0-100000000") turning one submit
// into millions of rows. SLURM's own MaxArraySize default is 1001; we allow
// considerably more but still cap it.
const maxArrayTasks = 100000

// ArraySpec is a parsed job-array specification: the concrete set of task
// indices plus an optional concurrency throttle.
type ArraySpec struct {
	// Indices are the distinct task indices, ascending. Always non-empty for a
	// successfully parsed spec.
	Indices []int
	// Throttle is the max number of tasks allowed to run at once (the "%N"
	// suffix). Zero means unbounded.
	Throttle int
}

// ParseArraySpec parses a SLURM-compatible array spec such as "0-99", "1-10:2",
// "1,3,5-9", or "0-999%20". The grammar is a comma-separated list of terms,
// each "start", "start-end", or "start-end:step", with an optional trailing
// "%throttle". Indices are de-duplicated and sorted ascending.
func ParseArraySpec(s string) (ArraySpec, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return ArraySpec{}, fmt.Errorf("empty array spec")
	}

	throttle := 0
	if i := strings.LastIndex(s, "%"); i >= 0 {
		thr := strings.TrimSpace(s[i+1:])
		n, err := strconv.Atoi(thr)
		if err != nil || n <= 0 {
			return ArraySpec{}, fmt.Errorf("bad array throttle %q", thr)
		}
		throttle = n
		s = strings.TrimSpace(s[:i])
	}

	seen := map[int]struct{}{}
	var indices []int
	add := func(n int) error {
		if _, ok := seen[n]; ok {
			return nil
		}
		seen[n] = struct{}{}
		indices = append(indices, n)
		if len(indices) > maxArrayTasks {
			return fmt.Errorf("array spec expands to more than %d tasks", maxArrayTasks)
		}
		return nil
	}

	for _, term := range strings.Split(s, ",") {
		term = strings.TrimSpace(term)
		if term == "" {
			return ArraySpec{}, fmt.Errorf("empty term in array spec %q", s)
		}

		step := 1
		if i := strings.Index(term, ":"); i >= 0 {
			st, err := strconv.Atoi(strings.TrimSpace(term[i+1:]))
			if err != nil || st <= 0 {
				return ArraySpec{}, fmt.Errorf("bad array step in %q", term)
			}
			step = st
			term = strings.TrimSpace(term[:i])
		}

		lo, hi, err := parseRange(term)
		if err != nil {
			return ArraySpec{}, err
		}
		for n := lo; n <= hi; n += step {
			if err := add(n); err != nil {
				return ArraySpec{}, err
			}
		}
	}

	if len(indices) == 0 {
		return ArraySpec{}, fmt.Errorf("array spec %q yields no tasks", s)
	}
	sort.Ints(indices)
	return ArraySpec{Indices: indices, Throttle: throttle}, nil
}

// parseRange parses "start" or "start-end" into an inclusive [lo, hi] range.
// Indices must be non-negative and lo <= hi.
func parseRange(term string) (int, int, error) {
	if i := strings.Index(term, "-"); i >= 0 {
		lo, err1 := strconv.Atoi(strings.TrimSpace(term[:i]))
		hi, err2 := strconv.Atoi(strings.TrimSpace(term[i+1:]))
		if err1 != nil || err2 != nil {
			return 0, 0, fmt.Errorf("bad array range %q", term)
		}
		if lo < 0 || hi < lo {
			return 0, 0, fmt.Errorf("bad array range %q", term)
		}
		return lo, hi, nil
	}
	n, err := strconv.Atoi(term)
	if err != nil || n < 0 {
		return 0, 0, fmt.Errorf("bad array index %q", term)
	}
	return n, n, nil
}
