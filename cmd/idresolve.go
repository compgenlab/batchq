package cmd

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"sort"
	"strconv"
	"strings"

	"github.com/mbreese/batchq/api"
	"github.com/mbreese/batchq/client"
)

// jobTarget is what a CLI id argument resolves to: either a single job/task or
// a whole array.
type jobTarget struct {
	isArray bool
	arrayID string        // set when isArray
	members []*api.JobDTO // all member DTOs, when isArray
	jobID   string        // set when !isArray
	dto     *api.JobDTO   // the job/task DTO, when !isArray
}

// splitTaskAddr parses a SLURM-style task address "<array_id>_<index>" into its
// array id and index. Job/array ids are UUIDs (no underscores), so the last '_'
// unambiguously separates the array id from the integer index.
func splitTaskAddr(arg string) (arrayID, index string, ok bool) {
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

// resolveTarget interprets a CLI id argument:
//   - "<array_id>_<index>" -> that single task
//   - a job id            -> that job
//   - an array id         -> the whole array (all member tasks)
func resolveTarget(ctx context.Context, c *client.Client, arg string) (*jobTarget, error) {
	if prefix, idx, ok := splitTaskAddr(arg); ok {
		members, err := c.ListJobs(ctx, client.ListJobsOptions{ArrayID: prefix, ShowAll: true})
		if err != nil {
			return nil, err
		}
		for _, m := range members {
			if m.Details["array_index"] == idx {
				return &jobTarget{jobID: m.JobID, dto: m}, nil
			}
		}
		return nil, fmt.Errorf("no array task %s", arg)
	}

	dto, err := c.GetJob(ctx, arg)
	if err == nil {
		return &jobTarget{jobID: arg, dto: dto}, nil
	}
	if !errors.Is(err, client.ErrNotFound) {
		return nil, err
	}

	// Not a job id — maybe it's an array id.
	members, err := c.ListJobs(ctx, client.ListJobsOptions{ArrayID: arg, ShowAll: true})
	if err != nil {
		return nil, err
	}
	if len(members) > 0 {
		return &jobTarget{isArray: true, arrayID: arg, members: members}, nil
	}
	return nil, fmt.Errorf("no such job or array: %s", arg)
}

// scancelJob cancels the SLURM side of one job/task, if any. An array task uses
// "<slurm_array_id>_<index>"; a plain job uses "<slurm_job_id>".
func scancelJob(dto *api.JobDTO) {
	if dto == nil {
		return
	}
	rd := dto.RunningDetails
	var arg string
	switch {
	case rd["slurm_array_id"] != "" && rd["slurm_task_index"] != "":
		arg = rd["slurm_array_id"] + "_" + rd["slurm_task_index"]
	case rd["slurm_array_id"] != "":
		arg = rd["slurm_array_id"]
	case rd["slurm_job_id"] != "":
		arg = rd["slurm_job_id"]
	}
	if arg == "" {
		return
	}
	if err := exec.Command("scancel", arg).Run(); err != nil {
		fmt.Printf("Error canceling slurm job %s: %v\n", arg, err)
	} else {
		fmt.Printf("Canceled slurm job: %s\n", arg)
	}
}

// scancelArray cancels the live SLURM array(s) backing an array's tasks, once
// per distinct slurm_array_id.
func scancelArray(members []*api.JobDTO) {
	seen := map[string]bool{}
	for _, m := range members {
		aid := m.RunningDetails["slurm_array_id"]
		if aid == "" || seen[aid] {
			continue
		}
		seen[aid] = true
		if err := exec.Command("scancel", aid).Run(); err != nil {
			fmt.Printf("Error canceling slurm array %s: %v\n", aid, err)
		} else {
			fmt.Printf("Canceled slurm array: %s\n", aid)
		}
	}
}

// printArraySummary prints a per-status rollup of an array's tasks. When verbose
// (used by `details`), it also lists each task's index and status.
func printArraySummary(arrayID string, members []*api.JobDTO, verbose bool) {
	counts := map[string]int{}
	done := 0
	for _, m := range members {
		counts[m.Status]++
		switch m.Status {
		case "SUCCESS", "FAILED", "CANCELED":
			done++
		}
	}
	fmt.Printf("array    : %s\n", arrayID)
	fmt.Printf("tasks    : %d (%d done)\n", len(members), done)
	order := []string{"USERHOLD", "WAITING", "QUEUED", "PROXYQUEUED", "RUNNING", "SUCCESS", "FAILED", "CANCELED"}
	parts := make([]string, 0, len(order))
	for _, st := range order {
		if counts[st] > 0 {
			parts = append(parts, fmt.Sprintf("%s %d", st, counts[st]))
		}
	}
	fmt.Printf("status   : %s\n", strings.Join(parts, " · "))

	if verbose {
		sorted := append([]*api.JobDTO{}, members...)
		sort.Slice(sorted, func(i, j int) bool {
			return arrayIndexOf(sorted[i]) < arrayIndexOf(sorted[j])
		})
		fmt.Printf("---[tasks]---\n")
		for _, m := range sorted {
			fmt.Printf("  %s_%s  %-11s %s\n", arrayID, m.Details["array_index"], m.Status, m.JobID)
		}
	}
	fmt.Println("")
}

func arrayIndexOf(dto *api.JobDTO) int {
	n, _ := strconv.Atoi(dto.Details["array_index"])
	return n
}

// forEachTarget resolves each arg (after numeric-range expansion) and invokes fn
// with the resolved target and a per-call context. Resolution errors are logged
// and the arg skipped.
func forEachTarget(c *client.Client, args []string, fn func(ctx context.Context, t *jobTarget)) {
	ids, _ := expandJobArgs(args)
	for _, arg := range ids {
		ctx, cancel := cmdContext()
		target, err := resolveTarget(ctx, c, arg)
		if err != nil {
			cancel()
			fmt.Printf("Error: %v\n", err)
			continue
		}
		fn(ctx, target)
		cancel()
	}
}
