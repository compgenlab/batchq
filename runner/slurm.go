package runner

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/mbreese/batchq/api"
	"github.com/mbreese/batchq/client"
	"github.com/mbreese/batchq/jobs"
	"github.com/mbreese/batchq/support"
)

type slurmRunner struct {
	client      *client.Client
	runnerId    string
	maxUserJobs int
	maxJobs     int
	minArray    int
	fullArray   bool
	availJobs   int
	account     string
	username    string
	partition   string
	host        string
	resources   map[string]string
}

type SlurmJobState struct {
	JobId    string
	State    string
	Start    string
	End      string
	ExitCode string
	NodeList string
}

// These aren't UTC times, so we just parse them as local time.

func parseSlurmTime(s string) (time.Time, bool) {
	if strings.ToUpper(s) == "UNKNOWN" || s == "" {
		return time.Time{}, false
	}
	t, err := time.ParseInLocation("2006-01-02T15:04:05", s, time.Local)
	if err != nil {
		return time.Time{}, false
	}
	return t.UTC(), true
}

func (s *SlurmJobState) EndAsTime() time.Time {
	t, _ := parseSlurmTime(s.End)
	return t
}

func (s *SlurmJobState) StartAsTime() time.Time {
	t, _ := parseSlurmTime(s.Start)
	return t
}

func (s *SlurmJobState) ExitCodeInt() int {
	val, err := strconv.Atoi(strings.Split(s.ExitCode, ":")[0])
	if err != nil {
		return -1
	}
	return val
}

/*
Create a new Runner that submits jobs to a SLURM queue. In this way, batchq
operates as a proxy to SLURM. This is helpful when you have a limit on the
number of jobs that can be submitted to the SLURM queue. By using batchq, you
can submit as many jobs as you'd like to your personal queue, and only submit
new jobs to the SLURM queue as your job-count decreases.
*/
func NewSlurmRunner(c *client.Client) *slurmRunner {
	id := support.NewUUID()
	runner := slurmRunner{
		runnerId:    id,
		client:      c,
		maxUserJobs: -1,
		maxJobs:     -1,
		minArray:    -1,
		account:     "",
		username:    "",
	}
	return &runner
}

func (r *slurmRunner) SetSlurmMaxUserJobs(maxUserJobs int) *slurmRunner {
	r.maxUserJobs = maxUserJobs
	return r
}
func (r *slurmRunner) SetMaxJobCount(maxJobs int) *slurmRunner {
	r.maxJobs = maxJobs
	return r
}

// SetSlurmMinArray sets the minimum array batch size: the runner holds back an
// array batch smaller than this (submitting nothing) until enough budget frees
// up to claim at least this many tasks at once, rather than emitting many tiny
// `sbatch --array` submissions. An array's final remainder (fewer than this many
// tasks left in total) is always submitted. <=0 disables the gate.
func (r *slurmRunner) SetSlurmMinArray(minArray int) *slurmRunner {
	r.minArray = minArray
	return r
}

// SetSlurmFullArray enables all-or-nothing array submission: an array is only
// submitted when its entire remaining set of tasks fits the current budget in a
// single pass, otherwise it is deferred. Overrides --slurm-min-array. An array
// larger than the job cap can never fit and will defer indefinitely.
func (r *slurmRunner) SetSlurmFullArray(full bool) *slurmRunner {
	r.fullArray = full
	return r
}
func (r *slurmRunner) SetSlurmAccount(account string) *slurmRunner {
	r.account = account
	return r
}
func (r *slurmRunner) SetSlurmUsername(username string) *slurmRunner {
	r.username = username
	return r
}
func (r *slurmRunner) SetSlurmPartition(partition string) *slurmRunner {
	r.partition = partition
	return r
}

// SetResources advertises generic resources (typically cluster/feature labels)
// so resource-tagged jobs can be routed to this SLURM cluster. Defaults to
// nil, in which case the runner claims only jobs that require no resources.
func (r *slurmRunner) SetResources(resources map[string]string) *slurmRunner {
	r.resources = resources
	return r
}

// SetHost sets the hostname advertised on each claim (the SLURM submit host).
func (r *slurmRunner) SetHost(host string) *slurmRunner {
	r.host = host
	return r
}

// SetRunnerID overrides the runner's identity (default: a fresh UUID). A stable
// id makes the server's Runners view show one row per runner across restarts.
// Empty is ignored (keeps the default).
func (r *slurmRunner) SetRunnerID(id string) *slurmRunner {
	if id != "" {
		r.runnerId = id
	}
	return r
}

// clampMinArray bounds the min-array gate to the effective per-pass job ceiling
// (the smaller of the set caps maxUserJobs/maxJobs; <=0 = unset). A minimum
// larger than the most tasks that could ever be claimed in a pass would defer an
// array forever, so it is clamped down. Returns the (possibly clamped) minimum,
// the ceiling used, and whether clamping occurred. With minArray or the ceiling
// unset, the input is returned unchanged.
func clampMinArray(minArray, maxUserJobs, maxJobs int) (min, ceiling int, clamped bool) {
	ceiling = -1
	if maxUserJobs > 0 {
		ceiling = maxUserJobs
	}
	if maxJobs > 0 && (ceiling < 0 || maxJobs < ceiling) {
		ceiling = maxJobs
	}
	if minArray > 0 && ceiling > 0 && minArray > ceiling {
		return ceiling, ceiling, true
	}
	return minArray, ceiling, false
}

func (r *slurmRunner) Start() bool {
	submittedOne := false
	r.availJobs = r.maxJobs

	// Array-batch gates. full-array (all-or-nothing) overrides min-array. For
	// min-array, clamp to the effective per-pass ceiling (the smaller of the set
	// caps): a minimum larger than the most tasks we could ever claim would defer
	// the array forever, so cap it and warn.
	minArray, ceiling, clamped := clampMinArray(r.minArray, r.maxUserJobs, r.maxJobs)
	if r.fullArray {
		if ceiling > 0 {
			fmt.Printf("Note: --slurm-full-array is set; any array with more than %d tasks (the job cap) can never fit a single pass and will be deferred.\n", ceiling)
		}
	} else if clamped {
		fmt.Printf("Warning: --slurm-min-array (%d) exceeds the job cap (%d); clamping to %d.\n", r.minArray, ceiling, ceiling)
	}

	ctx := context.Background()

	fmt.Println("Starting SLURM runner...")
	fmt.Println("Updating PROXYQUEUED job SLURM status...")
	r.UpdateSlurmJobStatus(ctx)

	for {
		// How many tasks we may submit this pass: bounded by the live
		// SLURM-queue budget (max_slurm_jobs minus current count) and the
		// per-invocation cap. squeue counts array tasks individually, so the
		// user count already reflects tasks. -1 means unbounded.
		maxTasks := -1
		if r.maxUserJobs > 0 {
			fmt.Println("Getting updated user job count...")
			count, err := SlurmGetUserJobCount(r.username)
			if err != nil {
				fmt.Printf("Error getting job count: %v\n", err)
				return false
			}
			budget := r.maxUserJobs - count
			if budget <= 0 {
				fmt.Printf("User has %d jobs in the SLURM queue. (Max: %d)\n", count, r.maxUserJobs)
				break
			}
			maxTasks = budget
		}
		if r.availJobs == 0 {
			fmt.Printf("No more jobs can be submitted. (Max: %d)\n", r.maxJobs)
			break
		}
		if r.availJobs > 0 && (maxTasks < 0 || r.availJobs < maxTasks) {
			maxTasks = r.availJobs
		}

		// procs, mem, and walltime aren't enforced on the slurm runner side;
		// SLURM enforces them. An array candidate is claimed in a budget-bounded
		// batch so it becomes one `sbatch --array`, drip-fed across passes.
		claimCtx, claimCancel := context.WithTimeout(ctx, 30*time.Second)
		resp, err := r.client.ClaimNextArrayBatch(claimCtx, r.runnerId, "slurm", r.host, -1, -1, -1, r.resources, maxTasks, minArray, r.fullArray)
		claimCancel()
		if err != nil {
			fmt.Printf("Error claiming job: %v\n", err)
			break
		}
		if resp.Job == nil {
			if !resp.MoreEligible {
				// No claimable work. Any Blocked jobs need resources this runner
				// doesn't advertise — it will never satisfy them, so stop.
				if resp.Blocked {
					fmt.Println("Remaining queued jobs need resources this runner doesn't advertise; nothing to submit.")
				}
				if resp.Deferred {
					// An array batch was held back by an array-batch gate: it
					// didn't fit the current budget. Stop; a later pass with more
					// freed SLURM slots will submit it.
					if r.fullArray {
						fmt.Println("Array batch deferred: the full array does not fit the current budget; waiting for more SLURM slots to free up.")
					} else {
						fmt.Printf("Array batch deferred: fewer than %d tasks fit the current budget; waiting for more SLURM slots to free up.\n", minArray)
					}
				}
				break
			}
			// Lost a claim race; back off briefly and retry.
			time.Sleep(1 * time.Second)
			continue
		}

		if resp.ArrayID != "" {
			if r.submitArrayBatch(ctx, resp) {
				submittedOne = true
			}
		} else {
			if r.submitSingleJob(ctx, resp.Job) {
				submittedOne = true
			}
		}
	}

	if submittedOne {
		fmt.Println("Updating PROXYQUEUED job SLURM status...")
		r.UpdateSlurmJobStatus(ctx)
	}
	fmt.Println("SLURM runner done.")

	return submittedOne
}

// submitSingleJob builds and sbatches one plain job, recording its slurm_job_id
// on success. Returns whether the job was submitted.
func (r *slurmRunner) submitSingleJob(ctx context.Context, jobdef *api.JobDTO) bool {
	fmt.Printf("Trying to submit job %s to SLURM\n", jobdef.JobID)
	src, err := r.buildSBatchScript(ctx, jobdef)
	if err != nil {
		fmt.Printf("Error building sbatch script for job %s: %v\n", jobdef.JobID, err)
		r.cancelJob(ctx, jobdef.JobID, err.Error())
		return false
	}
	slurmJobId, err := SlurmSbatch(src, jobEnvFor(jobdef))
	if err != nil {
		fmt.Printf("Error submitting SLURM job: %v\n", err)
		// The failed submission never reaches the DB; persist the generated
		// script + error for troubleshooting.
		if path, werr := saveFailedSlurmScript(jobdef.JobID, src, err); werr == nil {
			fmt.Printf("  Wrote failed sbatch script to %s (and %s.err)\n", path, path)
		}
		r.cancelJob(ctx, jobdef.JobID, fmt.Sprintf("Error submitting to SLURM: %s", err.Error()))
		return false
	}
	pctx, pcancel := context.WithTimeout(ctx, 30*time.Second)
	perr := r.client.MarkJobProxied(pctx, r.runnerId, jobdef.JobID, map[string]string{
		"slurm_job_id":      slurmJobId,
		"slurm_submit_time": support.GetNowUTCString(),
		"slurm_script":      src,
	})
	pcancel()
	if perr != nil {
		fmt.Printf("Error submitting SLURM job (proxy-queue update): %v\n", perr)
		return false
	}
	if r.availJobs > 0 {
		r.availJobs--
	}
	fmt.Printf("Submitted job %s with SLURM job-id %s\n", jobdef.JobID, slurmJobId)
	return true
}

// submitArrayBatch builds and sbatches one `--array` job for the whole claimed
// batch, then records the shared slurm_array_id and each task's slurm_task_index
// so reconciliation can map sacct's `<arrayid>_<index>` rows back. Returns
// whether anything was submitted.
func (r *slurmRunner) submitArrayBatch(ctx context.Context, resp *api.ClaimArrayResponse) bool {
	indices := make([]int, len(resp.Tasks))
	for i, t := range resp.Tasks {
		indices[i] = t.Index
	}
	fmt.Printf("Trying to submit array %s (%d task(s)) to SLURM\n", resp.ArrayID, len(resp.Tasks))
	src, err := r.buildArraySBatchScript(ctx, resp.Job, resp.ArrayID, indices, resp.Throttle)
	if err != nil {
		fmt.Printf("Error building array sbatch for %s: %v\n", resp.ArrayID, err)
		for _, t := range resp.Tasks {
			r.cancelJob(ctx, t.JobID, err.Error())
		}
		return false
	}
	slurmArrayId, err := SlurmSbatch(src, jobEnvFor(resp.Job))
	if err != nil {
		fmt.Printf("Error submitting SLURM array: %v\n", err)
		if path, werr := saveFailedSlurmScript(resp.ArrayID, src, err); werr == nil {
			fmt.Printf("  Wrote failed sbatch script to %s (and %s.err)\n", path, path)
		}
		for _, t := range resp.Tasks {
			r.cancelJob(ctx, t.JobID, fmt.Sprintf("Error submitting array to SLURM: %s", err.Error()))
		}
		return false
	}
	submitTime := support.GetNowUTCString()
	submitted := 0
	for _, t := range resp.Tasks {
		pctx, pcancel := context.WithTimeout(ctx, 30*time.Second)
		// slurm_script is intentionally omitted per task (it is identical across
		// the array; storing N copies would bloat job_running_details).
		perr := r.client.MarkJobProxied(pctx, r.runnerId, t.JobID, map[string]string{
			"slurm_array_id":    slurmArrayId,
			"slurm_task_index":  strconv.Itoa(t.Index),
			"slurm_submit_time": submitTime,
		})
		pcancel()
		if perr != nil {
			fmt.Printf("Error marking array task %s proxied: %v\n", t.JobID, perr)
			continue
		}
		submitted++
	}
	if r.availJobs > 0 {
		r.availJobs -= submitted
		if r.availJobs < 0 {
			r.availJobs = 0
		}
	}
	fmt.Printf("Submitted array %s as SLURM array job-id %s (%d/%d task(s))\n", resp.ArrayID, slurmArrayId, submitted, len(resp.Tasks))
	return submitted > 0
}

// jobEnvFor returns the captured environment for a job (split from the "env"
// detail), or nil when the job didn't capture its environment.
func jobEnvFor(jobdef *api.JobDTO) []string {
	if val := jobdef.Details["env"]; val != "" {
		return strings.Split(val, "\n-|-\n")
	}
	return nil
}

// cancelJob best-effort cancels a job with a reason, logging any error.
func (r *slurmRunner) cancelJob(ctx context.Context, jobID, reason string) {
	cctx, ccancel := context.WithTimeout(ctx, 30*time.Second)
	if cerr := r.client.CancelJob(cctx, jobID, reason); cerr != nil {
		fmt.Printf("Error canceling SLURM job %s: %v\n", jobID, cerr)
	}
	ccancel()
}

func (r *slurmRunner) UpdateSlurmJobStatus(ctx context.Context) {
	listCtx, listCancel := context.WithTimeout(ctx, 30*time.Second)
	proxied, err := r.client.ListJobs(listCtx, client.ListJobsOptions{
		Statuses: []string{jobs.PROXYQUEUED.String()},
	})
	listCancel()
	if err != nil {
		fmt.Printf("Error listing PROXYQUEUED jobs: %v\n", err)
		return
	}
	fmt.Printf("Reconciling %d PROXYQUEUED job(s)\n", len(proxied))

	// Group array tasks by their shared SLURM array id so each array is queried
	// once; everything else reconciles individually by slurm_job_id.
	arrays := map[string][]*api.JobDTO{}
	for _, job := range proxied {
		if aid := job.RunningDetails["slurm_array_id"]; aid != "" {
			arrays[aid] = append(arrays[aid], job)
			continue
		}
		r.reconcilePlain(ctx, job)
	}
	for aid, tasks := range arrays {
		r.reconcileArray(ctx, aid, tasks)
	}
}

// reconcilePlain reconciles a single (non-array) PROXYQUEUED job via its
// slurm_job_id.
func (r *slurmRunner) reconcilePlain(ctx context.Context, job *api.JobDTO) {
	slurmJobIdStr := job.RunningDetails["slurm_job_id"]
	if slurmJobIdStr == "" {
		fmt.Printf("  %s: no slurm_job_id in running_details — skipping\n", job.JobID)
		return
	}
	slurmJobId, err := strconv.Atoi(slurmJobIdStr)
	if err != nil {
		fmt.Printf("  %s: bad slurm_job_id %q — skipping (%v)\n", job.JobID, slurmJobIdStr, err)
		return
	}
	slurmState, err := SlurmGetJobState(slurmJobId)
	if err != nil {
		fmt.Printf("Error getting SLURM job state for job %s (slurm id: %d): %v\n", job.JobID, slurmJobId, err)
		return
	}
	if slurmState == nil {
		fmt.Printf("  %s: slurm id %d not found in sacct — skipping\n", job.JobID, slurmJobId)
		return
	}
	fmt.Printf("  %s: slurm id %d state=%s\n", job.JobID, slurmJobId, slurmState.State)
	r.applySlurmState(ctx, job, slurmState)
}

// reconcileArray reconciles all PROXYQUEUED tasks of one SLURM array with a
// single sacct query, mapping each `<arrayid>_<index>` row back to the task
// whose slurm_task_index matches.
func (r *slurmRunner) reconcileArray(ctx context.Context, arrayIdStr string, tasks []*api.JobDTO) {
	arrayId, err := strconv.Atoi(arrayIdStr)
	if err != nil {
		fmt.Printf("  array %s: bad slurm_array_id — skipping %d task(s) (%v)\n", arrayIdStr, len(tasks), err)
		return
	}
	states, err := SlurmGetArrayState(arrayId)
	if err != nil {
		fmt.Printf("Error getting SLURM array state for array %d: %v\n", arrayId, err)
		return
	}
	for _, job := range tasks {
		idxStr := job.RunningDetails["slurm_task_index"]
		idx, err := strconv.Atoi(idxStr)
		if err != nil {
			fmt.Printf("  %s: bad slurm_task_index %q — skipping (%v)\n", job.JobID, idxStr, err)
			continue
		}
		st, ok := states[idx]
		if !ok {
			// Not yet visible in sacct (still pending) — leave PROXYQUEUED.
			continue
		}
		fmt.Printf("  %s: slurm %d_%d state=%s\n", job.JobID, arrayId, idx, st.State)
		stCopy := st
		r.applySlurmState(ctx, job, &stCopy)
	}
}

// applySlurmState records the latest SLURM state on a job and, when SLURM has
// reached a terminal state, transitions the batchq job to its final status.
func (r *slurmRunner) applySlurmState(ctx context.Context, job *api.JobDTO, slurmState *SlurmJobState) {
	uctx, ucancel := context.WithTimeout(ctx, 30*time.Second)
	if uerr := r.client.UpdateRunningDetails(uctx, r.runnerId, job.JobID, map[string]string{
		"slurm_status":      slurmState.State,
		"slurm_last_update": support.GetNowUTCString(),
	}); uerr != nil {
		fmt.Printf("Error updating running details for %s: %v\n", job.JobID, uerr)
	}
	ucancel()

	var finalStatus string
	switch slurmState.State {
	case "PENDING", "RUNNING":
		return
	case "COMPLETED":
		finalStatus = jobs.SUCCESS.String()
	case "CANCELED", "CANCELLED":
		finalStatus = jobs.CANCELED.String()
	case "FAILED", "TIMEOUT", "OUT_OF_MEMORY":
		finalStatus = jobs.FAILED.String()
	default:
		return
	}
	// Only record the SLURM state as notes for non-success terminals; on
	// success there's nothing useful to say beyond the status itself.
	var endNotes string
	if finalStatus != jobs.SUCCESS.String() {
		endNotes = fmt.Sprintf("slurm reported state: %s", slurmState.State)
	}
	ectx, ecancel := context.WithTimeout(ctx, 30*time.Second)
	if eerr := r.client.EndProxiedJob(ectx, r.runnerId, job.JobID, finalStatus, slurmState.StartAsTime(), slurmState.EndAsTime(), slurmState.ExitCodeInt(), endNotes); eerr != nil {
		fmt.Printf("Error ending proxied job %s: %v\n", job.JobID, eerr)
	}
	ecancel()
}

func (r *slurmRunner) buildSBatchScript(ctx context.Context, jobdef *api.JobDTO) (string, error) {
	// we only support a limited set of SBATCH arguments
	// -c cpus_per_task (with -n 1 nodes)
	// -A account
	// --uid uid
	// --gid gid
	// -J job_name
	// --mem=mem_in_mb
	// -t walltime (DD-HH:MM:SS format)
	// -D working_dir
	// -o stdout
	// -e stderr
	// -d afterok:dep_id
	// --export=ALL this is all or nothing,
	//              if --export=ALL, then the full ENV will be loaded prior
	//              to running sbatch

	script := jobdef.Details["script"]
	if script == "" {
		return "", fmt.Errorf("job has no script body")
	}

	spl := strings.Split(script, "\n")

	// we should be starting with a shebang #!/bin/bash
	src := spl[0] + "\n"

	src += r.slurmResourceDirectives(jobdef)
	if jobdef.Name != "" {
		src += fmt.Sprintf("#SBATCH -J bq-%s.%s\n", jobdef.JobID, jobdef.Name)
	}
	if val := jobdef.Details["stdout"]; val != "" {
		src += fmt.Sprintf("#SBATCH -o %s\n", val)
	}
	if val := jobdef.Details["stderr"]; val != "" {
		src += fmt.Sprintf("#SBATCH -e %s\n", val)
	}

	dep, err := r.slurmAfterOkDirective(ctx, jobdef)
	if err != nil {
		return "", err
	}
	src += dep

	src += "JOB_ID=$SLURM_JOB_ID\n"

	src += strings.Join(spl[1:], "\n")

	return src, nil
}

// buildArraySBatchScript builds a single sbatch script for a whole array batch:
// one `#SBATCH --array=<indices>%throttle` line, -o/-e using the per-task
// template (SLURM substitutes its own %A/%a), and the array deps. The task
// index reaches the script via $SLURM_ARRAY_TASK_ID (exposed as
// BATCHQ_ARRAY_TASK_ID).
func (r *slurmRunner) buildArraySBatchScript(ctx context.Context, jobdef *api.JobDTO, arrayID string, indices []int, throttle int) (string, error) {
	script := jobdef.Details["script"]
	if script == "" {
		return "", fmt.Errorf("array has no script body")
	}
	spl := strings.Split(script, "\n")
	src := spl[0] + "\n"

	src += r.slurmResourceDirectives(jobdef)
	if jobdef.Name != "" {
		src += fmt.Sprintf("#SBATCH -J bq-%s.%s\n", arrayID, jobdef.Name)
	}
	if throttle > 0 {
		src += fmt.Sprintf("#SBATCH --array=%s%%%d\n", joinIntList(indices), throttle)
	} else {
		src += fmt.Sprintf("#SBATCH --array=%s\n", joinIntList(indices))
	}
	if val := jobdef.Details["stdout"]; val != "" {
		src += fmt.Sprintf("#SBATCH -o %s\n", val)
	}
	if val := jobdef.Details["stderr"]; val != "" {
		src += fmt.Sprintf("#SBATCH -e %s\n", val)
	}

	dep, err := r.slurmAfterOkDirective(ctx, jobdef)
	if err != nil {
		return "", err
	}
	src += dep

	src += "JOB_ID=$SLURM_JOB_ID\n"
	src += "BATCHQ_ARRAY_ID=" + arrayID + "\n"
	src += "BATCHQ_ARRAY_TASK_ID=$SLURM_ARRAY_TASK_ID\n"

	src += strings.Join(spl[1:], "\n")

	return src, nil
}

// slurmResourceDirectives returns the #SBATCH lines shared by plain and array
// jobs (account, partition, cpus, env, uid, gid, mem, walltime, wd).
func (r *slurmRunner) slurmResourceDirectives(jobdef *api.JobDTO) string {
	var src string
	if r.account != "" {
		src += fmt.Sprintf("#SBATCH -A %s\n", r.account)
	}
	if r.partition != "" {
		src += fmt.Sprintf("#SBATCH -p %s\n", r.partition)
	}
	if val := jobdef.Details["procs"]; val != "" {
		if procN, err := strconv.Atoi(val); err == nil && procN > 0 {
			src += fmt.Sprintf("#SBATCH -c %d\n", procN) // N cpus per task
			src += "#SBATCH -n 1\n"                      // one node
		}
	}
	if val := jobdef.Details["env"]; val != "" {
		src += "#SBATCH --export=ALL\n"
	}
	// sbatch --uid/--gid are privileged (root/SlurmUser only): a normal user
	// submitting on their own behalf must NOT pass them or sbatch rejects the
	// job (exit 255). Only emit them when this runner is actually root and can
	// legitimately submit on another user's behalf; otherwise the SLURM job
	// already runs as the submitting user.
	if support.AmIRoot() {
		if val := jobdef.Details["uid"]; val != "" {
			src += fmt.Sprintf("#SBATCH --uid %s\n", val)
		}
		if val := jobdef.Details["gid"]; val != "" {
			src += fmt.Sprintf("#SBATCH --gid %s\n", val)
		}
	}
	if val := jobdef.Details["mem"]; val != "" {
		src += fmt.Sprintf("#SBATCH --mem=%s\n", val)
	}
	if val := jobdef.Details["walltime"]; val != "" {
		src += fmt.Sprintf("#SBATCH -t %s\n", SlurmSecsToWalltime(val))
	}
	if val := jobdef.Details["wd"]; val != "" {
		src += fmt.Sprintf("#SBATCH -D %s\n", val)
	}
	return src
}

// slurmAfterOkDirective maps a job's batchq afterok deps to a SLURM
// `-d afterok:` directive (plus --kill-on-invalid-dep). Returns "" when there
// are no pending deps. Errors if a dep failed or isn't ready yet.
func (r *slurmRunner) slurmAfterOkDirective(ctx context.Context, jobdef *api.JobDTO) (string, error) {
	if len(jobdef.AfterOk) == 0 {
		return "", nil
	}
	// remap sbatch job-ids to slurm job-ids (running_detail: slurm_job_id)
	var slurmAfterOkId []string
	for _, depid := range jobdef.AfterOk {
		dctx, dcancel := context.WithTimeout(ctx, 30*time.Second)
		dep, err := r.client.GetJob(dctx, depid)
		dcancel()
		if err != nil {
			return "", fmt.Errorf("fetching dep job %s: %v", depid, err)
		}
		switch dep.Status {
		case jobs.CANCELED.String(), jobs.FAILED.String():
			return "", fmt.Errorf("dependency of job failed (depid: %s)", dep.JobID)
		case jobs.UNKNOWN.String(), jobs.USERHOLD.String(), jobs.WAITING.String():
			return "", fmt.Errorf("not ready to process dep job yet (depid: %s)", dep.JobID)
		case jobs.RUNNING.String(), jobs.PROXYQUEUED.String():
			// Trust the recorded slurm_job_id. SLURM itself, together with
			// --kill-on-invalid-dep=yes below, correctly handles a parent
			// that is PENDING/RUNNING (wait), COMPLETED (satisfy now), or
			// FAILED/CANCELLED (kill the child). Calling sacct/squeue here
			// just to validate the parent's state opens a race window: if
			// the parent was sbatch'd moments ago, its ID may not yet be
			// visible to outside readers even though we hold the ID.
			slurm_id := dep.RunningDetails["slurm_job_id"]
			if slurm_id == "" {
				return "", fmt.Errorf("missing slurm_job_id (depid: %s)", dep.JobID)
			}
			slurmAfterOkId = append(slurmAfterOkId, slurm_id)
		case jobs.SUCCESS.String():
			// we're all good... no need to add it.
		}
	}
	if len(slurmAfterOkId) == 0 {
		return "", nil
	}
	return "#SBATCH --kill-on-invalid-dep=yes\n" +
		fmt.Sprintf("#SBATCH -d afterok:%s\n", strings.Join(slurmAfterOkId, ":")), nil
}

// joinIntList renders a slice of ints as a comma-separated list.
func joinIntList(xs []int) string {
	parts := make([]string, len(xs))
	for i, x := range xs {
		parts[i] = strconv.Itoa(x)
	}
	return strings.Join(parts, ",")
}

/*
SlurmGetUserJobCount returns the number of jobs currently submitted to SLURM
for the given username. If username is empty, it returns the total number of
jobs in the queue. This uses "squeue" command to get the job count.
*/
func SlurmGetUserJobCount(username string) (int, error) {
	var cmd *exec.Cmd
	if username == "" {
		cmd = exec.Command("squeue")
	} else {
		cmd = exec.Command("squeue", "-u", username)
	}

	// Capture stdout
	out, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("running squeue: %w", err)
	}

	return countSqueueJobs(out)
}

// countSqueueJobs counts the job rows in `squeue` output: every non-blank line
// except the "JOBID ..." header. Blank lines are skipped (HasPrefix, not a
// fixed-width slice, so short/blank lines can't panic). Split out from
// SlurmGetUserJobCount so the parsing is unit-testable without a SLURM install.
func countSqueueJobs(out []byte) (int, error) {
	scanner := bufio.NewScanner(bytes.NewReader(out))
	count := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "JOBID") {
			// header line
			continue
		}
		count++
	}

	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("scanning squeue output: %w", err)
	}

	return count, nil
}

/*
SlurmJobStatus returns the current status for a job in SLURM
This can be:

	RUNNING, PENDING, COMPLETED, CANCELLED,
	TIMEOUT, OUT_OF_MEMORY, FAILURE (all other non-success)

If the job is done, the return code will also be given.
*/
func SlurmGetJobState(jobId int) (*SlurmJobState, error) {
	if jobId <= 0 {
		return nil, fmt.Errorf("invalid SLURM job-id")
	}

	cmd := exec.Command("sacct", "--format", "JobId,State,Start,End,ExitCode,NodeList", "--parsable", "-j", strconv.Itoa(jobId))

	// Capture stdout
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("running sacct: %v", err)
	}

	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		spl := strings.Split(line, "|")
		if spl[0] == strconv.Itoa(jobId) {
			return &SlurmJobState{
				JobId:    spl[0],
				State:    strings.Split(spl[1], " ")[0], // remove any trailing info after space
				Start:    spl[2],
				End:      spl[3],
				ExitCode: spl[4],
				NodeList: spl[5],
			}, nil
		}
	}
	return nil, nil // job not found
}

// SlurmGetArrayState returns the per-task state of a SLURM array job keyed by
// task index. It parses `sacct -j <arrayid>`'s `<arrayid>_<index>` rows,
// skipping the `.batch`/`.extern` step rows and the compressed
// `<arrayid>_[lo-hi]` pending-placeholder row.
func SlurmGetArrayState(arrayId int) (map[int]SlurmJobState, error) {
	if arrayId <= 0 {
		return nil, fmt.Errorf("invalid SLURM array job-id")
	}

	cmd := exec.Command("sacct", "--format", "JobId,State,Start,End,ExitCode,NodeList", "--parsable", "-j", strconv.Itoa(arrayId))
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("running sacct: %v", err)
	}
	return parseSacctArrayOutput(out, arrayId), nil
}

// parseSacctArrayOutput parses `sacct --parsable` output for an array job into a
// per-task-index state map. It keeps only the `<arrayid>_<index>` task rows,
// skipping the header, the `.batch`/`.extern` step rows, and the compressed
// `<arrayid>_[lo-hi]` pending-placeholder row.
func parseSacctArrayOutput(out []byte, arrayId int) map[int]SlurmJobState {
	states := map[int]SlurmJobState{}
	prefix := strconv.Itoa(arrayId) + "_"
	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		spl := strings.Split(line, "|")
		if len(spl) < 6 {
			continue
		}
		jobField := spl[0]
		if !strings.HasPrefix(jobField, prefix) {
			continue
		}
		idxPart := jobField[len(prefix):]
		if strings.Contains(idxPart, ".") {
			// step row: <arrayid>_<index>.batch / .extern
			continue
		}
		idx, err := strconv.Atoi(idxPart)
		if err != nil {
			// compressed pending placeholder: <arrayid>_[lo-hi]
			continue
		}
		states[idx] = SlurmJobState{
			JobId:    jobField,
			State:    strings.Split(spl[1], " ")[0],
			Start:    spl[2],
			End:      spl[3],
			ExitCode: spl[4],
			NodeList: spl[5],
		}
	}
	return states
}

/*
SlurmSbatch	submits the given script using the "sbatch" command.
All job configuration values should be included as '#SBATCH' prefixed
lines.
*/
func SlurmSbatch(script string, env []string) (string, error) {
	cmd := exec.Command("sbatch", "--parsable")
	if env != nil {
		cmd.Env = env
	}

	cmd.Stdin = strings.NewReader(script)

	// Capture stderr too — sbatch reports the actual reason for a rejection
	// there, so swallowing it (cmd.Output only keeps stdout) leaves a bare
	// "exit status N" with no detail.
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	out, err := cmd.Output()
	if err != nil {
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return "", fmt.Errorf("running sbatch: %w: %s", err, msg)
		}
		return "", fmt.Errorf("running sbatch: %w", err)
	}

	if line, _, err := bufio.NewReader(bytes.NewReader(out)).ReadLine(); err != nil {
		return "", fmt.Errorf("reading sbatch output: %w", err)
	} else {
		return strings.TrimSpace(string(line)), nil
	}
}

// saveFailedSlurmScript writes a generated sbatch script (and the submission
// error) to $BATCHQ_HOME/failed/ so a failed submission — which is never
// persisted to the DB — can still be inspected. Returns the script path.
func saveFailedSlurmScript(jobID, script string, subErr error) (string, error) {
	dir := filepath.Join(support.GetBatchqHome(), "failed")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(dir, "slurm-"+jobID+".sh")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		return "", err
	}
	if subErr != nil {
		_ = os.WriteFile(path+".err", []byte(subErr.Error()+"\n"), 0o644)
	}
	return path, nil
}

func SlurmSecsToWalltime(secStr string) string {
	secs, err := strconv.Atoi(secStr)
	if err != nil {
		return "00:00:00"
	}

	days := secs / (60 * 60 * 24)
	secs = secs % (60 * 60 * 24)
	hours := secs / (60 * 60)
	secs = secs % (60 * 60)
	minutes := secs / 60
	secs = secs % 60

	if days > 0 {
		return fmt.Sprintf("%d-%d:%d:%d", days, hours, minutes, secs)
	}
	return fmt.Sprintf("%d:%d:%d", hours, minutes, secs)
}
