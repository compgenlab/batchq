package runner

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
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
	availJobs   int
	account     string
	username    string
	partition   string
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

func (r *slurmRunner) Start() bool {
	submittedOne := false
	r.availJobs = r.maxJobs

	ctx := context.Background()

	fmt.Println("Starting SLURM runner...")
	fmt.Println("Updating PROXYQUEUED job SLURM status...")
	r.UpdateSlurmJobStatus(ctx)

	for {
		if r.maxUserJobs > 0 {
			fmt.Println("Getting updated user job count...")

			count, err := SlurmGetUserJobCount(r.username)
			if err != nil {
				fmt.Printf("Error getting job count: %v\n", err)
				return false
			}
			if count >= r.maxUserJobs {
				fmt.Printf("User has %d jobs running. (Max: %d)\n", count, r.maxUserJobs)
				break
			}
		}

		if r.availJobs == 0 {
			fmt.Printf("No more jobs can be submitted. (Max: %d)\n", r.maxJobs)
			break
		}

		// procs, mem, and walltime aren't enforced on the slurm runner side;
		// SLURM enforces them. We let the server return any QUEUED job.
		claimCtx, claimCancel := context.WithTimeout(ctx, 30*time.Second)
		resp, err := r.client.ClaimNextJob(claimCtx, r.runnerId, "slurm", -1, -1, -1)
		claimCancel()
		if err != nil {
			fmt.Printf("Error claiming job: %v\n", err)
			break
		}
		if resp.Job == nil {
			if !resp.MoreEligible {
				break
			}
			// More eligible but couldn't claim now (transient); back off briefly.
			time.Sleep(1 * time.Second)
			continue
		}

		jobdef := resp.Job
		fmt.Printf("Trying to submit job %s to SLURM\n", jobdef.JobID)

		if src, err := r.buildSBatchScript(ctx, jobdef); err != nil {
			// we must have a dependency issue with this job
			// or a dep failed.
			fmt.Printf("Error trying to build sbatch script for job: %s\n  => %s\n", jobdef.JobID, err.Error())
			cctx, ccancel := context.WithTimeout(ctx, 30*time.Second)
			if cerr := r.client.CancelJob(cctx, jobdef.JobID, err.Error()); cerr != nil {
				fmt.Printf("Error canceling SLURM job %s: %v\n", jobdef.JobID, cerr)
			}
			ccancel()
		} else if src == "" {
			fmt.Printf("Error trying to build sbatch script for job: %s (empty script, possibly due to SLURM sacct delay)\n", jobdef.JobID)
			// The claim already moved this job to RUNNING; report a transient
			// failure so the server returns it to QUEUED-equivalent state
			// (actually FAILED). Sleep to avoid busy-looping; we'll resubmit
			// from the user side if needed.
			r.failClaimedJob(ctx, jobdef.JobID)
			time.Sleep(1 * time.Second)
		} else {
			var jobEnv []string
			if val := jobdef.Details["env"]; val != "" {
				jobEnv = strings.Split(val, "\n-|-\n")
			}
			if slurmJobId, err := SlurmSbatch(src, jobEnv); err == nil {
				pctx, pcancel := context.WithTimeout(ctx, 30*time.Second)
				perr := r.client.MarkJobProxied(pctx, r.runnerId, jobdef.JobID, map[string]string{
					"slurm_job_id":      slurmJobId,
					"slurm_submit_time": support.GetNowUTCString(),
					"slurm_script":      src,
				})
				pcancel()
				if perr == nil {
					submittedOne = true
					if r.availJobs > 0 {
						r.availJobs--
					}
					fmt.Printf("Submitted job %s with SLURM job-id %s\n", jobdef.JobID, slurmJobId)
				} else {
					fmt.Printf("Error submitting SLURM job (proxy-queue update): %v\n", perr)
				}
			} else {
				fmt.Printf("Error submitting SLURM job: %v\n", err)
				cctx, ccancel := context.WithTimeout(ctx, 30*time.Second)
				if cerr := r.client.CancelJob(cctx, jobdef.JobID, fmt.Sprintf("Error submitting to SLURM: %s", err.Error())); cerr != nil {
					fmt.Printf("Error canceling SLURM job %s: %v\n", jobdef.JobID, cerr)
				}
				ccancel()
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

// failClaimedJob marks a job FAILED when the runner already won the claim
// but couldn't follow through. Without this the job is stuck in RUNNING.
func (r *slurmRunner) failClaimedJob(ctx context.Context, jobID string) {
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := r.client.EndJob(cctx, r.runnerId, jobID, 1); err != nil && !errors.Is(err, client.ErrInvalidState) {
		fmt.Printf("Error marking claimed job %s as failed: %v\n", jobID, err)
	}
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
	for _, job := range proxied {
		slurmJobIdStr := job.RunningDetails["slurm_job_id"]
		if slurmJobIdStr == "" {
			fmt.Printf("  %s: no slurm_job_id in running_details — skipping\n", job.JobID)
			continue
		}
		slurmJobId, err := strconv.Atoi(slurmJobIdStr)
		if err != nil {
			fmt.Printf("  %s: bad slurm_job_id %q — skipping (%v)\n", job.JobID, slurmJobIdStr, err)
			continue
		}
		slurmState, err := SlurmGetJobState(slurmJobId)
		if err != nil {
			fmt.Printf("Error getting SLURM job state for job %s (slurm id: %d): %v\n", job.JobID, slurmJobId, err)
			continue
		}
		if slurmState == nil {
			fmt.Printf("  %s: slurm id %d not found in sacct — skipping\n", job.JobID, slurmJobId)
			continue
		}
		fmt.Printf("  %s: slurm id %d state=%s\n", job.JobID, slurmJobId, slurmState.State)
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
			continue
		case "COMPLETED":
			finalStatus = jobs.SUCCESS.String()
		case "CANCELED", "CANCELLED":
			finalStatus = jobs.CANCELED.String()
		case "FAILED", "TIMEOUT", "OUT_OF_MEMORY":
			finalStatus = jobs.FAILED.String()
		default:
			continue
		}
		ectx, ecancel := context.WithTimeout(ctx, 30*time.Second)
		if eerr := r.client.EndProxiedJob(ectx, r.runnerId, job.JobID, finalStatus, slurmState.StartAsTime(), slurmState.EndAsTime(), slurmState.ExitCodeInt()); eerr != nil {
			fmt.Printf("Error ending proxied job %s: %v\n", job.JobID, eerr)
		}
		ecancel()
	}
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
		return "", nil
	}

	spl := strings.Split(script, "\n")

	// we should be starting with a shebang #!/bin/bash
	src := spl[0] + "\n"

	if r.account != "" {
		src += fmt.Sprintf("#SBATCH -A %s\n", r.account)
	}
	if r.partition != "" {
		src += fmt.Sprintf("#SBATCH -p %s\n", r.partition)
	}
	if jobdef.Name != "" {
		src += fmt.Sprintf("#SBATCH -J bq-%s.%s\n", jobdef.JobID, jobdef.Name)
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
	if val := jobdef.Details["uid"]; val != "" {
		src += fmt.Sprintf("#SBATCH --uid %s\n", val)
	}
	if val := jobdef.Details["gid"]; val != "" {
		src += fmt.Sprintf("#SBATCH --gid %s\n", val)
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
	if val := jobdef.Details["stdout"]; val != "" {
		src += fmt.Sprintf("#SBATCH -o %s\n", val)
	}
	if val := jobdef.Details["stderr"]; val != "" {
		src += fmt.Sprintf("#SBATCH -o %s\n", val)
	}

	if len(jobdef.AfterOk) > 0 {
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
				if slurm_id := dep.RunningDetails["slurm_job_id"]; slurm_id != "" {
					slurm_id_int, err := strconv.Atoi(slurm_id)
					if err != nil {
						return "", fmt.Errorf("bad slurm id (depid: %s, slurm_id: %s) %v", dep.JobID, slurm_id, err)
					}
					slurmState, err := SlurmGetJobState(slurm_id_int)
					if err != nil {
						return "", fmt.Errorf("error getting slurm job status (depid: %s, slurm_id: %d) error: %s", dep.JobID, slurm_id_int, err.Error())
					}
					if slurmState == nil {
						return "", nil
					}
					switch slurmState.State {
					case "PENDING", "RUNNING":
						slurmAfterOkId = append(slurmAfterOkId, slurm_id)
					case "COMPLETED":
						// fine, no need to add
					default:
						return "", fmt.Errorf("bad slurm job state (depid: %s, slurm_id: %d, state: %s)", dep.JobID, slurm_id_int, slurmState.State)
					}
				} else {
					return "", fmt.Errorf("missing slurm_job_id (depid: %s)", dep.JobID)
				}
			case jobs.SUCCESS.String():
				// we're all good... no need to add it.
			}
		}
		if len(slurmAfterOkId) > 0 {
			src += "#SBATCH --kill-on-invalid-dep=yes\n"
			src += fmt.Sprintf("#SBATCH -d afterok:%s\n", strings.Join(slurmAfterOkId, ":"))
		}
	}

	src += "JOB_ID=$SLURM_JOB_ID\n"

	src += strings.Join(spl[1:], "\n")

	return src, nil
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

	scanner := bufio.NewScanner(bytes.NewReader(out))
	count := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line[:5] == "JOBID" {
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

	// Capture stdout
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("running sbatch: %w", err)
	}

	if line, _, err := bufio.NewReader(bytes.NewReader(out)).ReadLine(); err != nil {
		return "", fmt.Errorf("reading sbatch output: %w", err)
	} else {
		return strings.TrimSpace(string(line)), nil
	}
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
