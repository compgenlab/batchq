package runner

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mbreese/batchq/db"
	"github.com/mbreese/batchq/jobs"
	"github.com/mbreese/batchq/support"
)

type slurmRunner struct {
	db          db.BatchDB
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

// These aren't UTC tims, so we just parse them as local time
func (s *SlurmJobState) EndAsTimeString() string {
	if strings.ToUpper(s.End) == "UNKNOWN" || s.End == "" {
		return ""
	}
	layout := "2006-01-02T15:04:05"
	t, err := time.ParseInLocation(layout, s.End, time.Local)
	if err != nil {
		return ""
	}
	return t.UTC().Format("2006-01-02 15:04:05 MST")
}

func (s *SlurmJobState) StartAsTimeString() string {
	if strings.ToUpper(s.Start) == "UNKNOWN" || s.Start == "" {
		return ""
	}
	layout := "2006-01-02T15:04:05"
	t, err := time.ParseInLocation(layout, s.Start, time.Local)
	if err != nil {
		return ""

	}
	return t.UTC().Format("2006-01-02 15:04:05 MST")
}

func (s *SlurmJobState) ExitCodeInt() int {
	val, err := strconv.Atoi(strings.Split(s.ExitCode, ":")[0])
	if err != nil {
		return -1
	}
	return val
}

// type jobStatus struct {
// 	job        *jobs.JobDef
// 	cmd        *exec.Cmd
// 	running    bool
// 	returnCode int
// 	startTime  time.Time
// }

/*
Create a new Runner that submits jobs to a SLURM queue. In this way, batchq
operates as a proxy to SLURM. This is helpful when you have a limit on the
number of jobs that can be submitted to the SLURM queue. By using batchq, you
can submit as many jobs as you'd like to your personal queue, and only submit
new jobs to the SLURM queue as your job-count decreases.
*/
func NewSlurmRunner(jobq db.BatchDB) *slurmRunner {
	id := uuid.New()
	runner := slurmRunner{
		runnerId:    id.String(),
		db:          jobq,
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

		// freeProc, Mem, Time aren't used here
		// we only care about whole jobs to re-submit.
		// we'll let SLURM deal with procs, mem, and time.
		// fmt.Println("Looking for a new job to submit...")

		if jobdef, hasNext := r.db.FetchNext(ctx, nil); jobdef != nil {
			fmt.Printf("Trying to submit job %d to SLURM\n", jobdef.JobId)

			if src, err := r.buildSBatchScript(ctx, jobdef); err != nil {
				// we must have a dependency issue with this job
				// or a dep failed.
				r.db.CancelJob(ctx, jobdef.JobId, err.Error())
				fmt.Printf("Error trying to build sbatch script for job: %d\n  => %s\n", jobdef.JobId, err.Error())
			} else if src == "" {
				fmt.Printf("Error trying to build sbatch script for job: %d (empty script, possibly due to SLURM sacct delay)\n", jobdef.JobId)
				// sleep 1 second to avoid busy-looping
				time.Sleep(1 * time.Second)
			} else if src != "" {
				var jobEnv []string
				if val := jobdef.GetDetail("env", ""); val != "" {
					jobEnv = strings.Split(val, "\n-|-\n")
				}
				// fmt.Println("  Submitting job to SLURM... (got sbatch script)")
				if slurmJobId, err := SlurmSbatch(src, jobEnv); err == nil {
					// fmt.Println("  Updating batchq job status to ProxyQueued...")
					if r.db.ProxyQueueJob(ctx, jobdef.JobId, r.runnerId, map[string]string{"slurm_job_id": slurmJobId, "slurm_submit_time": support.GetNowUTCString(), "slurm_script": src}) {
						submittedOne = true
						if r.availJobs > 0 {
							r.availJobs--
						}
						fmt.Printf("Submitted job %d with SLURM job-id %s\n", jobdef.JobId, slurmJobId)
					} else {
						fmt.Printf("Error submitting SLURM job (unknown reason)\n")
					}
				} else {
					fmt.Printf("Error submitting SLURM job: %v\n", err)
				}
			}
		} else if !hasNext {
			break
		}
	}

	if submittedOne {
		fmt.Println("Updating PROXYQUEUED job SLURM status...")
		r.UpdateSlurmJobStatus(ctx)
	}

	return submittedOne
}

func (r *slurmRunner) UpdateSlurmJobStatus(ctx context.Context) {
	proxied := r.db.GetProxyJobs(ctx)
	for _, job := range proxied {
		if slurmJobIdStr := job.GetRunningDetail("slurm_job_id", ""); slurmJobIdStr != "" {
			if slurmJobId, err := strconv.Atoi(slurmJobIdStr); err == nil {
				if slurmState, err := SlurmGetJobState(slurmJobId); err != nil {
					fmt.Printf("Error getting SLURM job state for job %d (slurm id: %d): %v\n", job.JobId, slurmJobId, err)
				} else if slurmState != nil {
					r.db.UpdateJobRunningDetails(ctx, job.JobId, map[string]string{"slurm_status": slurmState.State, "slurm_last_update": support.GetNowUTCString()})
					switch slurmState.State {
					case "PENDING":
						// r.db.UpdateJobRunningDetails(ctx, job.JobId, map[string]string{"slurm_status": slurmState.State, "slurm_last_update": support.GetNowUTCString()})
					case "RUNNING":
						// r.db.UpdateJobRunningDetails(ctx, job.JobId, map[string]string{"slurm_status": slurmState.State, "slurm_last_update": support.GetNowUTCString(), "slurm_start_time": slurmState.StartAsTimeString(), "slurm_node_list": slurmState.NodeList})
					case "COMPLETED":
						// success
						r.db.ProxyEndJob(ctx, job.JobId, jobs.SUCCESS, slurmState.StartAsTimeString(), slurmState.EndAsTimeString(), slurmState.ExitCodeInt())
						// fmt.Printf("Job %d completed successfully in SLURM (slurm id: %d)\n", job.JobId, slurmJobId)
					case "CANCELED":
						// canceled
						r.db.ProxyEndJob(ctx, job.JobId, jobs.CANCELED, slurmState.StartAsTimeString(), slurmState.EndAsTimeString(), slurmState.ExitCodeInt())
						// fmt.Printf("Job %d failed in SLURM (slurm id: %d)\n", job.JobId, slurmJobId)
					case "FAILED", "TIMEOUT", "OUT_OF_MEMORY":
						// failed
						r.db.ProxyEndJob(ctx, job.JobId, jobs.FAILED, slurmState.StartAsTimeString(), slurmState.EndAsTimeString(), slurmState.ExitCodeInt())
						// fmt.Printf("Job %d failed in SLURM (slurm id: %d)\n", job.JobId, slurmJobId)
					}
				}
			}
		}
	}
}

func (r *slurmRunner) buildSBatchScript(ctx context.Context, jobdef *jobs.JobDef) (string, error) {
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

	script := jobdef.GetDetail("script", "")
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
		src += fmt.Sprintf("#SBATCH -J bq-%d.%s\n", jobdef.JobId, jobdef.Name)
	}
	if val := jobdef.GetDetail("procs", ""); val != "" {
		if procN, err := strconv.Atoi(val); err == nil && procN > 0 {
			src += fmt.Sprintf("#SBATCH -c %d\n", procN) // N cpus per task
			src += "#SBATCH -n 1\n"                      // one node
		}
	}
	if val := jobdef.GetDetail("env", ""); val != "" {
		src += "#SBATCH --export=ALL\n"
	}
	if val := jobdef.GetDetail("uid", ""); val != "" {
		src += fmt.Sprintf("#SBATCH --uid %s\n", val)
	}
	if val := jobdef.GetDetail("gid", ""); val != "" {
		src += fmt.Sprintf("#SBATCH --gid %s\n", val)
	}
	if val := jobdef.GetDetail("mem", ""); val != "" {
		src += fmt.Sprintf("#SBATCH --mem=%s\n", val)
	}
	if val := jobdef.GetDetail("walltime", ""); val != "" {
		src += fmt.Sprintf("#SBATCH -t %s\n", SlurmSecsToWalltime(val))
	}
	if val := jobdef.GetDetail("wd", ""); val != "" {
		src += fmt.Sprintf("#SBATCH -D %s\n", val)
	}
	if val := jobdef.GetDetail("stdout", ""); val != "" {
		src += fmt.Sprintf("#SBATCH -o %s\n", val)
	}
	if val := jobdef.GetDetail("stderr", ""); val != "" {
		src += fmt.Sprintf("#SBATCH -o %s\n", val)
	}

	if len(jobdef.AfterOk) > 0 {
		// remap sbatch job-ids to slurm job-ids (running_detail: slurm_job_id)
		var slurmAfterOkId []string
		for _, depid := range jobdef.AfterOk {
			dep := r.db.GetJob(ctx, depid)
			switch dep.Status {
			case jobs.CANCELED, jobs.FAILED:
				// this is a problem
				return "", fmt.Errorf("dependency of job failed (depid: %d)", dep.JobId)
			case jobs.UNKNOWN, jobs.USERHOLD, jobs.WAITING:
				// this is a problem
				return "", fmt.Errorf("not ready to process dep job yet (depid: %d)", dep.JobId)
			case jobs.RUNNING, jobs.PROXYQUEUED:
				// check with slurm first
				if slurm_id := dep.GetRunningDetail("slurm_job_id", ""); slurm_id != "" {
					if slurm_id_int, err := strconv.Atoi(slurm_id); err != nil {
						return "", fmt.Errorf("bad slurm id (depid: %d, slurm_id: %s) %v", dep.JobId, slurm_id, err)
					} else {
						if slurmState, err := SlurmGetJobState(slurm_id_int); err != nil {
							// problem getting slurm state
							return "", fmt.Errorf("error getting slurm job status (depid: %d, slurm_id: %d) error: %s", dep.JobId, slurm_id_int, err.Error())
						} else if slurmState != nil {
							switch slurmState.State {
							case "PENDING":
								slurmAfterOkId = append(slurmAfterOkId, slurm_id)
							case "RUNNING":
								slurmAfterOkId = append(slurmAfterOkId, slurm_id)
							case "COMPLETED":
								// this is also fine
							default:
								return "", fmt.Errorf("bad slurm job state (depid: %d, slurm_id: %d, state: %s)", dep.JobId, slurm_id_int, slurmState.State)
							}
						} else {
							// dep-job not found yet... this won't trigger a cancel, but
							// it will trigger a retry later.
							return "", nil
						}
					}
				} else {
					return "", fmt.Errorf("missing slurm_job_id (depid: %d)", dep.JobId)
				}
			case jobs.SUCCESS:
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

	// fmt.Printf("SACCT OUTPUT:\n%s\n", string(out))

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
	// fmt.Fprintf(os.Stderr, "SLURM SBATCH SCRIPT:\n%s\n", script)

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

	// fmt.Fprintf(os.Stderr, "SBATCH RESULT: %s\n", string(out))

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
