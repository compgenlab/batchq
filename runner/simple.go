package runner

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/mbreese/batchq/api"
	"github.com/mbreese/batchq/client"
	"github.com/mbreese/batchq/jobs"
	"github.com/mbreese/batchq/support"
)

type simpleRunner struct {
	client         *client.Client
	maxProcs       int
	maxMemoryMb    int
	maxWalltimeSec int
	foreverMode    bool
	useCgroupV2    bool
	useCgroupV1    bool
	curJobs        []jobStatus
	maxJobs        int
	availJobs      int
	availProcs     int
	availMem       int
	shellBin       string
	runnerId       string
	interrupt      context.CancelFunc
	lock           sync.Mutex
	logLock        sync.Mutex
	spoolDir       string
	lastMsg        string
}

type jobStatus struct {
	job        *api.JobDTO
	cmd        *exec.Cmd
	running    bool
	returnCode int
	startTime  time.Time
}

/*
Create a new Runner that limits processors, memory, and wall time. For processors and memory,
there will be a fixed amount that can be used at one time. For wall time, this is the maximum
length of time any one job can run -- not a limit on the amount of time that this runner will
run.

So, if you have two jobs that will take one day each, and you specify walltime as 24:00:00, both
jobs could run serially and take 48 total hours.

If procs and mem are both <= 0, then one job will run at a time.

Unless daemon mode is set, this will run until there are no more jobs left that fit these
criterion.
*/
func NewSimpleRunner(c *client.Client) *simpleRunner {
	id := uuid.New()
	runner := simpleRunner{client: c, maxProcs: -1, maxMemoryMb: -1, maxWalltimeSec: -1, maxJobs: -1, foreverMode: false, runnerId: id.String(), spoolDir: filepath.Join(support.GetBatchqHome(), "spool")}
	return &runner
}

func (r *simpleRunner) SetMaxProcs(procs int) *simpleRunner {
	r.maxProcs = procs
	return r
}
func (r *simpleRunner) SetMaxMemMB(mem int) *simpleRunner {
	r.maxMemoryMb = mem
	return r
}
func (r *simpleRunner) SetMaxWalltimeSec(walltime int) *simpleRunner {
	r.maxWalltimeSec = walltime
	return r
}
func (r *simpleRunner) SetMaxJobCount(maxJobs int) *simpleRunner {
	r.maxJobs = maxJobs
	return r
}
func (r *simpleRunner) SetForever(mode bool) *simpleRunner {
	r.foreverMode = mode
	return r
}
func (r *simpleRunner) SetShell(shell string) *simpleRunner {
	r.shellBin = shell
	return r
}
func (r *simpleRunner) SetCgroupV2(useCgroupV2 bool) *simpleRunner {
	if useCgroupV2 && !support.AmIRoot() {
		log.Fatalln("cgroup support requires running as root.")
	}

	r.useCgroupV2 = useCgroupV2

	return r
}

func (r *simpleRunner) SetCgroupV1(useCgroupV1 bool) *simpleRunner {
	if useCgroupV1 && !support.AmIRoot() {
		log.Fatalln("cgroup support requires running as root.")
	}

	r.useCgroupV1 = useCgroupV1

	return r
}

func (r *simpleRunner) logf(msg string, v ...any) {
	out := fmt.Sprintf(msg, v...)
	if out != r.lastMsg {
		r.logLock.Lock()
		if out != "" {
			log.Print(out)
		}
		r.lastMsg = out
		r.logLock.Unlock()
	}
}

func (r *simpleRunner) Start() bool {
	if path, err := support.ExpandPathAbs(filepath.Join(support.GetBatchqHome(), "drain")); err == nil {
		os.Remove(path)
	}

	os.MkdirAll(r.spoolDir, 0755)

	// If we have no resource restrictions, then we will only run
	// one job at a time. That job can take up as many or few CPU/memory
	// as it wants, but there can be only one.
	maxConcurrentJobs := 1
	if r.maxProcs > 0 || r.maxMemoryMb > 0 {
		// jobs to run will be based on available resources
		maxConcurrentJobs = -1
	}

	r.availProcs = r.maxProcs
	r.availMem = r.maxMemoryMb
	r.availJobs = r.maxJobs

	keepRunning := true
	ranAtLeastOneJob := false

	// Channel to receive OS signals
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)

	sigCount := 0

	// Start signal handler in a goroutine
	go func() {
		draining := false
		lastIntT := time.Now().Add(-60 * time.Second)

		for sig := range sigs {
			fmt.Printf("\n\n*** Received signal: %v ***\n\n", sig)
			r.logf("")

			if !draining {
				if time.Since(lastIntT).Seconds() < 10 {
					draining = true
				} else {
					r.lock.Lock()
					r.logf("Currently running jobs (%d):\n", len(r.curJobs))
					for _, curJob := range r.curJobs {
						r.logf("  Job %s [proc: %d]\n", curJob.job.JobID, curJob.cmd.Process.Pid)
					}
					r.lock.Unlock()
					lastIntT = time.Now()
					r.logf("Hit Ctrl+C within 10 seconds to start draining jobs.\n")
				}
			}

			if draining {
				sigCount++
				switch sigCount {
				case 1:
					keepRunning = false
					r.logf("Waiting for jobs to complete. Hit Ctrl+C again to kill everything.\n")
					r.lock.Lock()
					if r.interrupt != nil {
						r.interrupt()
					}
					r.lock.Unlock()
				case 2:
					r.logf("Killing all running jobs...\n")
					for len(r.curJobs) > 0 {
						curJob := r.curJobs[0]
						r.curJobs = r.curJobs[1:]
						r.logf("Killing job %s [proc: %d]\n", curJob.job.JobID, curJob.cmd.Process.Pid)
						ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
						if err := r.client.CancelJob(ctx, curJob.job.JobID, "Shutting down runner"); err != nil {
							r.logf("Error canceling job %s: %v\n", curJob.job.JobID, err)
						}
						cancel()
						curJob.cmd.Cancel()
					}
					r.lock.Lock()
					if r.interrupt != nil {
						r.interrupt()
					}
					r.lock.Unlock()
				default:
					r.logf("Exiting... cleanup running jobs manually!!!\n")
					os.Exit(1)
				}
			}
		}
	}()

	r.logf("Starting job runner: %s\n", r.runnerId)
	if r.foreverMode {
		r.logf("Hit Ctrl+C to exit. Will try to drain jobs first.")
	}
	for keepRunning || len(r.curJobs) > 0 {
		// check to see if we need to cancel a running job
		for _, curJob := range r.curJobs {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

			// this could be optimized, but just iterating over jobs
			// is the easiest thing to do
			job, err := r.client.GetJob(ctx, curJob.job.JobID)
			cancel()
			if err != nil {
				r.logf("Error fetching job %s: %v\n", curJob.job.JobID, err)
				continue
			}
			if job.Status == jobs.CANCELED.String() {
				if curJob.cmd != nil && curJob.cmd.Cancel != nil {
					curJob.cmd.Cancel()
					continue
				} else {
					r.logf("Trying to cancel job: %s, but Cancel() is nil.\n", curJob.job.JobID)
				}
			}

			if job.Status == jobs.RUNNING.String() {
				// Check to see if the job has a walltime set...
				if val := job.Details["walltime"]; val != "" {
					if jobTime, err := strconv.Atoi(val); err != nil {
						log.Fatal(err)
					} else if jobTime > 0 {
						duration := time.Now().UTC().Sub(curJob.startTime)
						if duration.Seconds() > float64(jobTime) {
							r.logf("Job: %s exceeded wall-time (%s sec limit, %.2f sec elapsed)", curJob.job.JobID, val, duration.Seconds())
							cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
							if err := r.client.CancelJob(cctx, job.JobID, "exceeded wall-time"); err != nil {
								r.logf("Error canceling job %s: %v\n", job.JobID, err)
							}
							ccancel()
							curJob.cmd.Cancel()
							continue
						}
					}
				}
			}
		}

		if r.IsDrain() || !keepRunning {
			r.logf("Draining jobs (remaining:%d)\n", len(r.curJobs))
			if r.IsDrain() && len(r.curJobs) == 0 {
				keepRunning = false
				r.logf("Done processing jobs")
			}
		} else {
			// check to see if we have the resources to run a job
			findJob := false
			r.logf("Available resources (c:%d, m:%d)\n", r.availProcs, r.availMem)
			if maxConcurrentJobs > 0 {
				if len(r.curJobs) < maxConcurrentJobs {
					findJob = true
				}
			} else {
				if r.availMem > 0 && r.availProcs > 0 {
					findJob = true
				} else if r.availMem > 0 && r.availProcs == -1 {
					findJob = true
				} else if r.availProcs > 0 && r.availMem == -1 {
					findJob = true
				}
			}

			if keepRunning && r.availJobs == 0 {
				// No more jobs allowed
				findJob = false
				keepRunning = false
				r.logf("Reached maximum job count (%d). Not starting new jobs.\n", r.maxJobs)
			}

			// we have the resources... let's find a job to run
			if findJob {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

				resp, err := r.client.ClaimNextJob(ctx, r.runnerId, "simple", r.availProcs, r.availMem, r.maxWalltimeSec)
				cancel()
				if err != nil {
					r.logf("Error claiming job: %v\n", err)
				} else if resp.Job != nil {
					job := resp.Job
					r.logf("Processing job: %s\n", job.JobID)
					r.lock.Lock()
					if !r.startJob(job) {
						r.logf("Error starting job -- draining")
						keepRunning = false
					}
					r.lock.Unlock()
					ranAtLeastOneJob = true
					if r.availJobs > 0 {
						r.availJobs--
					}
					continue
				} else {
					if len(r.curJobs) == 0 && !resp.MoreEligible && !r.foreverMode {
						// we are done here
						// no jobs running, none left in queue, and we aren't waiting...
						keepRunning = false
						r.logf("Done processing jobs")
					}
				}
			}
		}

		// do we have more jobs to run? should we keep waiting to
		// run more jobs? if we are draining, let's wait to see if the jobs finish...

		if keepRunning || len(r.curJobs) > 0 {
			ctx, cancel := context.WithCancel(context.Background())
			r.lock.Lock()
			r.interrupt = cancel
			r.lock.Unlock()

			// sleep for 60 seconds to see if jobs complete
			if err := interruptibleSleep(ctx, 60*time.Second); err != nil {
				r.logf("Interrupted\n")
			}
			r.lock.Lock()
			r.interrupt = nil
			r.lock.Unlock()
		}
	}

	return ranAtLeastOneJob
}

func (r *simpleRunner) IsDrain() bool {
	if path, err := support.ExpandPathAbs(filepath.Join(support.GetBatchqHome(), "drain")); err == nil {
		return support.FileExists(path)
	}
	return false
}

func interruptibleSleep(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil // Sleep completed
	case <-ctx.Done():
		return ctx.Err() // Interrupted
	}
}

func (r *simpleRunner) startJob(job *api.JobDTO) bool {
	jobProcs := -1
	jobMem := -1

	if val := job.Details["procs"]; val != "" {
		if p, err := strconv.Atoi(val); err != nil {
			log.Fatal(err)
		} else {
			jobProcs = p
		}
	}
	if val := job.Details["mem"]; val != "" {
		if m, err := strconv.Atoi(val); err != nil {
			log.Fatal(err)
		} else {
			jobMem = m
		}
	}

	script := filepath.Join(r.spoolDir, fmt.Sprintf("batchq-%s.sh", job.JobID))
	if err := os.WriteFile(script, []byte(job.Details["script"]), 0755); err != nil {
		r.logf("Error writing script: %v\n", err)
		r.failClaimedJob(job.JobID, 1)
		return false
	}

	os.Chmod(script, 0755) // just in case it existed before

	cmd := exec.CommandContext(context.Background(), script)

	cmd.Cancel = func() error {
		if cmd.Process != nil {
			pgid, _ := syscall.Getpgid(cmd.Process.Pid)
			r.logf("Canceling job: %s [%d]\n", job.JobID, pgid)
			// the negative should kill all members of the pgid
			syscall.Kill(-pgid, syscall.SIGKILL)
			ps, err := cmd.Process.Wait()
			if err != nil {
				r.logf("Error killing process %d: %v\n", pgid, err)
			} else if ps != nil {
				r.logf("Killing process %d: %v\n", pgid, ps.String())
			} else {
				r.logf("Killing process %d: done\n", pgid)
			}
		} else {
			r.logf("Canceling job: %s, but process already done\n", job.JobID)
		}
		return nil
	}

	cmd.WaitDelay = 30 * time.Second

	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil

	var stdoutFile *os.File
	var stderrFile *os.File

	jobStdout := job.Details["stdout"]
	jobStderr := job.Details["stderr"]

	if jobStdout != "" {
		if filepath.Dir(jobStdout) != "." {
			os.MkdirAll(filepath.Dir(jobStdout), 0755)
		}
		if f, err := os.Create(jobStdout); err == nil {
			cmd.Stdout = f
			stdoutFile = f
		}
	}
	if jobStderr != "" {
		if jobStderr == jobStdout {
			cmd.Stderr = cmd.Stdout
		} else {
			if filepath.Dir(jobStderr) != "." {
				os.MkdirAll(filepath.Dir(jobStderr), 0755)
			}
			if f, err := os.Create(jobStderr); err == nil {
				cmd.Stderr = f
				stderrFile = f
			}
		}
	}
	if env := job.Details["env"]; env != "" {
		cmd.Env = strings.Split(env, "\n-|-\n")
		cmd.Env = append(cmd.Env, fmt.Sprintf("JOB_ID=%s", job.JobID))
		cmd.Env = append(cmd.Env, fmt.Sprintf("BATCHQ_JOB_ID=%s", job.JobID))
		cmd.Env = append(cmd.Env, fmt.Sprintf("BATCHQ_RUNNER_ID=%s", r.runnerId))
	} else {
		cmd.Env = []string{fmt.Sprintf("JOB_ID=%s", job.JobID)}
	}
	cmd.Dir = job.Details["wd"]

	// create a new progress group for this job (so we can kill them all if necessary)
	if support.AmIRoot() {
		uidS := job.Details["uid"]
		gidS := job.Details["gid"]

		uid, err := strconv.Atoi(uidS)
		if err != nil {
			r.logf("Missing UID from job details: %s\n", job.JobID)
			r.failClaimedJob(job.JobID, 1)
			return false
		}
		gid, err := strconv.Atoi(gidS)

		if err != nil {
			r.logf("Missing GID from job details: %s\n", job.JobID)
			r.failClaimedJob(job.JobID, 1)
			return false
		}
		cmd.SysProcAttr = &syscall.SysProcAttr{
			Setpgid: true,
			Credential: &syscall.Credential{
				Uid: uint32(uid),
				Gid: uint32(gid),
			},
		}

		if stdoutFile != nil {
			stdoutFile.Chown(uid, gid)
		}
		if stderrFile != nil {
			stderrFile.Chown(uid, gid)
		}
	} else {
		cmd.SysProcAttr = &syscall.SysProcAttr{
			Setpgid: true,
		}
	}

	cgroupV2Path := fmt.Sprintf("/sys/fs/cgroup/batchq-%s", job.JobID)
	cgroupV1BaseCPU := fmt.Sprintf("/sys/fs/cgroup/cpu/batchq-%s", job.JobID)
	cgroupV1BaseMem := fmt.Sprintf("/sys/fs/cgroup/memory/batchq-%s", job.JobID)
	if r.useCgroupV2 && support.AmIRoot() {

		// setup cgroups

		// Create the cgroup directory
		if err := os.MkdirAll(cgroupV2Path, 0755); err != nil {
			panic(err)
		}

		if jobProcs > 0 {
			support.MustWriteFile(filepath.Join(cgroupV2Path, "cpu.max"), fmt.Sprintf("%d 100000", 100000*jobProcs)) // 2 CPUs (200ms every 100ms)
		}
		if jobMem > 0 {
			support.MustWriteFile(filepath.Join(cgroupV2Path, "memory.max"), fmt.Sprintf("%dM", jobMem)) // 100MB limit
		}
	} else if r.useCgroupV1 && support.AmIRoot() {
		_ = os.MkdirAll(cgroupV1BaseCPU, 0755)
		_ = os.MkdirAll(cgroupV1BaseMem, 0755)
		// Set CPU quota (e.g., 2 CPUs assuming 100000us period)
		support.MustWriteFile(filepath.Join(cgroupV1BaseCPU, "cpu.cfs_period_us"), "100000")
		support.MustWriteFile(filepath.Join(cgroupV1BaseCPU, "cpu.cfs_quota_us"), fmt.Sprintf("%d", 100000*jobProcs))

		// Set memory limit
		support.MustWriteFile(filepath.Join(cgroupV1BaseMem, "memory.limit_in_bytes"), fmt.Sprintf("%d", jobMem*1024*1024)) //  MB

	}

	err := cmd.Start()
	if err != nil {
		r.logf("Error starting jobs: %v\n", err)
		r.failClaimedJob(job.JobID, 1)
		return false
	}
	if r.useCgroupV2 && support.AmIRoot() {
		pid := strconv.Itoa(cmd.Process.Pid)
		support.MustWriteFile(filepath.Join(cgroupV2Path, "cgroup.procs"), pid)
	} else if r.useCgroupV1 && support.AmIRoot() {
		pid := strconv.Itoa(cmd.Process.Pid)
		support.MustWriteFile(filepath.Join(cgroupV1BaseCPU, "tasks"), pid)
		support.MustWriteFile(filepath.Join(cgroupV1BaseMem, "tasks"), pid)
	}

	myStatus := jobStatus{job: job, running: false, returnCode: 0, cmd: cmd, startTime: time.Now()}
	r.curJobs = append(r.curJobs, myStatus)

	if r.availProcs != -1 && jobProcs > 0 {
		r.availProcs = r.availProcs - jobProcs
		r.logf("Job %s: Reserving %d proc (%d remaining)\n", job.JobID, jobProcs, r.availProcs)
	}
	if r.availMem != -1 && jobMem > 0 {
		r.availMem = r.availMem - jobMem
	}

	// Push the pid as a running detail. The claim already transitioned the
	// job to RUNNING; this just decorates it with the host-side handle.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := r.client.UpdateRunningDetails(ctx, r.runnerId, job.JobID, map[string]string{"pid": strconv.Itoa(cmd.Process.Pid)}); err != nil {
		r.logf("Error updating job running details %s: %v\n", job.JobID, err)
	}
	cancel()
	r.logf("Started job: %s [proc: %d] (c:%d, m:%d)\n", job.JobID, cmd.Process.Pid, jobProcs, jobMem)

	// Track it in the background
	go func() {
		state, _ := cmd.Process.Wait()
		if stdoutFile != nil {
			stdoutFile.Close()
		}
		if stderrFile != nil {
			stderrFile.Close()
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		r.lock.Lock()
		r.logf("Job %s [proc: %d] exited: %s\n", job.JobID, cmd.Process.Pid, state.String())
		var endErr error
		if state != nil && state.Success() {
			endErr = r.client.EndJob(ctx, r.runnerId, job.JobID, 0)
		} else if state != nil {
			endErr = r.client.EndJob(ctx, r.runnerId, job.JobID, state.ExitCode())
		} else {
			endErr = r.client.EndJob(ctx, r.runnerId, job.JobID, -1)
		}
		if endErr != nil && !errors.Is(endErr, client.ErrInvalidState) {
			r.logf("Error finalizing job %s: %v\n", job.JobID, endErr)
		}
		os.Remove(script)

		if r.useCgroupV2 && support.AmIRoot() {
			os.RemoveAll(cgroupV2Path)
		} else if r.useCgroupV1 && support.AmIRoot() {
			cleanupCgroupV1([]string{cgroupV1BaseCPU, cgroupV1BaseMem})
		}

		var newJobs []jobStatus
		for _, val := range r.curJobs {
			if val.job.JobID != myStatus.job.JobID {
				newJobs = append(newJobs, val)
			}
		}

		r.curJobs = newJobs
		if r.availProcs != -1 && jobProcs > 0 {
			r.availProcs = r.availProcs + jobProcs
			r.logf("Job %s: Returning %d proc (%d remaining)\n", job.JobID, jobProcs, r.availProcs)
		}
		if r.availMem != -1 && jobMem > 0 {
			r.availMem = r.availMem + jobMem
		}

		if r.interrupt != nil {
			r.interrupt()
		}
		r.lock.Unlock()

	}()

	return true
}

// failClaimedJob marks a claimed job as FAILED when the runner couldn't even
// get the process started. Without this the job would stay stuck in RUNNING
// — the atomic claim already flipped the status before we discovered the
// host-side problem.
func (r *simpleRunner) failClaimedJob(jobID string, code int) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := r.client.EndJob(ctx, r.runnerId, jobID, code); err != nil {
		r.logf("Error marking claimed job %s as failed: %v\n", jobID, err)
	}
}

func cleanupCgroupV1(paths []string) error {
	for _, path := range paths {
		tasksFile := filepath.Join(path, "tasks")

		data, err := os.ReadFile(tasksFile)
		if err != nil {
			return fmt.Errorf("read tasks file: %v", err)
		}

		for _, line := range strings.Split(string(data), "\n") {
			if strings.TrimSpace(line) == "" {
				continue
			}
			// Move process back to root
			if err := os.WriteFile(filepath.Join(filepath.Dir(path), "tasks"), []byte(line), 0644); err != nil {
				return fmt.Errorf("move PID %s: %v", line, err)
			}
		}

		// Now remove the cgroup directory
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("remove %s: %v", path, err)
		}
	}
	return nil
}
