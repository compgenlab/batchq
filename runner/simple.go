package runner

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/mbreese/batchq/db"
	"github.com/mbreese/batchq/jobs"
	"github.com/mbreese/batchq/support"
)

type simpleRunner struct {
	db             db.BatchDB
	maxProcs       int
	maxMemoryMb    int
	maxWalltimeSec int
	foreverMode    bool
	useCgroupV2    bool
	useCgroupV1    bool
	curJobs        []jobStatus
	availProcs     int
	availMem       int
	shellBin       string
	runnerId       string
	interrupt      context.CancelFunc
	lock           sync.Mutex
}

type jobStatus struct {
	job        *jobs.JobDef
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
func NewSimpleRunner(jobq db.BatchDB) *simpleRunner {
	id := uuid.New()
	runner := simpleRunner{db: jobq, maxProcs: -1, maxMemoryMb: -1, maxWalltimeSec: -1, foreverMode: false, runnerId: id.String()}
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

func (r *simpleRunner) Start() bool {

	// If we have no resource restrictions, then we will only run
	// one job at a time. That job can take up as many or few CPU/memory
	// as it wants, but there can be only one.
	maxJobs := 1
	if r.maxProcs > 0 || r.maxMemoryMb > 0 {
		// jobs to run will be based on available resources
		maxJobs = -1
	}

	r.availProcs = r.maxProcs
	r.availMem = r.maxMemoryMb

	keepRunning := true
	ranAtLeastOneJob := false

	log.Printf("Starting job runner: %s\n", r.runnerId)
	for keepRunning {
		now := time.Now()
		// check to see if we need to cancel a running job
		for _, curJob := range r.curJobs {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			// this could be optimized, but just iterating over jobs
			// is the easiest thing to do
			job := r.db.GetJob(ctx, curJob.job.JobId)
			if job.Status == jobs.CANCELLED {
				if curJob.cmd != nil && curJob.cmd.Cancel != nil {
					curJob.cmd.Cancel()
					continue
				} else {
					log.Printf("Trying to cancel job: %d, but Cancel() is nil.\n", curJob.job.JobId)
				}
			}

			if job.Status == jobs.RUNNING {
				// Check to see if the job has a walltime set...
				if val := job.GetDetail("walltime", ""); val != "" {
					if jobTime, err := strconv.Atoi(val); err != nil {
						log.Fatal(err)
					} else if jobTime > 0 {
						duration := now.Sub(curJob.startTime)
						if duration.Seconds() > float64(jobTime) {
							log.Printf("Job: %d exceeded wall-time (%s sec limit, %.2f sec elapsed)", curJob.job.JobId, val, duration.Seconds())
							curJob.cmd.Cancel()
							continue
						}
					}
				}

				// TODO: also check here to see if the job has been running for
				//       too long. We can check the wall-time here. If the job
				//       has been running too long, then just kill it here.

			}
		}

		// check to see if we have the resources to run a job
		findJob := false
		if maxJobs > 0 {
			if len(r.curJobs) < maxJobs {
				findJob = true
			}
		} else {
			fmt.Printf("Available resources (procs:%d, mem:%d)\n", r.availProcs, r.availMem)
			if r.availMem > 0 && r.availProcs > 0 {
				findJob = true
			} else if r.availMem > 0 && r.availProcs == -1 {
				findJob = true
			} else if r.availProcs > 0 && r.availMem == -1 {
				findJob = true
			}
		}

		// we have the resources... let's find a job to run
		if findJob {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			// we should be looking for a new job
			log.Printf("Looking for a job (%d, %d, %d)\n", r.availProcs, r.availMem, r.maxWalltimeSec)

			if job, hasMore := r.db.FetchNext(ctx, r.availProcs, r.availMem, r.maxWalltimeSec); job != nil {
				log.Printf("Processing job: %d\n", job.JobId)
				r.lock.Lock()
				r.startJob(job)
				r.lock.Unlock()
				ranAtLeastOneJob = true
				continue
			} else {
				if len(r.curJobs) == 0 && !hasMore && !r.foreverMode {
					// we are done here
					// no jobs running, none left in queue, and we aren't waiting...
					keepRunning = false
					log.Println("Done processing jobs")
				}
			}
		}

		// do we have more jobs to run? should we keep waiting to
		// run more jobs?

		if keepRunning {
			ctx, cancel := context.WithCancel(context.Background())
			r.interrupt = cancel
			// fmt.Println("Sleeping")

			// sleep for 60 seconds to see if jobs complete
			if err := interruptibleSleep(ctx, 60*time.Second); err != nil {
				// fmt.Println("Woken up...")
			}
			r.lock.Lock()
			r.interrupt = nil
			r.lock.Unlock()
		}
		// time.Sleep(60 * time.Second)
	}

	return ranAtLeastOneJob
}

func interruptibleSleep(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil // Sleep completed
	case <-ctx.Done():
		return ctx.Err() // Interrupted
	}
}

func (r *simpleRunner) startJob(job *jobs.JobDef) {
	jobProcs := -1
	jobMem := -1

	if val := job.GetDetail("procs", ""); val != "" {
		if p, err := strconv.Atoi(val); err != nil {
			log.Fatal(err)
		} else {
			jobProcs = p
		}
	}
	if val := job.GetDetail("mem", ""); val != "" {
		if m, err := strconv.Atoi(val); err != nil {
			log.Fatal(err)
		} else {
			jobMem = m
		}
	}

	// prog := r.shellBin
	// args := []string{}
	// if r.useSystemdRun {
	// 	prog = "systemd-run"

	// 	uname := job.GetDetail("user", "")
	// 	if uname == "" {
	// 		log.Printf("Unable to start job: %d. Missing username and trying to run under systemd-run.\n", job.JobId)
	// 		os.Exit(1)
	// 	}
	// 	args = []string{"--scope", "--uid", uname}

	// 	if jobProcs > 0 {
	// 		args = append(args, "-p", fmt.Sprintf("CPUQuota=%d%%", jobProcs*100))
	// 	}
	// 	if jobMem > 0 {
	// 		args = append(args, "-p", fmt.Sprintf("MemoryMax=%dM", jobMem))
	// 	}

	// 	args = append(args, r.shellBin)
	// }

	// log.Println(prog, args)

	// cmd := exec.CommandContext(context.Background(), prog, args...)
	cmd := exec.CommandContext(context.Background(), r.shellBin)

	cmd.Cancel = func() error {
		if cmd.Process != nil {
			pgid, _ := syscall.Getpgid(cmd.Process.Pid)
			log.Printf("Cancelling job: %d [%d]\n", job.JobId, cmd.Process.Pid)
			log.Println("Killing process group:", pgid)
			syscall.Kill(-pgid, syscall.SIGTERM)
			cmd.Process.Wait()
			log.Println("Process killed")
		} else {
			log.Printf("Cancelling job: %d, but process already done\n", job.JobId)
			// cmd.Process.Kill()
		}
		return nil
	}

	cmd.WaitDelay = 30 * time.Second

	cmd.Stdin = strings.NewReader(job.GetDetail("script", ""))
	cmd.Stdout = nil
	cmd.Stderr = nil

	var stdoutFile *os.File
	var stderrFile *os.File

	if stdout := job.GetDetail("stdout", ""); stdout != "" {
		if f, err := os.Create(stdout); err == nil {
			cmd.Stdout = f
			stdoutFile = f
		}
	}
	if stderr := job.GetDetail("stderr", ""); stderr != "" {
		if f, err := os.Create(stderr); err == nil {
			cmd.Stderr = f
			stderrFile = f
		}
	}
	if env := job.GetDetail("env", ""); env != "" {
		cmd.Env = strings.Split(fmt.Sprintf("%s\nJOB_ID=%d", env, job.JobId), "\n")
	} else {
		cmd.Env = append(os.Environ(), fmt.Sprintf("JOB_ID=%d", job.JobId))
	}
	cmd.Dir = job.GetDetail("wd", "")

	// create a new progress group for this job (so we can kill them all if necessary)
	if support.AmIRoot() {
		uidS := job.GetDetail("uid", "")
		gidS := job.GetDetail("gid", "")

		uid, err := strconv.Atoi(uidS)
		if err != nil {
			log.Fatalf("Missing UID from job details: %d\n", job.JobId)
		}
		gid, err := strconv.Atoi(gidS)

		if err != nil {
			log.Fatalf("Missing GID from job details: %d\n", job.JobId)
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

	cgroupV2Path := fmt.Sprintf("/sys/fs/cgroup/batchq-%d", job.JobId)
	cgroupV1BaseCPU := fmt.Sprintf("/sys/fs/cgroup/cpu/batchq-%d", job.JobId)
	cgroupV1BaseMem := fmt.Sprintf("/sys/fs/cgroup/memory/batchq-%d", job.JobId)
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
		log.Fatal(err)
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
	}
	if r.availMem != -1 && jobMem > 0 {
		r.availMem = r.availMem - jobMem
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r.db.StartJob(ctx, job.JobId, r.runnerId, map[string]string{"pid": strconv.Itoa(cmd.Process.Pid)})
	ctx.Done()
	log.Printf("Started job: %d [%d]\n%s\n", job.JobId, cmd.Process.Pid, job.GetDetail("script", ""))

	// Track it in the background
	go func() {
		err := cmd.Wait()
		if stdoutFile != nil {
			stdoutFile.Close()
		}
		if stderrFile != nil {
			stderrFile.Close()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err != nil {
			log.Printf("Process %d exited with error: %v\n", cmd.Process.Pid, err)
			r.db.EndJob(ctx, job.JobId, r.runnerId, err.(*exec.ExitError).ExitCode())
		} else {
			log.Printf("Process %d exited successfully\n", cmd.Process.Pid)
			r.db.EndJob(ctx, job.JobId, r.runnerId, 0)
		}
		ctx.Done()

		if r.useCgroupV2 && support.AmIRoot() {
			os.RemoveAll(cgroupV2Path)
		} else if r.useCgroupV1 && support.AmIRoot() {
			cleanupCgroupV1([]string{cgroupV1BaseCPU, cgroupV1BaseMem})
		}

		r.lock.Lock()
		var newJobs []jobStatus
		for _, val := range r.curJobs {
			if val != myStatus {
				newJobs = append(newJobs, val)
			}
		}

		r.curJobs = newJobs
		if r.availProcs != -1 && jobProcs > 0 {
			r.availProcs = r.availProcs + jobProcs
		}
		if r.availMem != -1 && jobMem > 0 {
			r.availMem = r.availMem + jobMem
		}

		if r.interrupt != nil {
			r.interrupt()
		}
		r.lock.Unlock()

	}()

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
