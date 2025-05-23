package runner

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/mbreese/batchq/db"
	"github.com/mbreese/batchq/jobs"
)

type simpleRunner struct {
	db             db.BatchDB
	maxProcs       int
	maxMemoryMb    int
	maxWalltimeSec int
	foreverMode    bool
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

	fmt.Printf("Starting job runner: %s\n", r.runnerId)
	for keepRunning {

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
					fmt.Printf("Trying to cancel job: %d, but Cancel() is nil.\n", curJob.job.JobId)
				}
			}

			if job.Status == jobs.RUNNING {
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

	cmd := exec.CommandContext(context.Background(), r.shellBin)

	cmd.Cancel = func() error {
		if cmd.Process != nil {
			log.Printf("Cancelling job: %d [%d]\n", job.JobId, cmd.Process.Pid)
			cmd.Process.Kill()
		} else {
			log.Printf("Cancelling job: %d, but process already done\n", job.JobId)
			cmd.Process.Kill()
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

	err := cmd.Start()
	if err != nil {
		log.Fatal(err)
	}

	myStatus := jobStatus{job: job, running: false, returnCode: 0, cmd: cmd}
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
