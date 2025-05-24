package jobs

import (
	"fmt"
	"os/user"
	"strconv"
	"strings"
	"time"

	"github.com/mbreese/batchq/support"
)

type StatusCode int

const (
	UNKNOWN   StatusCode = iota // just created
	USERHOLD                    // waiting to be manually released
	WAITING                     // waiting for a dependency to finish
	QUEUED                      // ready to run, pending resources
	RUNNING                     // job is currently running
	CANCELLED                   // job was cancelled
	SUCCESS                     // job finished successfully
	FAILED                      // job finished unsuccessfully
)

func (s StatusCode) String() string {
	switch s {
	case UNKNOWN:
		return "UNKNOWN"
	case USERHOLD:
		return "USERHOLD"
	case WAITING:
		return "WAITING"
	case QUEUED:
		return "QUEUED"
	case RUNNING:
		return "RUNNING"
	case CANCELLED:
		return "CANCELLED"
	case SUCCESS:
		return "SUCCESS"
	case FAILED:
		return "FAILED"
	default:
		return "INVALID"
	}
}

type JobDef struct {
	JobId      int
	Status     StatusCode
	Name       string
	SubmitTime time.Time
	StartTime  time.Time
	EndTime    time.Time
	ReturnCode int
	AfterOk    []int
	Details    []JobDefDetail
}

type JobDefDetail struct {
	Key   string
	Value string
}

func NewJobDef(name string, src string) *JobDef {
	jobdef := &JobDef{JobId: -1, Status: UNKNOWN, Name: name}
	jobdef.Details = make([]JobDefDetail, 0)

	// username := support.GetCurrentUsername()
	if u, err := user.Current(); err == nil {
		jobdef.
			AddDetail("uid", u.Uid).
			AddDetail("gid", u.Gid)
	}

	jobdef.AddDetail("script", src)

	return jobdef
}

func (job *JobDef) GetDetail(key string, defval string) string {
	for _, detail := range job.Details {
		if detail.Key == key {
			return detail.Value
		}
	}
	return defval
}

func (job *JobDef) AddDetail(key string, val string) *JobDef {
	if job.JobId == -1 {
		// can only alter a job that hasn't been submitted.
		job.Details = append(job.Details, JobDefDetail{Key: key, Value: val})
	}
	return job
}

func (job *JobDef) AddAfterOk(depId int) *JobDef {
	// can only alter a job that hasn't been submitted.
	if job.JobId == -1 {
		// must be unique... (DB constraint)
		if !support.Contains(job.AfterOk, depId) {
			job.AfterOk = append(job.AfterOk, depId)
		}
	}
	return job
}

func (job *JobDef) Print() {
	fmt.Printf("jobid    : %d\n", job.JobId)
	fmt.Printf("status   : %s\n", job.Status.String())
	fmt.Printf("name     : %s\n", job.Name)
	fmt.Printf("submit   : %v\n", job.SubmitTime)
	fmt.Printf("start    : %v\n", job.StartTime)
	fmt.Printf("end      : %v\n", job.EndTime)

	if len(job.AfterOk) > 0 {
		fmt.Printf("after-ok : %s\n", support.JoinInt(job.AfterOk, ","))
	}

	var script string
	for _, detail := range job.Details {
		if detail.Key == "script" {
			script = detail.Value
		} else {
			fmt.Printf("%-8s : %s\n", detail.Key, detail.Value)
		}
	}
	fmt.Printf("\n%s\n", script)
}

// Return value in MB (trim M/MB/G/GB suffix, if ends in G or GB, multiply by 1000)
func ParseMemoryString(val string) int {
	jobMem := -1

	if val != "" {
		// we still need to parse it
		if strings.ToUpper(val[len(val)-2:]) == "GB" {
			if mem, err := strconv.Atoi(val[:len(val)-2]); err == nil {
				jobMem = mem * 1000
			}
		} else if strings.ToUpper(val[len(val)-1:]) == "G" {
			if mem, err := strconv.Atoi(val[:len(val)-1]); err == nil {
				jobMem = mem * 1000
			}
		} else if strings.ToUpper(val[len(val)-2:]) == "MB" {
			if mem, err := strconv.Atoi(val[:len(val)-2]); err == nil {
				jobMem = mem
			}
		} else if strings.ToUpper(val[len(val)-1:]) == "M" {
			if mem, err := strconv.Atoi(val[:len(val)-1]); err == nil {
				jobMem = mem
			}
		} else {
			if mem, err := strconv.Atoi(val); err == nil {
				jobMem = mem
			}
		}
	}
	return jobMem
}

// write memory as a string
func PrintMemory(mem int) string {
	if mem > 1000 {
		gb := float64(mem) / 1000
		return fmt.Sprintf("%.2fGB", gb)
	}
	return fmt.Sprintf("%dMB", mem)
}

// write memory as a string
func PrintMemoryString(mem string) string {
	if mem != "" {
		if val, err := strconv.Atoi(mem); err == nil {
			return PrintMemory(val)
		}
	}
	return ""
}

// Parse wall time string: DD-HH:MM:SS
// return value in seconds
func ParseWalltimeString(val string) int {
	days := 0
	hours := 0
	minutes := 0
	seconds := -1

	if strings.Contains(val, "-") {
		spl := strings.SplitN(val, "-", 2)
		if num, err := strconv.Atoi(spl[0]); err != nil {
			fmt.Printf("%v", spl)
			return -1
		} else {
			days = num
			val = spl[1]
		}
	}
	if strings.Contains(val, ":") {
		spl := strings.Split(val, ":")
		if len(spl) == 3 {
			// HH:MM:SS
			if num, err := strconv.Atoi(spl[0]); err != nil {
				return -1
			} else {
				hours = num
			}
			if num, err := strconv.Atoi(spl[1]); err != nil {
				return -1
			} else {
				minutes = num
			}
			if num, err := strconv.Atoi(spl[2]); err != nil {
				return -1
			} else {
				seconds = num
			}
		} else if len(spl) == 2 {
			// MM:SS
			if num, err := strconv.Atoi(spl[1]); err != nil {
				return -1
			} else {
				minutes = num
			}
			if num, err := strconv.Atoi(spl[2]); err != nil {
				return -1
			} else {
				seconds = num
			}
		} else if len(spl) == 1 {
			// SS
			if num, err := strconv.Atoi(spl[2]); err != nil {
				return -1
			} else {
				seconds = num
			}
		}
	} else {
		// Treat everything else as the number of seconds...
		if num, err := strconv.Atoi(val); err != nil {
			return -1
		} else {
			seconds = num
		}

	}

	seconds += days * 60 * 60 * 24
	seconds += hours * 60 * 60
	seconds += minutes * 60

	return seconds
}

// write walltime as a string
func PrintWalltime(secs int) string {
	mins := 0
	hours := 0
	days := 0

	if secs > 60 {
		mins = secs / 60
		secs = secs - mins*60
	}
	if mins > 60 {
		hours = mins / 60
		mins = mins - hours*60
	}
	if hours > 24 {
		days = hours / 24
		hours = hours - days*24
	}

	if days > 0 {
		return fmt.Sprintf("%d-%02d:%02d:%02d", days, hours, mins, secs)
	}
	return fmt.Sprintf("%02d:%02d:%02d", hours, mins, secs)
}

// write walltime as a string (input is sec as string)
func PrintWalltimeString(secs string) string {
	if secs != "" {
		if val, err := strconv.Atoi(secs); err == nil {
			return PrintWalltime(val)
		}
	}
	return ""
}
