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
	// just created (should be changed shortly)
	UNKNOWN StatusCode = iota

	// waiting to be manually released
	USERHOLD
	// waiting for a dependency to finish
	WAITING
	// ready to run, pending resources
	QUEUED

	// submitted to a different queue (e.g. SLURM),
	// all dependencies are handled there
	// next step is CANCELED, SUCCESS, or FAILED
	PROXYQUEUED

	// job is currently running in a batchq runner
	// next step is CANCELED, SUCCESS, or FAILED
	RUNNING

	// end state: job was canceled
	CANCELED
	// end state: job finished successfully
	SUCCESS
	// end state: job finished unsuccessfully
	FAILED
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
	case CANCELED:
		return "CANCELED"
	case PROXYQUEUED:
		return "PROXYQUEUED"
	case SUCCESS:
		return "SUCCESS"
	case FAILED:
		return "FAILED"
	default:
		return "INVALID"
	}
}

type JobDef struct {
	JobId          int
	Status         StatusCode
	Name           string
	Notes          string
	SubmitTime     time.Time
	StartTime      time.Time
	EndTime        time.Time
	ReturnCode     int
	AfterOk        []int
	Details        []JobDefDetail
	RunningDetails []JobRunningDetail
}

type JobDefDetail struct {
	Key   string
	Value string
}

type JobRunningDetail struct {
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
func (job *JobDef) GetRunningDetail(key string, defval string) string {
	for _, detail := range job.RunningDetails {
		if detail.Key == key {
			return detail.Value
		}
	}
	return defval
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
	fmt.Printf("notes    : %s\n", job.Notes)
	fmt.Printf("submit   : %v\n", job.SubmitTime)
	fmt.Printf("start    : %v\n", job.StartTime)
	fmt.Printf("end      : %v\n", job.EndTime)

	if len(job.AfterOk) > 0 {
		fmt.Printf("after-ok : %s\n", support.JoinInt(job.AfterOk, ","))
	}

	fmt.Printf("---[job details]---\n")

	var script string
	maxKeyLen := 0
	for _, detail := range job.Details {
		if len(detail.Key) > maxKeyLen {
			maxKeyLen = len(detail.Key)
		}
	}
	if len(job.Details) > 1 {
		// there must be at least "script", so
		// only do this if there are other details
		for _, detail := range job.Details {
			switch detail.Key {
			case "script":
				script = detail.Value
			case "env":
				fmt.Printf("%-*s : %-60.60s...\n", maxKeyLen, detail.Key, strings.ReplaceAll(detail.Value, "\n-|-\n", ";"))
			default:
				fmt.Printf("%-*s : %s\n", maxKeyLen, detail.Key, detail.Value)
			}
		}
	}
	if len(job.RunningDetails) > 0 {
		fmt.Printf("---[job running details]---\n")
		maxKeyLen = 0
		for _, detail := range job.RunningDetails {
			if len(detail.Key) > maxKeyLen {
				maxKeyLen = len(detail.Key)
			}
		}
		for _, detail := range job.RunningDetails {
			fmt.Printf("%-*s : %s\n", maxKeyLen, detail.Key, detail.Value)
		}
	}
	fmt.Printf("---[job script]---\n%s\n", script)
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

	// DD-HH:MM:SS
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

	// HH:MM:SS
	if strings.Contains(val, ":") {
		spl := strings.Split(val, ":")

		if len(spl) > 2 && spl[len(spl)-3] != "" {
			if num, err := strconv.Atoi(spl[len(spl)-3]); err != nil {
				return -1
			} else {
				hours = num
			}
		}
		if len(spl) > 1 && spl[len(spl)-2] != "" {
			if num, err := strconv.Atoi(spl[len(spl)-2]); err != nil {
				return -1
			} else {
				minutes = num
			}
		}
		if spl[len(spl)-1] != "" {
			if num, err := strconv.Atoi(spl[len(spl)-1]); err != nil {
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
func WalltimeToString(secs int) string {
	mins := 0
	hours := 0
	days := 0

	if secs >= 60 {
		mins = secs / 60
		secs = secs - mins*60
	}
	if mins >= 60 {
		hours = mins / 60
		mins = mins - hours*60
	}
	if hours >= 24 {
		days = hours / 24
		hours = hours - days*24
	}

	ret := ""
	if days > 0 {
		ret += fmt.Sprintf("%dd", days)
	}
	if hours > 0 {
		ret += fmt.Sprintf("%dh", hours)
	}
	if mins > 0 {
		ret += fmt.Sprintf("%dm", mins)
	}
	if secs > 0 {
		ret += fmt.Sprintf("%ds", secs)
	}
	return ret
}

// write walltime as a string (input is sec as string)
func WalltimeStringToString(secs string) string {
	if secs != "" {
		if val, err := strconv.Atoi(secs); err == nil {
			return WalltimeToString(val)
		}
	}
	return ""
}
