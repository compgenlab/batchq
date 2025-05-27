package cmd

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/mbreese/batchq/db"
	"github.com/mbreese/batchq/jobs"
	"github.com/mbreese/batchq/support"
	"github.com/spf13/cobra"
)

var submitCmd = &cobra.Command{
	Use:   "submit {args} filename/cmd",
	Short: "Submit a new job",

	Run: func(cmd *cobra.Command, args []string) {
		var scriptSrc string

		if len(args) > 0 {
			if f, err := os.Open(args[0]); err == nil {
				defer f.Close()
				data, err := io.ReadAll(f)
				if err != nil {
					fmt.Println("Error:", err)
					return
				}
				scriptSrc = string(data)
			} else {
				scriptSrc = fmt.Sprintf("#!/bin/sh\n%s\n", strings.Join(args, " "))
			}
		} else {
			fi, err := os.Stdin.Stat()
			if err != nil {
				panic(err)
			}
			if fi.Mode()&os.ModeCharDevice == 0 {
				// there is data waiting... so read it. (otherwise, stdin is a terminal and has no data)
				data, err := io.ReadAll(os.Stdin)
				if err != nil {
					fmt.Println("Error:", err)
					return
				}
				scriptSrc = string(data)
			}
		}

		if scriptSrc == "" {
			cmd.Help()
			return
		}

		details := make(map[string]string)

		// these are the default values...
		details["stdout"] = "./batchq-%JOBID.stdout"
		details["stderr"] = "./batchq-%JOBID.stderr"
		details["wd"] = "."

		// get default/config values if not specified
		if tmp, ok := Config.GetInt("job_defaults", "procs"); ok && tmp > 0 {
			details["procs"] = strconv.Itoa(tmp)
		}
		if tmp, ok := Config.Get("job_defaults", "mem"); ok {
			details["mem"] = strconv.Itoa(jobs.ParseMemoryString(tmp))
		}
		if tmp, ok := Config.Get("job_defaults", "walltime"); ok {
			details["walltime"] = strconv.Itoa(jobs.ParseWalltimeString(tmp))
		}
		if tmp, ok := Config.Get("job_defaults", "wd"); ok {
			details["wd"] = tmp
		}
		if tmp, ok := Config.Get("job_defaults", "stdout"); ok {
			details["stdout"] = tmp
		}
		if tmp, ok := Config.Get("job_defaults", "stderr"); ok {
			details["stderr"] = tmp
		}
		if tmp, ok := Config.GetBool("job_defaults", "hold"); ok && tmp {
			jobHold = true
		}
		if tmp, ok := Config.GetBool("job_defaults", "env"); ok && tmp {
			jobEnv = true
		}

		// process values from script prefixed with #BATCHQ
		//
		// supported values:
		// #BATCHQ -name val
		// #BATCHQ -procs N
		// #BATCHQ -mem val
		// #BATCHQ -walltime d-h:m:s
		// #BATCHQ -wd val
		// #BATCHQ -stdout val
		// #BATCHQ -stderr val
		// #BATCHQ -env
		// #BATCHQ -hold
		// #BATCHQ -afterok val1,val2

		incomment := true
		for _, line := range strings.Split(scriptSrc, "\n") {
			if incomment && strings.TrimSpace(line) != "" {
				if line[0] != '#' {
					incomment = false
				}
				if !slurmMode {
					// batchq mode, with #BATCHQ headers
					if len(line) > 9 && line[:9] == "#BATCHQ -" {
						sub := line[9:]
						spl := strings.SplitN(sub, " ", 2)
						k := strings.TrimSpace(spl[0])
						v := ""
						if len(spl) > 1 {
							v = strings.TrimSpace(spl[1])
						}

						switch k {
						case "procs", "p":
							if procs, err := strconv.Atoi(v); err != nil {
								log.Fatalf("Bad value for -procs: %s", v)
							} else {
								if procs > 0 {
									details["procs"] = strconv.Itoa(procs)
								} else {
									log.Fatalf("Bad value for -procs: %s", v)
								}
							}
						case "mem", "m":
							if v == "" {
								log.Fatal("Missing value for -mem")
							}
							details["mem"] = strconv.Itoa(jobs.ParseMemoryString(v))
						case "walltime", "t":
							if v == "" {
								log.Fatal("Missing value for -walltime")
							}
							details["walltime"] = strconv.Itoa(jobs.ParseWalltimeString(v))
						case "name":
							if v == "" {
								log.Fatal("Missing value for -name")
							}
							if jobName == "" {
								// if this is set in the cmdline, don't reset it here.
								jobName = v
							}
						case "wd":
							if v == "" {
								log.Fatal("Missing value for -wd")
							}
							details["wd"] = v
						case "stdout":
							if v == "" {
								log.Fatal("Missing value for -stdout")
							}
							details["stdout"] = v
						case "stderr":
							if v == "" {
								log.Fatal("Missing value for -stderr")
							}
							details["stderr"] = v
						case "env":
							jobEnv = true
						case "hold":
							jobHold = true
						case "afterok":
							if jobDeps == "" {
								jobDeps = v
							}
						default:
						}
					}
				} else {
					// process values from a SLURM script
					//
					// values can be keys as -X val or --long-x=val

					var k string
					var v string
					if len(line) > 10 && line[:10] == "#SBATCH --" {
						sub := line[10:]
						spl := strings.SplitN(sub, "=", 2)
						k = strings.TrimSpace(spl[0])
						v = strings.TrimSpace(spl[1])
					} else if len(line) > 9 && line[:9] == "#SBATCH -" {
						sub := line[9:]
						spl := strings.SplitN(sub, " ", 2)
						k = strings.TrimSpace(spl[0])
						v = strings.TrimSpace(spl[1])
					}
					if k != "" {
						switch k {
						case "c", "cpus-per-task":
							if procs, err := strconv.Atoi(v); err != nil {
								log.Fatalf("Bad value for -c: %s", v)
							} else {
								if procs > 0 {
									details["procs"] = strconv.Itoa(procs)
								} else {
									log.Fatalf("Bad value for -c: %s", v)
								}
							}
						case "mem":
							if v == "" {
								log.Fatal("Missing value for --mem")
							}
							details["mem"] = strconv.Itoa(jobs.ParseMemoryString(v))
						case "t", "time":
							if v == "" {
								log.Fatal("Missing value for -walltime")
							}
							details["walltime"] = strconv.Itoa(jobs.ParseWalltimeString(v))
						case "J", "job-name":
							if v == "" {
								log.Fatal("Missing value for -J")
							}
							if jobName == "" {
								// if this is set in the cmdline, don't reset it here.
								jobName = v
							}
						case "D", "chdir":
							if v == "" {
								log.Fatal("Missing value for -D")
							}
							details["wd"] = v
						case "o", "output":
							if v == "" {
								log.Fatal("Missing value for -stdout")
							}
							details["stdout"] = strings.Replace(v, "%j", "%JOBID", 1)
						case "e", "error":
							if v == "" {
								log.Fatal("Missing value for -stderr")
							}
							details["stderr"] = strings.Replace(v, "%j", "%JOBID", 1)
						case "export":
							if v == "ALL" {
								jobEnv = true
							} else {
								log.Fatal("Bad value for --export. Only --export=ALL is supported by batchq!")
							}
						case "H", "hold":
							jobHold = true
						case "d", "dependency":
							if jobDeps == "" {
								if v[:8] == "afterok:" {
									jobDeps = strings.Join(strings.Split(v[8:], ":"), ",")
								} else {
									log.Fatal("Bad value for -d. Only -d afterok:... is supported by batchq!")
								}
							}
						default:
							fmt.Fprintf(os.Stderr, "Unsupported SBATCH directive: %s\n", line)
						}
					}
				}
			}
		}

		// process command line values
		// set values
		if jobProcs > 0 {
			details["procs"] = strconv.Itoa(jobProcs)
		}
		if jobMemStr != "" {
			details["mem"] = strconv.Itoa(jobs.ParseMemoryString(jobMemStr))
		}
		if jobTimeStr != "" {
			details["walltime"] = strconv.Itoa(jobs.ParseWalltimeString(jobTimeStr))
		}
		if jobWd != "" {
			details["wd"] = jobWd
		}
		if jobStdout != "" {
			details["stdout"] = jobStdout
		}
		if jobStderr != "" {
			details["stderr"] = jobStderr
		}

		// if the job name isn't set, look for a default option
		if jobName == "" {
			if tmp, ok := Config.Get("job_defaults", "name"); ok {
				jobName = tmp
			}
		}

		// replace relative paths for wd, stderr, stdout
		for _, k := range []string{"wd", "stderr", "stdout"} {
			if val, ok := details[k]; ok {
				if val1, err := support.ExpandPathAbs(val); err == nil {
					details[k] = val1
				}
			}
		}

		// if stderr/stdout are directories, then point to dir/batchq-%JOBID.stderr/out
		if val, ok := details["stdout"]; ok {
			if isdir, err := isDirectory(val); err == nil && isdir {
				details["stdout"] = path.Join(val, "batchq-%JOBID.stdout")
			}
		}
		if val, ok := details["stderr"]; ok {
			if isdir, err := isDirectory(val); err == nil && isdir {
				details["stderr"] = path.Join(val, "batchq-%JOBID.stderr")
			}
		}

		// replace env for the actual env...
		if jobEnv {
			env := ""
			for _, e := range os.Environ() {
				if env == "" {
					env = e
				} else {
					env = fmt.Sprintf("%s\n-|-\n%s", env, e)
				}
			}
			details["env"] = env
		}
		if verbose {
			for k, v := range details {
				fmt.Printf("%s: %s\n", k, v)
			}
			fmt.Print("[script]\n")
			fmt.Print(scriptSrc)
		}

		job := jobs.NewJobDef(jobName, scriptSrc)
		if jobHold {
			job.Status = jobs.USERHOLD
		}

		for k, v := range details {
			job.AddDetail(k, v)
		}

		var jobq db.BatchDB
		var err error
		if jobq, err = db.OpenDB(dbpath); err != nil {
			log.Fatalln(err)
		}
		defer jobq.Close()

		baddep := false
		for _, val := range strings.Split(jobDeps, ",") {
			if val != "" {
				if depid, err := strconv.Atoi(val); err != nil {
					log.Fatal(err)
				} else {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()

					dep := jobq.GetJob(ctx, depid)
					if dep == nil || dep.Status == jobs.CANCELLED || dep.Status == jobs.FAILED {
						// bad dependency
						fmt.Printf("Bad job dependency: %d\n", depid)
						baddep = true
					}
					job.AddAfterOk(depid)
				}
			}
		}

		if baddep {
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		job = jobq.SubmitJob(ctx, job)
		fmt.Printf("%d\n", job.JobId)

	},
}

func isDirectory(path string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		if path[len(path)-1] == os.PathSeparator {
			// if we end with a "/", assume we want a directory
			return true, nil
		}
		return false, err // path doesn't exist or other error
	}
	return info.IsDir(), nil
}

var jobName string
var jobDeps string
var jobProcs int
var jobMemStr string
var jobTimeStr string
var jobWd string
var jobStdout string
var jobStderr string
var jobEnv bool
var jobHold bool

var verbose bool
var slurmMode bool

func init() {

	submitCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Verbose output")
	submitCmd.Flags().BoolVar(&slurmMode, "slurm", false, "Script has SLURM-compatible configuration values (#SBATCH)")

	submitCmd.Flags().StringVar(&jobWd, "wd", "", "Working directory")
	submitCmd.Flags().StringVar(&jobStdout, "stdout", "", "Stdout output file")
	submitCmd.Flags().StringVar(&jobStderr, "stderr", "", "Stderr output file")
	submitCmd.Flags().StringVar(&jobName, "name", "", "Job name")
	submitCmd.Flags().StringVar(&jobDeps, "deps", "", "Dependencies (comma delimited job ids)")
	submitCmd.Flags().BoolVar(&jobEnv, "env", false, "Capture current environment")
	submitCmd.Flags().BoolVar(&jobHold, "hold", false, "Hold job")
	submitCmd.Flags().IntVarP(&jobProcs, "procs", "p", -1, "Processors required")
	submitCmd.Flags().StringVarP(&jobMemStr, "mem", "m", "", "Max-memory (MB,GB)")
	submitCmd.Flags().StringVarP(&jobTimeStr, "walltime", "t", "", "Max-time (D-HH:MM:SS)")

	rootCmd.AddCommand(submitCmd)
}
