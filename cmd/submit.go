package cmd

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/mbreese/batchq/api"
	"github.com/mbreese/batchq/client"
	"github.com/mbreese/batchq/jobs"
	"github.com/mbreese/batchq/support"
	"github.com/spf13/cobra"
)

var submitCmd = &cobra.Command{
	Use:   "submit {args} filename/cmd",
	Short: "Submit a new job",

	Run: func(cmd *cobra.Command, args []string) {
		var scriptSrc string

		if len(args) > 0 && args[0] != "-" {
			if cmd.Flags().ArgsLenAtDash() == -1 {
				if f, err := os.Open(args[0]); err == nil {
					defer f.Close()
					data, err := io.ReadAll(f)
					if err != nil {
						fmt.Fprintf(os.Stderr, "Error: %v\n", err)
						os.Exit(2)
					}
					scriptSrc = string(data)
				} else {
					scriptSrc = fmt.Sprintf("#!/bin/sh\n%s\n", strings.Join(args, " "))
				}
			} else {
				scriptSrc = fmt.Sprintf("#!/bin/sh\n%s\n", strings.Join(args[cmd.Flags().ArgsLenAtDash():], " "))
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
					fmt.Fprintf(os.Stderr, "Error: %v\n", err)
					os.Exit(3)
				}
				scriptSrc = string(data)
			}
		}

		if scriptSrc == "" {
			cmd.Help()
			return
		}

		details := make(map[string]string)

		// Submit-time defaults — Config.JobDefaults has built-in
		// fallbacks already applied (stdout/stderr/wd); the optional
		// numeric/walltime/mem knobs remain zero-valued when not set.
		details["stdout"] = Config.JobDefaults.Stdout
		details["stderr"] = Config.JobDefaults.Stderr
		details["wd"] = Config.JobDefaults.Wd
		if Config.JobDefaults.Procs > 0 {
			details["procs"] = strconv.Itoa(Config.JobDefaults.Procs)
		}
		if v := Config.JobDefaults.Mem; v != "" {
			details["mem"] = strconv.Itoa(jobs.ParseMemoryString(v))
		}
		if v := Config.JobDefaults.Walltime; v != "" {
			details["walltime"] = strconv.Itoa(jobs.ParseWalltimeString(v))
		}
		if Config.JobDefaults.Hold {
			jobHold = true
		}
		if Config.JobDefaults.Env {
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
		// #BATCHQ -run-id workflow-run-id
		// #BATCHQ -input  path   (repeatable; accumulates)
		// #BATCHQ -output path   (repeatable; accumulates)

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
						case "run-id":
							if v == "" {
								log.Fatal("Missing value for -run-id")
							}
							if jobRunID == "" {
								jobRunID = v
							}
						case "input":
							if v == "" {
								log.Fatal("Missing value for -input")
							}
							jobInputs = append(jobInputs, v)
						case "output":
							if v == "" {
								log.Fatal("Missing value for -output")
							}
							jobOutputs = append(jobOutputs, v)
						case "resource":
							if v == "" {
								log.Fatal("Missing value for -resource")
							}
							jobResources = append(jobResources, v)
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
						case "gres":
							// --gres=name[:type][:count][,...] -> resource.name[:type]=count
							if v == "" {
								log.Fatal("Missing value for --gres")
							}
							jobResources = append(jobResources, slurmGresToResources(v)...)
						case "C", "constraint":
							// --constraint=feat[&feat...] -> resource.feat (label flags)
							if v == "" {
								log.Fatal("Missing value for --constraint")
							}
							jobResources = append(jobResources, slurmConstraintFeatures(v)...)
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

		// generic required resources (--resource name=value / #BATCHQ -resource ...)
		for _, entry := range jobResources {
			name, val, _ := strings.Cut(entry, "=")
			name = strings.TrimSpace(name)
			if name == "" || strings.ContainsAny(name, " \t") {
				log.Fatalf("Bad --resource name: %q", entry)
			}
			switch name {
			case "procs", "mem", "walltime":
				log.Fatalf("--resource %q is reserved; use -p/-m/-t instead", name)
			}
			details[jobs.ResourcePrefix+name] = strings.TrimSpace(val)
		}

		// if the job name isn't set, look for a default option
		if jobName == "" {
			jobName = Config.JobDefaults.Name
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
				fmt.Fprintf(os.Stderr, "%s: %s\n", k, v)
			}
			fmt.Fprint(os.Stderr, "[script]\n")
			fmt.Fprint(os.Stderr, scriptSrc)
		}

		// The script source is carried in details["script"] over the wire.
		details["script"] = scriptSrc

		var afterOk []string
		for _, val := range strings.Split(jobDeps, ",") {
			depid := strings.TrimSpace(val)
			if depid == "" {
				continue
			}
			afterOk = append(afterOk, depid)
		}

		c := mustDialClient()
		defer c.Close()

		baddep := false
		for _, depid := range afterOk {
			ctx, cancel := cmdContext()
			dep, err := c.GetJob(ctx, depid)
			cancel()
			if err != nil {
				if errors.Is(err, client.ErrNotFound) {
					fmt.Fprintf(os.Stderr, "ERROR: Bad job dependency: %s\n", depid)
					baddep = true
					continue
				}
				log.Fatalln(err)
			}
			if dep == nil || dep.Status == jobs.CANCELED.String() || dep.Status == jobs.FAILED.String() {
				fmt.Fprintf(os.Stderr, "ERROR: Bad job dependency: %s\n", depid)
				baddep = true
			}
		}

		if baddep {
			os.Exit(1)
		}

		ctx, cancel := cmdContext()
		defer cancel()
		if jobRunID != "" {
			details["run_id"] = jobRunID
		}
		req := &api.SubmitJobRequest{
			Name:        jobName,
			Hold:        jobHold,
			AfterOk:     afterOk,
			Details:     details,
			InputFiles:  jobInputs,
			OutputFiles: jobOutputs,
		}
		dto, err := c.SubmitJob(ctx, req)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("%s\n", dto.JobID)

	},
}

// slurmGresToResources translates a SLURM --gres value into batchq --resource
// entries. Each comma-separated gres is "name[:type][:count]" (count defaults
// to 1); the name[:type] becomes the resource name and the count its value, so
// "gpu:a100:2,mps:50" -> ["gpu:a100=2", "mps=50"].
func slurmGresToResources(v string) []string {
	var out []string
	for _, g := range strings.Split(v, ",") {
		g = strings.TrimSpace(g)
		if g == "" {
			continue
		}
		parts := strings.Split(g, ":")
		count := "1"
		if n := len(parts); n >= 2 {
			if _, err := strconv.Atoi(parts[n-1]); err == nil {
				count = parts[n-1]
				parts = parts[:n-1]
			}
		}
		name := strings.Join(parts, ":")
		if name == "" {
			continue
		}
		out = append(out, name+"="+count)
	}
	return out
}

// slurmConstraintFeatures translates a SLURM --constraint value into bare
// --resource feature flags. Only the simple AND forms ("a&b", "a,b", "[a&b]")
// are handled; OR groups ("|"), counts ("*N"), and parentheses are skipped.
func slurmConstraintFeatures(v string) []string {
	v = strings.Trim(strings.TrimSpace(v), "[]")
	var out []string
	for _, f := range strings.FieldsFunc(v, func(r rune) bool { return r == '&' || r == ',' }) {
		f = strings.TrimSpace(f)
		if f == "" || strings.ContainsAny(f, "|*()") {
			continue
		}
		out = append(out, f)
	}
	return out
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
var jobRunID string
var jobInputs []string
var jobOutputs []string
var jobResources []string

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
	submitCmd.Flags().StringVar(&jobRunID, "run-id", "", "Workflow run ID (groups related jobs)")
	submitCmd.Flags().StringArrayVar(&jobInputs, "input", nil, "Input file path (repeatable)")
	submitCmd.Flags().StringArrayVar(&jobOutputs, "output", nil, "Output file path (repeatable)")
	submitCmd.Flags().StringArrayVar(&jobResources, "resource", nil, "Required resource (name=value or name, repeatable)")

	rootCmd.AddCommand(submitCmd)
}
