package runner

type Runner interface {
	// starts the runner
	// how the runner operates is completely up to the implementation.
	// it could run one job and quit, run all jobs it can, or wait for
	// more jobs to appear.

	Start() bool
}
