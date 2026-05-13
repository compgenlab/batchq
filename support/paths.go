package support

import (
	"os"
	"path/filepath"
)

// GetBatchqHome returns the resolved $BATCHQ_HOME (defaulting to ~/.batchq).
// Falls back to "." only if both the env var and the user's home directory
// are unavailable — callers that need the result to exist on disk are
// responsible for creating it.
func GetBatchqHome() string {
	home := os.Getenv("BATCHQ_HOME")
	if home == "" {
		home = "~/.batchq"
	}
	if ret, err := ExpandPathAbs(home); err == nil {
		return ret
	}
	if userHome, err := os.UserHomeDir(); err == nil {
		return filepath.Join(userHome, ".batchq")
	}
	return "."
}
