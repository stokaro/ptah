//go:build !unix && !windows

package migraterollout

import (
	"os"
	"os/exec"
)

// configureProcess has nothing to set on a platform without process groups.
func configureProcess(*exec.Cmd) {}

// interruptProcess sends nothing: the run finishes and its document is
// recorded.
func interruptProcess(*os.Process) {}
