//go:build windows

package migraterollout

import (
	"os"
	"os/exec"
)

// configureProcess leaves a target run in the rollout's console, so a Ctrl-C
// there reaches it directly, once.
func configureProcess(*exec.Cmd) {}

// interruptProcess sends nothing. Windows has no interrupt one process can
// send another's console group without also receiving it, and ending the run
// by force would stop it between two statements. A run that is not stopped by
// the console's Ctrl-C finishes, and its document is recorded.
func interruptProcess(*os.Process) {}
