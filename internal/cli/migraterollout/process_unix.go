//go:build unix

package migraterollout

import (
	"os"
	"os/exec"
	"syscall"
)

// configureProcess starts a target run in a process group of its own. A Ctrl-C
// at the terminal reaches the rollout's group only, so each run is asked to
// stop exactly once, by interruptProcess; a second interrupt would end the run
// at once rather than at a safe point.
func configureProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// interruptProcess asks a target run to stop, as Ctrl-C asks
// `ptah migrations up`: the run ends at a safe point and writes the document
// that says where.
func interruptProcess(process *os.Process) {
	_ = process.Signal(os.Interrupt)
}
