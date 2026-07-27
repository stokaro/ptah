//go:build aix || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris

package schemasource

import (
	"errors"
	"os/exec"
	"syscall"
)

// prepareProcess starts the loader in a new process group so cancellation can
// terminate descendants as well as the direct child.
func prepareProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

type unixProcessTree struct {
	processGroupID int
}

func attachProcessTree(cmd *exec.Cmd) (processTree, error) {
	return unixProcessTree{processGroupID: cmd.Process.Pid}, nil
}

func (tree unixProcessTree) terminate() error {
	err := syscall.Kill(-tree.processGroupID, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}

func (unixProcessTree) close() error { return nil }
