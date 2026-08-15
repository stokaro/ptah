//go:build aix || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris

package processcapture

import (
	"errors"
	"os/exec"
	"syscall"
)

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
