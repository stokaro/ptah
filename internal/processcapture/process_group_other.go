//go:build !aix && !darwin && !dragonfly && !freebsd && !illumos && !linux && !netbsd && !openbsd && !solaris && !windows

package processcapture

import (
	"errors"
	"os"
	"os/exec"
)

func prepareProcess(_ *exec.Cmd) {}

type directProcessTree struct {
	process *os.Process
}

func attachProcessTree(cmd *exec.Cmd) (processTree, error) {
	return directProcessTree{process: cmd.Process}, nil
}

func (tree directProcessTree) terminate() error {
	err := tree.process.Kill()
	if errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	return err
}

func (directProcessTree) close() error { return nil }
