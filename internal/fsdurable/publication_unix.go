//go:build !windows && !js

package fsdurable

import (
	"errors"
	"os"
)

// withRootDirFD runs fn with a descriptor for the directory root guards.
// os.Root exposes no descriptor, so the directory is reopened through the root
// itself and the descriptor is borrowed under SyscallConn.Control, which keeps
// the runtime from closing it while the syscall is in flight.
func withRootDirFD(root *os.Root, fn func(dirfd int) error) error {
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	conn, err := dir.SyscallConn()
	if err != nil {
		return errors.Join(err, dir.Close())
	}
	var opErr error
	controlErr := conn.Control(func(fd uintptr) {
		opErr = fn(int(fd))
	})
	closeErr := dir.Close()
	if opErr != nil {
		return errors.Join(opErr, controlErr, closeErr)
	}
	return errors.Join(controlErr, closeErr)
}

func rootRenameNoReplace(root *os.Root, oldName, newName string) error {
	return classifyConditionalRename(
		"renameat-noreplace",
		oldName,
		newName,
		withRootDirFD(root, func(dirfd int) error {
			return renameNoReplaceAt(dirfd, oldName, newName)
		}),
	)
}

func rootRenameExchange(root *os.Root, oldName, newName string) error {
	return classifyConditionalRename(
		"renameat-exchange",
		oldName,
		newName,
		withRootDirFD(root, func(dirfd int) error {
			return renameExchangeAt(dirfd, oldName, newName)
		}),
	)
}
