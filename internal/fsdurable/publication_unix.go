//go:build !windows

package fsdurable

import (
	"errors"
	"os"
	"syscall"
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

// classifyConditionalRename separates "the caller lost the race" from "this
// filesystem cannot do conditional renames at all". The second case must fail
// closed: silently retrying with rename(2) would restore the unconditional
// replacement this package exists to prevent, and the developer filesystems
// that do support the flags would never show it.
func classifyConditionalRename(op, oldName, newName string, err error) error {
	if err == nil {
		return nil
	}
	if isUnsupportedRenameError(err) {
		return unsupportedPublicationError(newName, &os.LinkError{
			Op:  op,
			Old: oldName,
			New: newName,
			Err: err,
		})
	}
	return &os.LinkError{Op: op, Old: oldName, New: newName, Err: err}
}

func isUnsupportedRenameError(err error) bool {
	return errors.Is(err, syscall.EINVAL) ||
		errors.Is(err, syscall.ENOSYS) ||
		errors.Is(err, syscall.EOPNOTSUPP) ||
		errors.Is(err, errConditionalRenameUnavailable)
}
