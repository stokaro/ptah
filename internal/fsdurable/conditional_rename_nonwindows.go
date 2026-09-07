//go:build !windows

package fsdurable

import (
	"errors"
	"os"
	"syscall"
)

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
