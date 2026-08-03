//go:build !windows

package fsdurable

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
)

func openPublicationFile(root *os.Root, name string) (*os.File, error) {
	return root.OpenFile(name, os.O_RDWR, 0)
}

// commitPublicationFile publishes the staged entry over targetName only while
// the target still holds dest. Unix has no single syscall that both replaces an
// existing file and binds the file it replaces, so the two expectations use
// different primitives: an absent destination is a no-replace rename, and an
// expected file is an atomic exchange whose displaced side is verified
// afterwards and swapped back when it turns out to belong to someone else.
func commitPublicationFile(
	root *os.Root,
	_ *os.File,
	stagedName, targetName string,
	dest Destination,
	hooks publicationHooks,
) (bool, error) {
	if dest.kind == destinationAbsent {
		hooks.runBeforeCommit()
		if err := rootRenameNoReplace(root, stagedName, targetName); err != nil {
			if errors.Is(err, fs.ErrExist) {
				return false, destinationChangedError(targetName, err)
			}
			return false, err
		}
		return true, nil
	}
	return exchangePublicationFile(root, stagedName, targetName, dest, hooks)
}

func exchangePublicationFile(
	root *os.Root,
	stagedName, targetName string,
	dest Destination,
	hooks publicationHooks,
) (bool, error) {
	hooks.runBeforeCommit()
	if err := rootRenameExchange(root, stagedName, targetName); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, destinationChangedError(targetName, err)
		}
		return false, err
	}
	displacedInfo, statErr := root.Lstat(stagedName)
	if statErr == nil && dest.matches(displacedInfo) {
		return true, removeDisplacedFile(root, stagedName)
	}
	restoreErr := rootRenameExchange(root, stagedName, targetName)
	if restoreErr == nil {
		return false, destinationChangedError(
			targetName,
			errors.Join(errDisplacedDestination, statErr),
		)
	}
	recovery, preserveErr := preserveDisplacedFile(root, stagedName, targetName)
	return true, fmt.Errorf(
		"%w: %s: the displaced destination could not be restored and is preserved at %s: %w",
		ErrDestinationChanged,
		targetName,
		recovery,
		errors.Join(errDisplacedDestination, statErr, restoreErr, preserveErr),
	)
}

func modesEqual(actual, expected fs.FileMode) bool {
	return actual == expected
}
