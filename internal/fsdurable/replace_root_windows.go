package fsdurable

import (
	"errors"
	"fmt"
	"os"
)

// ReplaceFileAt replaces newName with oldName through root and flushes the
// published file before returning. Errors after the rename wrap
// ErrReplacementCommitted so callers can avoid treating them as pre-commit
// failures.
func ReplaceFileAt(root *os.Root, oldName, newName string) error {
	staged, err := root.OpenFile(oldName, os.O_RDWR, 0)
	if err != nil {
		return err
	}

	stagedInfo, err := staged.Stat()
	if err != nil {
		return errors.Join(
			fmt.Errorf("stat staged file before rooted replacement %s: %w", oldName, err),
			staged.Close(),
		)
	}
	if err := root.Rename(oldName, newName); err != nil {
		return errors.Join(err, staged.Close())
	}

	publishedInfo, verifyErr := root.Lstat(newName)
	if verifyErr != nil {
		verifyErr = fmt.Errorf("stat published file after rooted replacement %s: %w", newName, verifyErr)
	}
	if verifyErr == nil && !publishedInfo.Mode().IsRegular() {
		verifyErr = fmt.Errorf("published file is not regular after rooted replacement: %s", newName)
	}
	if verifyErr == nil && !os.SameFile(stagedInfo, publishedInfo) {
		verifyErr = fmt.Errorf("published file changed during rooted replacement: %s", newName)
	}
	return replacementCommittedError(errors.Join(verifyErr, staged.Sync(), staged.Close()))
}
