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
	published, err := root.Open(newName)
	if err != nil {
		return replacementCommittedError(errors.Join(err, staged.Close()))
	}
	publishedInfo, statErr := published.Stat()
	if statErr != nil {
		return replacementCommittedError(errors.Join(
			fmt.Errorf("stat published file after rooted replacement %s: %w", newName, statErr),
			published.Close(),
			staged.Close(),
		))
	}
	if closeErr := published.Close(); closeErr != nil {
		return replacementCommittedError(errors.Join(closeErr, staged.Close()))
	}
	if !os.SameFile(stagedInfo, publishedInfo) {
		return replacementCommittedError(errors.Join(
			fmt.Errorf("published file changed during rooted replacement: %s", newName),
			staged.Close(),
		))
	}
	return replacementCommittedError(errors.Join(staged.Sync(), staged.Close()))
}

func replacementCommittedError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrReplacementCommitted, err)
}
