package fsdurable

import (
	"errors"
	"fmt"
)

var (
	// ErrReplacementCommitted reports that the rooted rename succeeded but a
	// subsequent identity verification or durability operation failed.
	ErrReplacementCommitted = errors.New("file replacement committed with uncertain durability")
	// ErrStagedFileChanged reports that a rooted staged entry no longer
	// identifies the file captured by its caller.
	ErrStagedFileChanged = errors.New("staged file identity or metadata changed")
)

func replacementCommittedError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrReplacementCommitted, err)
}
