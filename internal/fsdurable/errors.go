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
	// ErrDestinationChanged reports that the publication target did not hold
	// the state the caller required when the commit was attempted. The
	// publication is refused unless the error also wraps
	// ErrReplacementCommitted.
	ErrDestinationChanged = errors.New("publication destination identity or metadata changed")
	// ErrConditionalPublicationUnsupported reports that the filesystem or
	// platform cannot commit a publication conditionally on the destination
	// state. Publication fails closed instead of degrading to an
	// unconditional rename.
	ErrConditionalPublicationUnsupported = errors.New("conditional publication is unsupported here")
)

// errConditionalRenameUnavailable marks a platform or filesystem that ships no
// usable conditional rename primitive, so every platform can report the same
// fail-closed cause through ErrConditionalPublicationUnsupported.
var errConditionalRenameUnavailable = errors.New("no conditional rename primitive available")

func replacementCommittedError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", ErrReplacementCommitted, err)
}

func destinationChangedError(targetName string, cause error) error {
	if cause == nil {
		return fmt.Errorf("%w: %s", ErrDestinationChanged, targetName)
	}
	return fmt.Errorf("%w: %s: %w", ErrDestinationChanged, targetName, cause)
}

func unsupportedPublicationError(targetName string, cause error) error {
	return fmt.Errorf(
		"%w: %s: the filesystem rejected the conditional rename primitive: %w",
		ErrConditionalPublicationUnsupported,
		targetName,
		cause,
	)
}
