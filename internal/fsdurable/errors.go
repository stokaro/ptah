package fsdurable

import "errors"

var (
	// ErrReplacementCommitted reports that a file replacement completed but a
	// subsequent durability or verification operation failed.
	ErrReplacementCommitted = errors.New("file replacement committed with uncertain durability")
)
