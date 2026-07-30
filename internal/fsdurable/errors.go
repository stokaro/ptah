package fsdurable

import "errors"

var (
	// ErrReplacementCommitted reports that the rooted rename succeeded but a
	// subsequent identity verification or durability operation failed.
	ErrReplacementCommitted = errors.New("file replacement committed with uncertain durability")
)
