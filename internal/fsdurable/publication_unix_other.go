//go:build !windows && !darwin && !linux

package fsdurable

// The released platforms are linux, darwin and windows. Other unix targets
// still compile this package, so they need a conditional rename that fails
// closed: reporting ErrConditionalPublicationUnsupported keeps an unbuilt
// platform from quietly falling back to an unconditional rename.

func renameNoReplaceAt(_ int, _, _ string) error {
	return errConditionalRenameUnavailable
}

func renameExchangeAt(_ int, _, _ string) error {
	return errConditionalRenameUnavailable
}
