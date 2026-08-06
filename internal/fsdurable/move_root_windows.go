package fsdurable

import (
	"errors"
	"fmt"
	"os"
)

// MoveFileNoReplaceAt publishes oldName at newName inside root without
// replacing an existing entry and flushes the published file before returning.
// Both names must identify direct children of root.
//
// Windows has no rooted MoveFileEx, so the move runs through the same
// rename-by-handle primitive the rooted publication uses, which never requests
// replacement. MOVEFILE_WRITE_THROUGH has no equivalent there either, so
// durability is supplied by flushing the moved file afterwards. Errors raised
// after the rename took effect wrap ErrReplacementCommitted.
func MoveFileNoReplaceAt(root *os.Root, oldName, newName string) error {
	if err := validateDirectChildName("moveat", oldName); err != nil {
		return err
	}
	if err := validateDirectChildName("moveat", newName); err != nil {
		return err
	}
	if err := rootRenameNoReplace(root, oldName, newName); err != nil {
		return err
	}
	published, err := openPublicationFile(root, newName)
	if err != nil {
		return replacementCommittedError(
			fmt.Errorf("open published file after rooted move %s: %w", newName, err),
		)
	}
	return replacementCommittedError(errors.Join(published.Sync(), published.Close()))
}
