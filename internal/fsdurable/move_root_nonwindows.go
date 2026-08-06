//go:build !windows

package fsdurable

import (
	"errors"
	"os"
)

// MoveFileNoReplaceAt publishes oldName at newName inside root without
// replacing an existing entry. Both names are resolved through the retained
// root, so replacing the directory's pathname cannot redirect the move.
// Callers must sync root afterwards.
//
// It uses the same link-then-unlink sequence as the pathname-based
// MoveFileNoReplace: link fails with fs.ErrExist when the destination is taken,
// which is exactly the no-replace guarantee, and it does not depend on a
// conditional rename primitive the filesystem may not implement.
func MoveFileNoReplaceAt(root *os.Root, oldName, newName string) error {
	if err := root.Link(oldName, newName); err != nil {
		return err
	}
	if err := root.Remove(oldName); err != nil {
		return errors.Join(err, root.Remove(newName))
	}
	return nil
}
