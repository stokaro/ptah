//go:build !windows

package fsdurable

import (
	"errors"
	"os"
)

// MoveFileNoReplace publishes oldPath at newPath without replacing an
// existing entry. Callers must sync the containing directory afterwards.
func MoveFileNoReplace(oldPath, newPath string) error {
	if err := os.Link(oldPath, newPath); err != nil {
		return err
	}
	if err := os.Remove(oldPath); err != nil {
		return errors.Join(err, os.Remove(newPath))
	}
	return nil
}
