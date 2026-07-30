package fsdurable

import (
	"errors"
	"fmt"
	"os"
)

// ReplaceFileAt replaces newName with oldName through root and flushes the
// published file before returning.
func ReplaceFileAt(root *os.Root, oldName, newName string) (err error) {
	staged, err := root.OpenFile(oldName, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, staged.Close())
	}()

	stagedInfo, err := staged.Stat()
	if err != nil {
		return fmt.Errorf("stat staged file before rooted replacement %s: %w", oldName, err)
	}
	if err := root.Rename(oldName, newName); err != nil {
		return err
	}
	published, err := root.Open(newName)
	if err != nil {
		return err
	}
	publishedInfo, statErr := published.Stat()
	if statErr != nil {
		return errors.Join(
			fmt.Errorf("stat published file after rooted replacement %s: %w", newName, statErr),
			published.Close(),
		)
	}
	if closeErr := published.Close(); closeErr != nil {
		return closeErr
	}
	if !os.SameFile(stagedInfo, publishedInfo) {
		return fmt.Errorf("published file changed during rooted replacement: %s", newName)
	}
	return staged.Sync()
}
