//go:build !windows

package fsdurable

import (
	"io/fs"
	"os"
)

func openPublicationFile(root *os.Root, name string) (*os.File, error) {
	return root.OpenFile(name, os.O_RDWR, 0)
}

func renamePublicationFile(
	root *os.Root,
	_ *os.File,
	stagedName, targetName string,
) (bool, error) {
	if err := root.Rename(stagedName, targetName); err != nil {
		return false, err
	}
	return true, nil
}

func modesEqual(actual, expected fs.FileMode) bool {
	return actual == expected
}
