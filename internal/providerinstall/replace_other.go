//go:build !windows

package providerinstall

import "os"

// replaceRecord renames temp over the record at path. These systems replace a
// file that a reader holds open, and the reader keeps the record it opened.
func replaceRecord(temp, path string) error {
	return os.Rename(temp, path)
}
