//go:build !windows

package providerinstall

import "os"

// replaceRecord moves the record written at temp over the one at path. These
// systems replace a file another install holds open.
func replaceRecord(temp, path string) error {
	return os.Rename(temp, path)
}
