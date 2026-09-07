//go:build js

package devlock

import (
	"os"
	"path/filepath"
)

// filesystemIdentity falls back to the cleaned absolute path on js/wasm, where
// FileInfo.Sys carries no device or inode number. Two paths naming one file
// through a link would read as two realms, which is a weaker guarantee than
// the unix version gives -- and an adequate one here, because the whole
// filesystem belongs to a single wasm instance that creates no such links.
func filesystemIdentity(path string) (string, error) {
	if _, err := os.Stat(path); err != nil {
		return "", err
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return filepath.Clean(abs), nil
}
