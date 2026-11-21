//go:build unix

package advisorysource

import (
	"fmt"
	"io/fs"
	"os"
	"syscall"
)

// checkPrivate refuses a cache directory or entry that another user owns or
// that its group or others can write: whoever can write the cache chooses a
// copy the scan may read.
func checkPrivate(path string, info fs.FileInfo) error {
	if info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("%s can be written by its group or others", path)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int64(stat.Uid) != int64(os.Geteuid()) {
		return fmt.Errorf("%s is owned by another user", path)
	}
	return nil
}
