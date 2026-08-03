package fsdurable

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// recoveryNameLayout renders a filename-safe UTC instant. Windows forbids
// colons in filenames, so RFC 3339 cannot be used here.
const recoveryNameLayout = "20060102T150405.000000000Z"

const recoveryNameAttempts = 1_000

// preserveDisplacedFile gives a destination that a conditional publication
// displaced but could not put back a stable, greppable name next to the
// target, and syncs the parent directory before the caller reports anything.
//
// The ordering matters: the recovery entry must be durable before the error
// reaches a human, otherwise a crash during reporting turns a recoverable
// displacement into silent data loss. The name is deliberately not a dotfile,
// because the only recovery artifact this package produced before was a hidden
// random name that nobody could find.
func preserveDisplacedFile(root *os.Root, displacedName, targetName string) (string, error) {
	stamp := time.Now().UTC().Format(recoveryNameLayout)
	for attempt := range recoveryNameAttempts {
		name := targetName + ".ptah-recovery-" + stamp
		if attempt > 0 {
			name += "-" + strconv.Itoa(attempt)
		}
		err := rootRenameNoReplace(root, displacedName, name)
		if errors.Is(err, fs.ErrExist) {
			continue
		}
		if err != nil {
			return recoveryPath(root, displacedName), err
		}
		return recoveryPath(root, name), SyncRoot(root)
	}
	return recoveryPath(root, displacedName), &fs.PathError{
		Op:   "preserve",
		Path: targetName,
		Err:  fs.ErrExist,
	}
}

func recoveryPath(root *os.Root, name string) string {
	path := filepath.Join(root.Name(), name)
	absolute, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	return absolute
}

// removeDisplacedFile discards the destination a conditional publication
// displaced on purpose. Failure leaves a stale entry behind but never loses the
// published content, so the caller reports it as a post-commit defect.
func removeDisplacedFile(root *os.Root, displacedName string) error {
	err := root.Remove(displacedName)
	if err == nil || errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return fmt.Errorf("remove displaced publication destination %s: %w", displacedName, err)
}
