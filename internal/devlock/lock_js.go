//go:build js

package devlock

import "os"

// A js/wasm build is one instance holding a filesystem no other process can
// see, so the realm has exactly one candidate holder and the lock is always
// available. This is not "locking is unsupported here": there is nothing to
// exclude.
func tryLockFile(*os.File) (bool, error) { return true, nil }

func unlockFile(*os.File) error { return nil }
