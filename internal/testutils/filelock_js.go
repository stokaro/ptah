//go:build js

package testutils

import (
	"errors"
	"os"
)

// errNoCrossProcessLock is what a caller gets instead of a lock that would
// prove nothing.
//
// AcquireExclusiveFileLock exists to test cross-process lock behavior, and a
// js/wasm instance has no second process to contend with. A stub that reported
// success would leave every such test passing while exercising nothing, which
// is worse than not building at all -- so this refuses in as many words.
var errNoCrossProcessLock = errors.New(
	"cross-process file locking cannot be exercised on js/wasm: one instance has no second process")

func lockFile(*os.File) error { return errNoCrossProcessLock }

func unlockFile(*os.File) error { return errNoCrossProcessLock }
