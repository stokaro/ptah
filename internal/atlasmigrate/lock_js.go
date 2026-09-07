//go:build js

package atlasmigrate

import "os"

// See internal/devlock/lock_js.go: one wasm instance is the only holder its
// filesystem can have, so the migration directory needs no exclusion.
func tryLockFile(*os.File) (bool, error) { return true, nil }

func unlockFile(*os.File) error { return nil }
