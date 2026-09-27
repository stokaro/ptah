//go:build js

package clirun

import "os"

// tryLock always succeeds. A js/wasm build is one instance holding a filesystem
// no other process can see, so there is nobody to exclude.
func tryLock(*os.File) (bool, error) { return true, nil }
