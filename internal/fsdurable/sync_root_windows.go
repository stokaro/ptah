package fsdurable

import "os"

// SyncRoot is a no-op on Windows because FlushFileBuffers does not support
// directory handles. ReplaceFileAt flushes the published file instead.
func SyncRoot(*os.Root) error {
	return nil
}
