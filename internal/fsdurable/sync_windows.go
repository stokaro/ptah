package fsdurable

// SyncDir is a no-op on Windows because FlushFileBuffers does not support
// directory handles. Callers sync file contents before publication and use
// write-through replacement for commit-marker renames.
func SyncDir(string) error {
	return nil
}
