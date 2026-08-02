package fsdurable_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/fsdurable"
)

func TestSyncDir_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "entry"), []byte("value"), 0o600), qt.IsNil)

	c.Assert(fsdurable.SyncDir(dir), qt.IsNil)
}
