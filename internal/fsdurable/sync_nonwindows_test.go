//go:build !windows

package fsdurable_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/fsdurable"
)

func TestSyncDir_FailurePath(t *testing.T) {
	c := qt.New(t)

	err := fsdurable.SyncDir(filepath.Join(c.TempDir(), "missing"))

	c.Assert(err, qt.ErrorMatches, `open directory for sync: .*`)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}
