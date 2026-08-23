//go:build !windows

package agentaudit_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/agentaudit"
)

// The mode assertion lives in its own file because it is real on one platform
// and vacuous on the other. Written as one test, the Windows half reduces to
// comparing the 0o666 that os.Stat synthesizes from the read-only attribute
// against zero -- which is how CI reported this, and which would otherwise have
// been "fixed" by deleting the assertion that works.

func TestOpenFile_ProtectsTheLogFromOtherUsers(t *testing.T) {
	c := qt.New(t)
	path := agentaudit.DefaultPath(c.TempDir())

	writer, err := agentaudit.OpenFile(path, agentaudit.Options{
		SessionID: "session-1", Surface: agentaudit.SurfaceAssist,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(writer.Record(agentaudit.Event{
		Operation: "read_artifact", Outcome: agentaudit.OutcomePermitted,
	}), qt.IsNil)
	c.Assert(writer.Close(), qt.IsNil)

	info, err := os.Stat(path)
	c.Assert(err, qt.IsNil)
	c.Assert(info.Mode().Perm()&0o077, qt.Equals, os.FileMode(0))

	dir, err := os.Stat(filepath.Dir(path))
	c.Assert(err, qt.IsNil)
	c.Assert(dir.Mode().Perm()&0o077, qt.Equals, os.FileMode(0))
}
