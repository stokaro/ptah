//go:build unix

package cabundle_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cabundle"
)

// TestLoad_FailurePath_Unreadable refuses a bundle the process may not read.
// Windows does not deny a read by mode bits, so the case exists on Unix only.
func TestLoad_FailurePath_Unreadable(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "ca.pem")
	c.Assert(os.WriteFile(path, []byte("unused"), 0o000), qt.IsNil)

	pool, err := cabundle.Load(path)

	c.Assert(err, qt.ErrorIs, fs.ErrPermission)
	c.Assert(err, qt.ErrorMatches, `read CA bundle: open .*ca\.pem: permission denied`)
	c.Assert(pool, qt.IsNil)
}
