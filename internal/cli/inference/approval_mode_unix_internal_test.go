//go:build !windows

package inference

// White-box testing required: it asserts about writePlanFile, which is
// unexported for the reason given in approval_internal_test.go.
//
// The mode assertion lives in its own file because it is real on one platform
// and vacuous on the other. Written as one test, the Windows half compares the
// 0o666 that os.Stat synthesizes from the read-only attribute against 0o600 --
// which is how CI reported it, and which would otherwise have been "fixed" by
// deleting the assertion that works.

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestWritePlanFile_IsWrittenForTheApproverRatherThanTheFilesystem pins the
// mode.
//
// The file is handed to a person to sign and carries what a generation change
// would do. It is the operator's to share rather than the filesystem's to
// offer.
func TestWritePlanFile_IsWrittenForTheApproverRatherThanTheFilesystem(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "cutover.plan")

	c.Assert(writePlanFile(&bytes.Buffer{}, path, aPlan()), qt.IsNil)

	info, err := os.Stat(path)
	c.Assert(err, qt.IsNil)
	c.Assert(info.Mode().Perm(), qt.Equals, os.FileMode(0o600))
}
