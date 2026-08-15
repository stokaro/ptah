//go:build unix

package atlasprojectpath_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasprojectpath"
	"go.5x5.cz/ptah/internal/pathguard"
)

// This test needs a directory whose name contains "?" -- that is the whole
// subject, since the question is whether a query-like suffix in a plain path
// is preserved rather than parsed. Win32 reserves "?" in file names along with
// < > : " / \ | *, so such a directory cannot be created there and the
// behavior is unobservable rather than different.
//
// TestLocalDir_PreservesPlainPathURLCharacters covers the same rule with
// "%2F", which every platform allows, and it runs everywhere.

func TestLocalDirWithQuery_PreservesPlainPathQueryLikeSuffix(t *testing.T) {
	c := qt.New(t)
	baseDir := t.TempDir()
	want, err := pathguard.ResolveWithinRoot(filepath.Join(baseDir, "migrations?format=atlas"), "")
	c.Assert(err, qt.IsNil)

	resolved, query, err := atlasprojectpath.LocalDirWithQuery("migrations?format=atlas", baseDir)

	c.Assert(err, qt.IsNil)
	c.Assert(resolved, qt.Equals, want)
	c.Assert(query, qt.HasLen, 0)
}
