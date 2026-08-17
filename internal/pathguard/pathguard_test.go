package pathguard_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/pathguard"
)

func TestResolveWithinRootAllowsMissingChild(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	path := filepath.Join(root, "migrations")

	resolved, err := pathguard.ResolveWithinRoot(path, root)
	c.Assert(err, qt.IsNil)
	c.Assert(filepath.Base(resolved), qt.Equals, "migrations")
}

func TestResolveWithinRootRejectsTraversal(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	path := filepath.Join(root, "..", "outside")

	_, err := pathguard.ResolveWithinRoot(path, root)
	c.Assert(err, qt.ErrorMatches, `.*outside allowed root.*`)
	c.Assert(err, qt.ErrorIs, pathguard.ErrOutsideRoot)
}

func TestResolveWithinRootRejectsSymlinkEscape(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	outside := t.TempDir()
	link := filepath.Join(root, "link")
	c.Assert(os.Symlink(outside, link), qt.IsNil)

	_, err := pathguard.ResolveWithinRoot(filepath.Join(link, "migrations"), root)
	c.Assert(err, qt.ErrorMatches, `.*outside allowed root.*`)
	c.Assert(err, qt.ErrorIs, pathguard.ErrOutsideRoot)
}

// TestResolveCLIPathAnswersBothSpellingsOfOneDestination is the replacement for
// a test that asserted "../outside" was refused.
//
// That refusal was a spelling filter, not a boundary: ResolveCLIPath exempted
// absolute paths, so the identical destination spelled in full was accepted by
// the same call. stokaro/ptah#1622 removed it, and what is pinned now is that
// the two spellings agree -- which is the property the old rule broke and the
// one the community Atlas binary has.
func TestResolveCLIPathAnswersBothSpellingsOfOneDestination(t *testing.T) {
	c := qt.New(t)
	parent := t.TempDir()
	outside := filepath.Join(parent, "outside")
	c.Assert(os.Mkdir(outside, 0o750), qt.IsNil)
	work := filepath.Join(parent, "work")
	c.Assert(os.Mkdir(work, 0o750), qt.IsNil)
	t.Chdir(work)

	relative, relativeErr := pathguard.ResolveCLIPath("../outside")
	absolute, absoluteErr := pathguard.ResolveCLIPath(outside)

	c.Assert(relativeErr, qt.IsNil)
	c.Assert(absoluteErr, qt.IsNil)
	c.Assert(relative, qt.Equals, absolute)
}

// TestResolveWithinRootStillRefusesATraversal is the control for the test
// above: dropping the CLI helper's relative-only rule must not touch the entry
// point that takes an explicit root, which binds every spelling.
func TestResolveWithinRootStillRefusesATraversal(t *testing.T) {
	c := qt.New(t)
	parent := t.TempDir()
	outside := filepath.Join(parent, "outside")
	c.Assert(os.Mkdir(outside, 0o750), qt.IsNil)
	root := filepath.Join(parent, "work")
	c.Assert(os.Mkdir(root, 0o750), qt.IsNil)

	_, relativeErr := pathguard.ResolveWithinRoot("../outside", root)
	_, absoluteErr := pathguard.ResolveWithinRoot(outside, root)

	c.Assert(relativeErr, qt.ErrorIs, pathguard.ErrOutsideRoot)
	c.Assert(absoluteErr, qt.ErrorIs, pathguard.ErrOutsideRoot)
}

func TestOpenCLIDirectoryPreservesExplicitAbsolutePath(t *testing.T) {
	c := qt.New(t)
	outside := t.TempDir()

	opened, err := pathguard.OpenCLIDirectory(outside)

	c.Assert(err, qt.IsNil)
	absolute, err := filepath.Abs(outside)
	c.Assert(err, qt.IsNil)
	c.Assert(opened.Path(), qt.Equals, absolute)
	c.Assert(opened.Close(), qt.IsNil)
}
