package pathguard_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/pathguard"
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

func TestReadFileWithinRoot_HappyPath(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	c.Assert(os.Mkdir(filepath.Join(root, "sub"), 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(root, "sub", "rows.yaml"), []byte("inside"), 0o600), qt.IsNil)

	tests := []struct {
		name string
		path string
	}{
		{name: "absolute", path: filepath.Join(root, "sub", "rows.yaml")},
		{name: "climbs and comes back", path: filepath.Join(root, "sub", "..", "sub", "rows.yaml")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			data, err := pathguard.ReadFileWithinRoot(test.path, root)
			c.Assert(err, qt.IsNil)
			c.Assert(string(data), qt.Equals, "inside")
		})
	}
}

func TestReadFileWithinRoot_FailurePath(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	setup := qt.New(t)
	setup.Assert(os.Mkdir(root, 0o750), qt.IsNil)
	setup.Assert(os.WriteFile(filepath.Join(parent, "outside.yaml"), []byte("outside"), 0o600), qt.IsNil)

	t.Run("climbs out", func(t *testing.T) {
		c := qt.New(t)
		data, err := pathguard.ReadFileWithinRoot(filepath.Join(root, "..", "outside.yaml"), root)
		c.Assert(err, qt.ErrorIs, pathguard.ErrOutsideRoot)
		c.Assert(data, qt.IsNil)
	})
	t.Run("absolute path outside", func(t *testing.T) {
		c := qt.New(t)
		data, err := pathguard.ReadFileWithinRoot(filepath.Join(parent, "outside.yaml"), root)
		c.Assert(err, qt.ErrorIs, pathguard.ErrOutsideRoot)
		c.Assert(data, qt.IsNil)
	})
	t.Run("no root is not a read with no boundary", func(t *testing.T) {
		c := qt.New(t)
		data, err := pathguard.ReadFileWithinRoot(filepath.Join(parent, "outside.yaml"), "  ")
		c.Assert(err, qt.ErrorMatches, `allowed root is required`)
		c.Assert(data, qt.IsNil)
	})
	t.Run("missing file", func(t *testing.T) {
		c := qt.New(t)
		data, err := pathguard.ReadFileWithinRoot(filepath.Join(root, "absent.yaml"), root)
		c.Assert(err, qt.ErrorIs, os.ErrNotExist)
		c.Assert(data, qt.IsNil)
	})
}

// TestStartsAtRoot holds the answer to every platform at once. The table runs on
// each of them and the rows do not vary, which is the rule: a project file names
// the same thing wherever it is read.
func TestStartsAtRoot(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{path: "/etc/passwd", want: true},
		{path: "//server/share/rows.yaml", want: true},
		{path: `\etc\passwd`, want: true},
		{path: `\\server\share\rows.yaml`, want: true},
		{path: `C:\secret\rows.yaml`, want: true},
		{path: `c:/secret/rows.yaml`, want: true},
		{path: `Z:rows.yaml`, want: true},
		{path: "rows.yaml", want: false},
		{path: "sub/rows.yaml", want: false},
		{path: "../rows.yaml", want: false},
		{path: `..\rows.yaml`, want: false},
		{path: "1:rows.yaml", want: false},
		{path: "a", want: false},
		{path: "", want: false},
	}
	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			qt.New(t).Assert(pathguard.StartsAtRoot(test.path), qt.Equals, test.want)
		})
	}
}
