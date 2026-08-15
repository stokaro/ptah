//go:build unix

package atlasmigrate

// White-box testing required: the moment this measures -- after the migration
// directory is bound and before anything is written -- has no exported name,
// because nothing in the product needs one. A pre-swap test through the
// exported API would only exercise the open, and the open is not what the
// rooted handle uniquely defends.

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasmigrateimport"
	"go.5x5.cz/ptah/internal/pathguard"
)

// These are the `migrate new` half of the rooted-writer regressions
// stokaro/ptah#1118 asks for, and the counterpart to the three
// TestGenerateDiff_* rows next door.
//
// The swap happens AFTER the migration directory is bound and before anything
// is written, through the package's own seam. A pre-swap test would only
// exercise the open; what the handle uniquely defends is a replacement that
// lands in the window a pathname-based writer would resolve again.
//
// This file is white-box for that reason alone: the seam is unexported because
// nothing in the product needs it, and the moment it marks has no other name.
// Everything asserted below is otherwise observable — the files on disk and the
// contents of the decoy directory.

// noSkeletonProjectRoot is the direct-CLI shape: no project root is opened, so
// the run is confined by nothing but the --dir the operator named.
func noSkeletonProjectRoot(string) (*pathguard.OpenedDirectory, error) { return nil, nil }

// makeSkeletonDirs creates each path a row stages, relative to base.
func makeSkeletonDirs(c *qt.C, base string, dirs []string) {
	c.Helper()
	for _, dir := range dirs {
		c.Assert(os.MkdirAll(filepath.Join(base, dir), 0o755), qt.IsNil)
	}
}

// closeOpenedRoot tolerates the nil handle noSkeletonProjectRoot returns.
func closeOpenedRoot(opened *pathguard.OpenedDirectory) error {
	if opened == nil {
		return nil
	}
	return opened.Close()
}

// swapSkeletonSymlink replaces link so it points at target.
func swapSkeletonSymlink(c *qt.C, link, target string) {
	c.Helper()
	c.Assert(os.Remove(link), qt.IsNil)
	c.Assert(os.Symlink(target, link), qt.IsNil)
}

// skeletonDirNames lists the base names in dir, or nil when dir is absent.
func skeletonDirNames(c *qt.C, dir string) []string {
	c.Helper()
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

// TestWriteSkeletonMigration_ReplacedDirectoryCannotRedirectTheWrite asserts
// both halves: the migration lands in the directory the run bound, and the
// directory the symlink was swapped to is left completely untouched.
//
// The second half is the one that fails when the rooted handle is removed.
// Without it a run that merely errored out would look the same as one that
// stayed inside the boundary.
func TestWriteSkeletonMigration_ReplacedDirectoryCannotRedirectTheWrite(t *testing.T) {
	tests := []struct {
		name string
		// openRoot opens the project root the run is confined to, or answers a
		// nil handle for the direct-CLI shape where an explicit --dir is the
		// operator's own choice of destination.
		openRoot func(root string) (*pathguard.OpenedDirectory, error)
		// dirs and decoyDirs are the directories the row stages before the run,
		// relative to the temporary root and to the decoy. Only the ancestor row
		// stages anything in the decoy, and it does so because the swapped
		// symlink has to resolve to a migrations directory at all.
		dirs      []string
		decoyDirs []string
		// link is a symlink staged under the root, pointing at linkTarget; both
		// are relative to the root, as are selected -- the --dir the run is
		// given -- and bound, where the migration has to land.
		link       string
		linkTarget string
		selected   string
		bound      string
		swapTo     func(root, decoy string) (link, target string)
		// wantDecoyEntries is what the row staged in the decoy before the run,
		// and therefore all the decoy may hold after it.
		wantDecoyEntries int
	}{
		{
			name:       "the migration directory symlink is swapped, under a project root",
			openRoot:   pathguard.OpenCLIDirectory,
			dirs:       []string{"real"},
			link:       "migrations",
			linkTarget: "real",
			selected:   "migrations",
			bound:      "real",
			swapTo: func(root, decoy string) (string, string) {
				return filepath.Join(root, "migrations"), decoy
			},
		},
		{
			name:       "the migration directory symlink is swapped, with no project root",
			openRoot:   noSkeletonProjectRoot,
			dirs:       []string{"real"},
			link:       "migrations",
			linkTarget: "real",
			selected:   "migrations",
			bound:      "real",
			swapTo: func(root, decoy string) (string, string) {
				return filepath.Join(root, "migrations"), decoy
			},
		},
		{
			name:       "an ancestor symlink is swapped",
			openRoot:   pathguard.OpenCLIDirectory,
			dirs:       []string{"realnest/migrations"},
			decoyDirs:  []string{"migrations"},
			link:       "nest",
			linkTarget: "realnest",
			selected:   "nest/migrations",
			bound:      "realnest/migrations",
			swapTo: func(root, decoy string) (string, string) {
				return filepath.Join(root, "nest"), decoy
			},
			wantDecoyEntries: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := t.TempDir()
			decoy := t.TempDir()
			makeSkeletonDirs(c, root, test.dirs)
			makeSkeletonDirs(c, decoy, test.decoyDirs)
			c.Assert(os.Symlink(filepath.Join(root, test.linkTarget), filepath.Join(root, test.link)), qt.IsNil)

			opened, openErr := test.openRoot(root)
			c.Assert(openErr, qt.IsNil)
			defer func() { _ = closeOpenedRoot(opened) }()

			link, target := test.swapTo(root, decoy)
			afterSkeletonDirBound = func() { swapSkeletonSymlink(c, link, target) }
			defer func() { afterSkeletonDirBound = nil }()

			written, err := WriteSkeletonMigration(
				opened, filepath.Join(root, test.selected), atlasmigrateimport.FormatGolangMigrate, "added")

			c.Assert(err, qt.IsNil)
			c.Assert(written, qt.HasLen, 2)
			// The bound directory holds the pair plus atlas.sum, and the decoy
			// holds nothing this run put there.
			c.Assert(skeletonDirNames(c, filepath.Join(root, test.bound)), qt.HasLen, 3)
			c.Assert(skeletonDirNames(c, decoy), qt.HasLen, test.wantDecoyEntries)
		})
	}
}
