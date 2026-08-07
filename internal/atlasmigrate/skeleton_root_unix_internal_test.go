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

// openSkeletonProjectRoot confines the run to root.
func openSkeletonProjectRoot(c *qt.C, root string) *pathguard.OpenedDirectory {
	c.Helper()
	opened, err := pathguard.OpenCLIDirectory(root)
	c.Assert(err, qt.IsNil)
	return opened
}

// noSkeletonProjectRoot is the direct-CLI shape.
func noSkeletonProjectRoot(*qt.C, string) *pathguard.OpenedDirectory { return nil }

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
		// openRoot returns the project root the run is confined to, or nil for
		// the direct-CLI shape where an explicit --dir is the operator's own
		// choice of destination.
		openRoot func(c *qt.C, root string) *pathguard.OpenedDirectory
		stage    func(c *qt.C, root, decoy string) (selected, bound string)
		swapTo   func(root, decoy string) (link, target string)
		// wantDecoyEntries is what the row staged in the decoy before the run:
		// nothing, except the ancestor row, which needs a migrations directory
		// for the swapped symlink to resolve to at all.
		wantDecoyEntries int
	}{
		{
			name:     "the migration directory symlink is swapped, under a project root",
			openRoot: openSkeletonProjectRoot,
			stage: func(c *qt.C, root, _ string) (string, string) {
				bound := filepath.Join(root, "real")
				c.Assert(os.MkdirAll(bound, 0o755), qt.IsNil)
				link := filepath.Join(root, "migrations")
				c.Assert(os.Symlink(bound, link), qt.IsNil)
				return link, bound
			},
			swapTo: func(root, decoy string) (string, string) {
				return filepath.Join(root, "migrations"), decoy
			},
		},
		{
			name:     "the migration directory symlink is swapped, with no project root",
			openRoot: noSkeletonProjectRoot,
			stage: func(c *qt.C, root, _ string) (string, string) {
				bound := filepath.Join(root, "real")
				c.Assert(os.MkdirAll(bound, 0o755), qt.IsNil)
				link := filepath.Join(root, "migrations")
				c.Assert(os.Symlink(bound, link), qt.IsNil)
				return link, bound
			},
			swapTo: func(root, decoy string) (string, string) {
				return filepath.Join(root, "migrations"), decoy
			},
		},
		{
			name:     "an ancestor symlink is swapped",
			openRoot: openSkeletonProjectRoot,
			stage: func(c *qt.C, root, decoy string) (string, string) {
				bound := filepath.Join(root, "realnest", "migrations")
				c.Assert(os.MkdirAll(bound, 0o755), qt.IsNil)
				c.Assert(os.MkdirAll(filepath.Join(decoy, "migrations"), 0o755), qt.IsNil)
				c.Assert(os.Symlink(filepath.Join(root, "realnest"), filepath.Join(root, "nest")), qt.IsNil)
				return filepath.Join(root, "nest", "migrations"), bound
			},
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
			selected, bound := test.stage(c, root, decoy)

			opened := test.openRoot(c, root)
			defer func() { _ = closeOpenedRoot(opened) }()

			link, target := test.swapTo(root, decoy)
			afterSkeletonDirBound = func() { swapSkeletonSymlink(c, link, target) }
			defer func() { afterSkeletonDirBound = nil }()

			written, err := WriteSkeletonMigration(opened, selected, atlasmigrateimport.FormatGolangMigrate, "added")

			c.Assert(err, qt.IsNil)
			c.Assert(written, qt.HasLen, 2)
			// The bound directory holds the pair plus atlas.sum, and the decoy
			// holds nothing this run put there.
			c.Assert(skeletonDirNames(c, bound), qt.HasLen, 3)
			c.Assert(skeletonDirNames(c, decoy), qt.HasLen, test.wantDecoyEntries)
		})
	}
}
