//go:build !windows

package atlasmigrate_test

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/pathguard"
)

// These tests replace the migration directory, or one of its ancestors, after
// the run has captured and verified it and before anything is published. The
// swap is the exact shape stokaro/ptah#1118 describes: the writer used to carry
// a string path and resolve it again for staging, publication, the atlas.sum
// commit and recovery, so a pathname that pointed at one directory when it was
// validated could point at another by the time it was written.
//
// Every row asserts BOTH halves: the artifacts appear inside the directory the
// run opened, and the replacement directory is left completely untouched. The
// second half is the one that fails when the rooted handle is removed --
// without it a run that merely errors out would look the same as one that
// stayed inside the root.

// swapMigrationSymlink replaces link so it points at target. It is the hostile
// step every row below performs from inside a callback the run invokes while
// holding the migration-directory lock.
func swapMigrationSymlink(c *qt.C, link, target string) {
	c.Helper()
	c.Assert(os.Remove(link), qt.IsNil)
	c.Assert(os.Symlink(target, link), qt.IsNil)
}

func dirEntryNames(c *qt.C, dir string) []string {
	c.Helper()
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func writeDiffDesiredSchema(c *qt.C, path string) {
	c.Helper()
	c.Assert(os.WriteFile(path, []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  email TEXT NOT NULL DEFAULT ''
);
`), 0o600), qt.IsNil)
}

func openDiffProjectRoot(c *qt.C, root string) *pathguard.OpenedDirectory {
	c.Helper()
	opened, err := pathguard.OpenDirectory(root)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(opened.Close(), qt.IsNil)
	})
	return opened
}

// diffSwapCase is one hostile replacement staged through a callback the diff
// run makes while it holds the lock.
type diffSwapCase struct {
	name string
	// stage builds the directory layout and returns the --dir value, the
	// directory the handles must stay bound to, and the decoy the swap points
	// at. Both directories start empty, so a run that follows the swapped
	// pathname passes the planning recheck and publishes into the decoy.
	stage func(c *qt.C, root, decoy string) (dirFlag, bound string)
	// swap performs the replacement.
	swap func(c *qt.C, root, decoy string)
	// hook selects which callback performs the swap.
	hook func(opts *atlasmigrate.DiffOptions, swap func())
	// projectRoot selects the boundary the run is given: the opened project root
	// for a directory that came from atlas.hcl, or none for a --dir the operator
	// named directly. Both must stay bound to the object they opened; only the
	// containment rule differs between them.
	projectRoot func(c *qt.C, root string) *pathguard.OpenedDirectory
}

func noProjectRoot(*qt.C, string) *pathguard.OpenedDirectory {
	return nil
}

func verifyDirHook(opts *atlasmigrate.DiffOptions, swap func()) {
	opts.VerifyDir = func(fs.FS) error {
		swap()
		return nil
	}
}

func preparePublicationHook(opts *atlasmigrate.DiffOptions, swap func()) {
	opts.PreparePublication = func([]string) error {
		swap()
		return nil
	}
}

func TestGenerateDiff_ReplacedDirectoryCannotRedirectPublication(t *testing.T) {
	tests := []diffSwapCase{
		{
			name: "migration directory symlink swapped before planning",
			stage: func(c *qt.C, root, _ string) (string, string) {
				bound := filepath.Join(root, "real")
				c.Assert(os.MkdirAll(bound, 0o755), qt.IsNil)
				link := filepath.Join(root, "migrations")
				c.Assert(os.Symlink(bound, link), qt.IsNil)
				return link, bound
			},
			swap: func(c *qt.C, root, decoy string) {
				swapMigrationSymlink(c, filepath.Join(root, "migrations"), decoy)
			},
			hook:        verifyDirHook,
			projectRoot: openDiffProjectRoot,
		},
		{
			name: "migration directory symlink swapped with no project root",
			stage: func(c *qt.C, root, _ string) (string, string) {
				bound := filepath.Join(root, "real")
				c.Assert(os.MkdirAll(bound, 0o755), qt.IsNil)
				link := filepath.Join(root, "migrations")
				c.Assert(os.Symlink(bound, link), qt.IsNil)
				return link, bound
			},
			swap: func(c *qt.C, root, decoy string) {
				swapMigrationSymlink(c, filepath.Join(root, "migrations"), filepath.Join(decoy, "migrations"))
			},
			hook:        verifyDirHook,
			projectRoot: noProjectRoot,
		},
		{
			name: "ancestor symlink swapped before planning",
			stage: func(c *qt.C, root, _ string) (string, string) {
				bound := filepath.Join(root, "realnest", "migrations")
				c.Assert(os.MkdirAll(bound, 0o755), qt.IsNil)
				// The link target is relative on purpose. os.Root resolves an
				// absolute target against the process root, so it reports an
				// escape even for a link that points at a sibling inside this
				// very directory, and the run is then refused before it plans
				// anything -- see
				// TestGenerateDiff_AbsoluteAncestorSymlinkIsRefusedBeforePlanning.
				// A relative target keeps the ancestor reachable, which is what
				// this row needs in order to reach the swap at all.
				c.Assert(os.Symlink("realnest", filepath.Join(root, "nest")), qt.IsNil)
				return filepath.Join(root, "nest", "migrations"), bound
			},
			swap: func(c *qt.C, root, decoy string) {
				swapMigrationSymlink(c, filepath.Join(root, "nest"), decoy)
			},
			hook:        verifyDirHook,
			projectRoot: openDiffProjectRoot,
		},
		{
			name: "migration directory symlink swapped during editor preparation",
			stage: func(c *qt.C, root, _ string) (string, string) {
				bound := filepath.Join(root, "real")
				c.Assert(os.MkdirAll(bound, 0o755), qt.IsNil)
				link := filepath.Join(root, "migrations")
				c.Assert(os.Symlink(bound, link), qt.IsNil)
				return link, bound
			},
			swap: func(c *qt.C, root, decoy string) {
				swapMigrationSymlink(c, filepath.Join(root, "migrations"), decoy)
			},
			hook:        preparePublicationHook,
			projectRoot: openDiffProjectRoot,
		},
	}

	for _, test := range tests {
		// Converted by hand rather than by qtlint, which withholds the fix
		// here: the closure hands its *qt.C to the row's staging functions,
		// and one of them registers a Cleanup on it. That is the withholding
		// working -- the tool cannot bound what a *qt.C reaches through a
		// field. It is safe all the same, and for the reason the rule is
		// about: every one of those calls now receives the subtest's own
		// checker, so the opened directory closes with the subtest that
		// opened it instead of with the parent.
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := c.TempDir()
			decoy := c.TempDir()
			c.Assert(os.MkdirAll(filepath.Join(decoy, "migrations"), 0o755), qt.IsNil)
			dirFlag, bound := test.stage(c, root, decoy)
			schemaPath := filepath.Join(root, "schema.sql")
			writeDiffDesiredSchema(c, schemaPath)
			conn := connectSQLite(c, filepath.Join(root, "dev.db"))
			defer dbschema.CloseAndWarn(conn)

			opts := atlasmigrate.DiffOptions{
				Dir:         dirFlag,
				Root:        test.projectRoot(c, root),
				Desired:     localDesiredSet(c, "file://"+schemaPath),
				Name:        "add_email",
				LockTimeout: time.Second,
			}
			test.hook(&opts, func() { test.swap(c, root, decoy) })

			result, err := atlasmigrate.GenerateDiff(context.Background(), conn, opts)

			c.Assert(err, qt.IsNil)
			c.Assert(result.MigrationPaths, qt.HasLen, 1)
			// The run published into the object it opened, not into whatever the
			// pathname selects now.
			c.Assert(atlasSQLFiles(c, bound), qt.HasLen, 1)
			c.Assert(fileExists(filepath.Join(bound, "atlas.sum")), qt.IsTrue)
			// No migration file, checksum, journal, staging or recovery artifact
			// reached the replacement.
			c.Assert(dirEntryNames(c, decoy), qt.DeepEquals, []string{"migrations"})
			c.Assert(dirEntryNames(c, filepath.Join(decoy, "migrations")), qt.HasLen, 0)
		})
	}
}

// TestGenerateDiff_AbsoluteAncestorSymlinkIsRefusedBeforePlanning pins a
// behavior the Go 1.26.6 toolchain changed, which is why the ancestor row above
// stages a relative link instead.
//
// os.Root resolves an absolute symlink target against the process root, so it
// reports an escape even when the target is a sibling inside the same project
// root -- as `nest` is here. Through Go 1.26.5 os.Root.MkdirAll answered
// fs.ErrExist for such an existing component, and rootMkdirAll deliberately
// tolerates fs.ErrExist because containment is the rooted open's job rather
// than the create's, so the run reached planning and published into the
// directory it had opened. Go 1.26.6 makes MkdirAll stat the component it found,
// so the escape surfaces at the create and the run refuses first.
//
// The refusal is the fail-closed direction and the standard library exports no
// sentinel for it, so this records the behavior rather than string-matching an
// unexported error in order to undo it. The property that did not change, and
// the one asserted below, is that a refused run writes nothing anywhere.
func TestGenerateDiff_AbsoluteAncestorSymlinkIsRefusedBeforePlanning(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()

	bound := filepath.Join(root, "realnest", "migrations")
	c.Assert(os.MkdirAll(bound, 0o755), qt.IsNil)
	c.Assert(os.Symlink(filepath.Join(root, "realnest"), filepath.Join(root, "nest")), qt.IsNil)

	schemaPath := filepath.Join(root, "schema.sql")
	writeDiffDesiredSchema(c, schemaPath)
	conn := connectSQLite(c, filepath.Join(root, "dev.db"))
	defer dbschema.CloseAndWarn(conn)

	result, err := atlasmigrate.GenerateDiff(context.Background(), conn, atlasmigrate.DiffOptions{
		Dir:         filepath.Join(root, "nest", "migrations"),
		Root:        openDiffProjectRoot(c, root),
		Desired:     localDesiredSet(c, "file://"+schemaPath),
		Name:        "add_email",
		LockTimeout: time.Second,
	})

	c.Assert(err, qt.ErrorMatches, `create migration directory parent: .*`)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	c.Assert(atlasSQLFiles(c, bound), qt.HasLen, 0)
	c.Assert(fileExists(filepath.Join(bound, "atlas.sum")), qt.IsFalse)
}

func TestGenerateDiff_CreatesMissingMigrationDirectoryInsideOpenedRoot(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	schemaPath := filepath.Join(root, "schema.sql")
	writeDiffDesiredSchema(c, schemaPath)
	conn := connectSQLite(c, filepath.Join(root, "dev.db"))
	defer dbschema.CloseAndWarn(conn)
	// Neither the migration directory nor its parent exists yet, so both are
	// materialized through the opened root rather than through a pathname.
	migrationsDir := filepath.Join(root, "nest", "migrations")

	result, err := atlasmigrate.GenerateDiff(context.Background(), conn, atlasmigrate.DiffOptions{
		Dir:         migrationsDir,
		Root:        openDiffProjectRoot(c, root),
		Desired:     localDesiredSet(c, "file://"+schemaPath),
		Name:        "add_email",
		LockTimeout: time.Second,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.MigrationPaths, qt.HasLen, 1)
	c.Assert(atlasSQLFiles(c, migrationsDir), qt.HasLen, 1)
	c.Assert(result.SumPath, qt.Equals, filepath.Join(migrationsDir, "atlas.sum"))
	c.Assert(fileExists(result.SumPath), qt.IsTrue)
}

func TestGenerateDiff_RefusesMigrationDirectoryOutsideOpenedRoot(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	outside := c.TempDir()
	c.Assert(os.Symlink(outside, filepath.Join(root, "migrations")), qt.IsNil)
	schemaPath := filepath.Join(root, "schema.sql")
	writeDiffDesiredSchema(c, schemaPath)
	conn := connectSQLite(c, filepath.Join(root, "dev.db"))
	defer dbschema.CloseAndWarn(conn)

	result, err := atlasmigrate.GenerateDiff(context.Background(), conn, atlasmigrate.DiffOptions{
		Dir:         filepath.Join(root, "migrations"),
		Root:        openDiffProjectRoot(c, root),
		Desired:     localDesiredSet(c, "file://"+schemaPath),
		Name:        "add_email",
		LockTimeout: time.Second,
	})

	c.Assert(err, qt.ErrorIs, pathguard.ErrOutsideRoot)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	c.Assert(dirEntryNames(c, outside), qt.HasLen, 0)
}
