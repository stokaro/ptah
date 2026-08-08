//go:build unix

package generator

// White-box testing required: the moment these fixtures measure -- after the
// migration directory is bound and before the transaction reads or writes
// anything through it -- has no exported name, because nothing in the product
// needs one. A test that replaces the directory before calling the exported API
// only exercises the open, and the open is not what the rooted handle uniquely
// defends. Everything asserted below is otherwise observable: the files on disk
// and the contents of the directory the replacement pointed at.

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/migrator"
)

// These are the `migration/generator` half of the rooted-writer regressions
// stokaro/ptah#1118 asks for, and the counterpart to the TestGenerateDiff_*
// rows in internal/atlasmigrate and the TestWriteSkeletonMigration_* rows
// beside them.
//
// Every replacement below happens after the directory is bound: some swap the
// migration directory itself, one swaps an ancestor. Measured on master, the
// same replacement racing `ptah migrations create` published the migration file
// and a staged atlas.sum into a directory outside AllowedOutputRoot in 4 of 40
// runs, and a planned migration whose directory was replaced between planning
// and publication wrote both files into the replacement while reporting success.
//
// The file is //go:build unix because Win32 refuses to rename a directory the
// run holds open without FILE_SHARE_DELETE, so on Windows there is no hostile
// step to perform rather than a step that is expected to fail.

// replaceWithSymlink renames path aside and leaves a symlink to target in its
// place, which is what a pathname-based writer follows from here on. It returns
// the retained original, which is where a rooted writer's files must land.
func replaceWithSymlink(c *qt.C, path, target string) string {
	c.Helper()
	retained := path + "-retained"
	c.Assert(os.Rename(path, retained), qt.IsNil)
	c.Assert(os.Symlink(target, path), qt.IsNil)
	return retained
}

// generatorDirNames lists the base names in dir.
func generatorDirNames(c *qt.C, dir string) []string {
	c.Helper()
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

// TestGenerateEmptyMigrationReplacedDirectoryCannotRedirectTheWrite asserts
// both halves the acceptance criteria ask for: the migration lands in the
// directory the run bound, and the directory the replacement pointed at is left
// completely untouched.
//
// The second half is the one that fails when the rooted handle is removed.
// Without it, a run that merely errored out would look the same as one that
// stayed inside the boundary.
func TestGenerateEmptyMigrationReplacedDirectoryCannotRedirectTheWrite(t *testing.T) {
	tests := []struct {
		name string
		// allowedRoot returns the confinement root, or "" for the direct-CLI
		// shape where an explicit absolute directory is the operator's own
		// choice of destination.
		allowedRoot func(root string) string
		dirFormat   migrator.MigrationDirFormat
		// stage builds the tree and returns the directory the run selects.
		stage func(c *qt.C, root, decoy string) string
		// replace performs the swap once the directory is bound and returns the
		// directory the bound handle still points at.
		replace func(c *qt.C, root, decoy string) string
		// wantFiles is how many files the layout writes: two for the Atlas
		// layout (the migration and atlas.sum) and two for the paired layout
		// (the up and down halves).
		wantFiles int
		// wantDecoyEntries is what the row staged in the decoy before the run:
		// nothing, except the ancestor row, which needs a migrations directory
		// for the swapped symlink to resolve through at all.
		wantDecoyEntries int
	}{
		{
			name:        "the migration directory is replaced, under an allowed root",
			allowedRoot: func(root string) string { return root },
			dirFormat:   migrator.MigrationDirFormatAtlas,
			stage:       stageTopLevelMigrationDir,
			replace: func(c *qt.C, root, decoy string) string {
				return replaceWithSymlink(c, filepath.Join(root, "migrations"), decoy)
			},
			wantFiles: 2,
		},
		{
			name:        "the migration directory is replaced, with no allowed root",
			allowedRoot: func(string) string { return "" },
			dirFormat:   migrator.MigrationDirFormatAtlas,
			stage:       stageTopLevelMigrationDir,
			replace: func(c *qt.C, root, decoy string) string {
				return replaceWithSymlink(c, filepath.Join(root, "migrations"), decoy)
			},
			wantFiles: 2,
		},
		{
			name:        "the migration directory is replaced, paired layout",
			allowedRoot: func(root string) string { return root },
			dirFormat:   migrator.MigrationDirFormatPtah,
			stage:       stageTopLevelMigrationDir,
			replace: func(c *qt.C, root, decoy string) string {
				return replaceWithSymlink(c, filepath.Join(root, "migrations"), decoy)
			},
			wantFiles: 2,
		},
		{
			name:        "an ancestor directory is replaced",
			allowedRoot: func(root string) string { return root },
			dirFormat:   migrator.MigrationDirFormatAtlas,
			stage: func(c *qt.C, root, decoy string) string {
				c.Helper()
				c.Assert(os.MkdirAll(filepath.Join(root, "nest", "migrations"), 0o755), qt.IsNil)
				c.Assert(os.MkdirAll(filepath.Join(decoy, "migrations"), 0o755), qt.IsNil)
				return filepath.Join(root, "nest", "migrations")
			},
			replace: func(c *qt.C, root, decoy string) string {
				return filepath.Join(
					replaceWithSymlink(c, filepath.Join(root, "nest"), decoy),
					"migrations",
				)
			},
			wantFiles:        2,
			wantDecoyEntries: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := t.TempDir()
			decoy := t.TempDir()
			selected := test.stage(c, root, decoy)

			var retained string
			afterMigrationWriterBound = func() { retained = test.replace(c, root, decoy) }
			defer func() { afterMigrationWriterBound = nil }()

			files, err := GenerateEmptyMigration(EmptyMigrationOptions{
				MigrationName:     "added",
				OutputDir:         selected,
				AllowedOutputRoot: test.allowedRoot(root),
				DirFormat:         test.dirFormat,
			})

			c.Assert(err, qt.IsNil)
			c.Assert(files, qt.IsNotNil)
			c.Assert(generatorDirNames(c, retained), qt.HasLen, test.wantFiles)
			c.Assert(generatorDirNames(c, decoy), qt.HasLen, test.wantDecoyEntries)
		})
	}
}

// stageTopLevelMigrationDir prepares root/migrations as the selected directory.
func stageTopLevelMigrationDir(c *qt.C, root, _ string) string {
	c.Helper()
	selected := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(selected, 0o755), qt.IsNil)
	return selected
}

// TestGenerateEmptyMigrationCreatesMissingDirectoryThroughTheBoundParent covers
// the initially-missing directory the acceptance criteria name: it is
// materialized through the parent handle bound for the transaction, so a
// replacement of the parent's name cannot move where it is created.
func TestGenerateEmptyMigrationCreatesMissingDirectoryThroughTheBoundParent(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	decoy := t.TempDir()
	c.Assert(os.MkdirAll(filepath.Join(root, "nest"), 0o755), qt.IsNil)
	selected := filepath.Join(root, "nest", "migrations")

	var retained string
	afterMigrationWriterBound = func() {
		retained = replaceWithSymlink(c, filepath.Join(root, "nest"), decoy)
	}
	defer func() { afterMigrationWriterBound = nil }()

	files, err := GenerateEmptyMigration(EmptyMigrationOptions{
		MigrationName:     "added",
		OutputDir:         selected,
		AllowedOutputRoot: root,
		DirFormat:         migrator.MigrationDirFormatAtlas,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.IsNotNil)
	c.Assert(generatorDirNames(c, filepath.Join(retained, "migrations")), qt.HasLen, 2)
	c.Assert(generatorDirNames(c, decoy), qt.HasLen, 0)
}

// TestMigrationPlanWriteFilesReplacedDirectoryCannotRedirectPublication is the
// same measurement for the planned-migration writer, staged in the one window
// that writer still has: after the publication has revalidated the directory it
// holds and before it writes a byte.
//
// A replacement landing earlier -- any time between planning and publication --
// is refused outright, and TestMigrationPlanWriteFiles_RefusesRecreatedDirectory
// next door measures that. A replacement landing here cannot be refused, because
// the verification has already happened; the only thing that can save the batch
// is that the write does not consult the pathname at all. So the assertion is
// the positive one: the files land in the object the plan bound, and the
// directory the replacement points at is left completely untouched.
func TestMigrationPlanWriteFilesReplacedDirectoryCannotRedirectPublication(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	decoy := t.TempDir()
	outputDir := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(outputDir, 0o755), qt.IsNil)

	plan, err := NewMigrationPlanForTest(outputDir, root, "", []MigrationPlanSpecForTest{{
		Version: 1700000000,
		Name:    "create_users",
		UpSQL:   "CREATE TABLE users (id INTEGER);\n",
		DownSQL: "DROP TABLE users;\n",
	}})
	c.Assert(err, qt.IsNil)

	var retained string
	afterMigrationPublicationVerified = func() { retained = replaceWithSymlink(c, outputDir, decoy) }
	defer func() { afterMigrationPublicationVerified = nil }()

	files, err := plan.WriteFilesContext(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(files.Files, qt.HasLen, 1)
	c.Assert(generatorDirNames(c, retained), qt.DeepEquals, []string{
		"1700000000_create_users.down.sql",
		"1700000000_create_users.up.sql",
	})
	c.Assert(generatorDirNames(c, decoy), qt.HasLen, 0)
}

// TestMigrationPlanBindRefusesADirectoryReachedThroughAnAncestorOutsideTheRoot
// pins what the confinement root contributes to a plan, which is not the same
// thing as what the bound handle contributes.
//
// The handle alone keeps every write inside the object it opened, whatever the
// pathname later resolves to -- that is the row above. It cannot decide that the
// object should never have been opened. Here `nest` is a symlink out of the
// allowed root before the plan binds anything, so the migration directory
// genuinely lives outside the boundary the caller configured, and the only step
// that can say so is the rooted open.
//
// //go:build unix because the escape is a symlink.
func TestMigrationPlanBindRefusesADirectoryReachedThroughAnAncestorOutsideTheRoot(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	outside := t.TempDir()
	c.Assert(os.MkdirAll(filepath.Join(outside, "migrations"), 0o755), qt.IsNil)
	c.Assert(os.Symlink(outside, filepath.Join(root, "nest")), qt.IsNil)

	plan, err := NewMigrationPlanForTest(
		filepath.Join(root, "nest", "migrations"),
		root,
		"",
		[]MigrationPlanSpecForTest{{
			Version: 1700000000,
			Name:    "create_users",
			UpSQL:   "CREATE TABLE users (id INTEGER);\n",
			DownSQL: "DROP TABLE users;\n",
		}},
	)

	c.Assert(err, qt.ErrorMatches, `.*outside allowed root.*`)
	c.Assert(plan, qt.IsNil)
	c.Assert(generatorDirNames(c, filepath.Join(outside, "migrations")), qt.HasLen, 0)
}
