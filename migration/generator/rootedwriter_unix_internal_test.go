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

// TestNextAvailableMigrationVersionReadsTheHeldDirectory pins the half of the
// binding that no publication guard can reach: the version scan.
//
// The scan runs during planning, between binding the migration directory and
// recording the specs, and it decides which version numbers the plan may use.
// Reading it by pathname means the plan avoids colliding with whatever the name
// resolves to at that instant, which is not necessarily the directory it holds
// and will publish into; the collision it was supposed to avoid then happens at
// publication, against files the scan never saw.
//
// Every other fixture in the package stages a directory whose pathname and
// bound object are the same, so the pathname-based scan and the handle-based
// one answer identically and neither is pinned. This one makes them answer
// differently on purpose, and the last assertion is what proves the fixture
// separates them: read by pathname the same writer would say 901.
//
// //go:build unix because the divergence is staged with a symlink swap. The
// bound directory has to survive the swap, which rules out removing and
// recreating it -- an unlinked directory lists as empty and both answers would
// collapse back together.
func TestNextAvailableMigrationVersionReadsTheHeldDirectory(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	bound := filepath.Join(root, "real")
	decoy := filepath.Join(root, "decoy")
	c.Assert(os.MkdirAll(bound, 0o755), qt.IsNil)
	c.Assert(os.MkdirAll(decoy, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(bound, migrator.GenerateMigrationFileName(105, "add_email", "up")),
		[]byte("SELECT 1;\n"), 0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(decoy, migrator.GenerateMigrationFileName(900, "decoy", "up")),
		[]byte("SELECT 1;\n"), 0o600,
	), qt.IsNil)
	selected := filepath.Join(root, "migrations")
	c.Assert(os.Symlink(bound, selected), qt.IsNil)

	writer, err := bindPlannedMigrationDir("", selected)
	c.Assert(err, qt.IsNil)
	defer func() { _ = writer.Close() }()

	// From here the pathname the writer was selected by names a different
	// directory, with a different highest version in it.
	c.Assert(os.Remove(selected), qt.IsNil)
	c.Assert(os.Symlink(decoy, selected), qt.IsNil)

	version, err := nextAvailableMigrationVersion(writer, 100, "add_email")

	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(106))
	c.Assert(
		nextAvailablePtahVersion(migrationDirFileNames(writer.Path()), 100, "add_email"),
		qt.Equals,
		int64(901),
	)
}

// TestMigrationPlanUsesTheHeldDirectoryForVersionAndPublication carries the row
// above through to publication, on the shape an operator can picture: no
// symlink anywhere, just a directory renamed aside while a plan holds it and a
// different directory taking the pathname it was selected by.
//
// The scan row next door stops at the number. The number is only half of what
// the binding claims, because a version chosen from the held directory and then
// published somewhere else would be no better than a version chosen from the
// pathname. Both halves are asserted here against the same plan, in the order
// PlanMigration runs them: bind, capture, scan, publish.
//
// The impostor at the pathname carries a *higher* version than the directory
// the plan holds, so the two readings cannot agree by accident -- 106 beside
// the 105 the plan bound, against 901 beside the 900 the impostor holds. The
// third assertion after the scan is what proves the fixture separates them.
//
// The two rows differ in what the pathname names by publication time, and they
// fail in opposite ways:
//
//   - impostor still there: the batch must be refused outright, and the
//     impostor must not receive one byte of it. This is the row a plan that
//     revalidated by pathname would pass while publishing into the impostor.
//   - pathname handed back: the batch must land in the retained object under
//     the number the scan chose. This is the row that fails if the plan gave
//     up its claim, or renumbered against the impostor.
//
// //go:build unix because the hostile step is a rename of a directory the run
// holds open, which Win32 refuses without FILE_SHARE_DELETE -- on Windows there
// would be no step to perform rather than a step expected to fail.
func TestMigrationPlanUsesTheHeldDirectoryForVersionAndPublication(t *testing.T) {
	const upSQL = "CREATE TABLE users (id INTEGER);\n"
	const downSQL = "DROP TABLE users;\n"
	boundFile := migrator.GenerateMigrationFileName(105, "add_email", "up")
	impostorFile := migrator.GenerateMigrationFileName(900, "decoy", "up")
	publishedUp := migrator.GenerateMigrationFileName(106, "add_email", "up")
	publishedDown := migrator.GenerateMigrationFileName(106, "add_email", "down")

	tests := []struct {
		name string
		// settle runs after the version has been chosen and before publication,
		// and reports where the bound object and the impostor live by then.
		settle func(c *qt.C, root, selected, aside string) (bound, impostor string)
		// check asserts what the plan's one publication attempt returned.
		check func(c *qt.C, files *MigrationFiles, err error)
		// wantBound and wantImpostor are the two directories afterwards.
		wantBound    []string
		wantImpostor []string
	}{
		{
			name: "the impostor still holds the pathname at publication",
			settle: func(_ *qt.C, _, selected, aside string) (string, string) {
				return aside, selected
			},
			check: func(c *qt.C, files *MigrationFiles, err error) {
				c.Assert(err, qt.ErrorIs, ErrMigrationDirectoryChanged)
				c.Assert(files, qt.IsNil)
			},
			wantBound:    []string{boundFile},
			wantImpostor: []string{impostorFile},
		},
		{
			name: "the pathname names the bound directory again at publication",
			settle: func(c *qt.C, root, selected, aside string) (string, string) {
				impostor := filepath.Join(root, "impostor")
				c.Assert(os.Rename(selected, impostor), qt.IsNil)
				c.Assert(os.Rename(aside, selected), qt.IsNil)
				return selected, impostor
			},
			check: func(c *qt.C, files *MigrationFiles, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(files.Files, qt.HasLen, 1)
				c.Assert(files.Files[0].Version, qt.Equals, int64(106))
			},
			wantBound:    []string{boundFile, publishedDown, publishedUp},
			wantImpostor: []string{impostorFile},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := t.TempDir()
			selected := filepath.Join(root, "migrations")
			c.Assert(os.MkdirAll(selected, 0o755), qt.IsNil)
			c.Assert(os.WriteFile(
				filepath.Join(selected, boundFile), []byte(upSQL), 0o600,
			), qt.IsNil)

			writer, err := bindPlannedMigrationDir("", selected)
			c.Assert(err, qt.IsNil)
			plannedContents, err := captureMigrationDirectoryContents(writer)
			c.Assert(err, qt.IsNil)

			// The hostile step: the bound directory is renamed aside and a
			// different one, holding a higher version, takes the pathname.
			aside := filepath.Join(root, "renamed-aside")
			c.Assert(os.Rename(selected, aside), qt.IsNil)
			c.Assert(os.MkdirAll(selected, 0o755), qt.IsNil)
			c.Assert(os.WriteFile(
				filepath.Join(selected, impostorFile), []byte(upSQL), 0o600,
			), qt.IsNil)

			version, err := nextAvailableMigrationVersion(writer, 100, "add_email")
			c.Assert(err, qt.IsNil)
			c.Assert(version, qt.Equals, int64(106))
			c.Assert(
				nextAvailablePtahVersion(migrationDirFileNames(writer.Path()), 100, "add_email"),
				qt.Equals,
				int64(901),
			)

			plan := &MigrationPlan{
				outputDir:       selected,
				dir:             writer,
				plannedContents: plannedContents,
				specs: []generatedMigrationSpec{{
					Version: version,
					Name:    "add_email",
					UpSQL:   upSQL,
					DownSQL: downSQL,
				}},
			}
			defer plan.release()

			bound, impostor := test.settle(c, root, selected, aside)

			files, err := plan.WriteFilesContext(t.Context())

			test.check(c, files, err)
			c.Assert(generatorDirNames(c, bound), qt.DeepEquals, test.wantBound)
			c.Assert(generatorDirNames(c, impostor), qt.DeepEquals, test.wantImpostor)
		})
	}
}

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

// TestGenerateEmptyMigrationScansTheHeldDirectoryForItsVersion is the skeleton
// writer's half of the scan the planned writer's rows above pin, and it was the
// half nothing measured: reverting writeEmptyMigrationFiles to list the pathname
// left migration/generator, internal/atlasmigrate and internal/pathguard all at
// exit 0, while the same revert in the planned writer reddened two rows.
//
// The two writers ask the same question at the same moment and only one of them
// was held to the answer. Every other fixture for this one stages a directory
// whose pathname and bound object are the same, or an empty decoy that carries
// no version at all, so a pathname-based scan and a handle-based one agree by
// construction. The row next door,
// TestGenerateEmptyMigrationReplacedDirectoryCannotRedirectTheWrite, pins where
// the bytes land and nothing about which number is on them.
//
// What a scan over the pathname costs is a version chosen to avoid colliding
// with a directory this transaction is not writing into. The number it picks is
// then free in the impostor and, as here, already taken in the directory the
// handle holds -- so the write either collides with a migration the scan never
// saw, or numbers a new migration behind one that is already applied.
//
// The impostor carries a much higher version than the bound directory, so the
// two readings cannot agree by accident, and the last assertion of each row is
// what proves the fixture separates them rather than hoping it does.
//
// The assertion is on the version alone. Where the bytes land is already pinned
// next door, and the rooted handle keeps them in the retained directory under
// either reading -- which is exactly why a wrong number here is invisible to a
// destination assertion.
//
// This row lives in the //go:build unix file for the reason stated above it: the
// hostile step is a rename of a directory the run holds open, which Win32
// refuses without FILE_SHARE_DELETE. It asserts which directory was listed, not
// whether two identifiers match, so it does not depend on the filesystem
// reissuing an inode -- measured over 20 remove-and-recreate cycles of one
// pathname, ext4 reissues the inode every time and APFS never does, and this row
// answers the same on both.
func TestGenerateEmptyMigrationScansTheHeldDirectoryForItsVersion(t *testing.T) {
	tests := []struct {
		name      string
		dirFormat migrator.MigrationDirFormat
		// boundFile is the migration already in the directory the run binds,
		// and impostorFile the one in the directory that takes over its
		// pathname before the scan runs.
		boundFile    string
		impostorFile string
		// wantVersion is the version the scan must choose, one past the bound
		// directory's highest. wantPathnameVersion is what the same scan
		// answers over the pathname, one past the impostor's.
		wantVersion         int64
		wantPathnameVersion int64
		// pathnameScan re-runs this layout's scan over a set of names, so the
		// row can state what reading the pathname would have produced.
		pathnameScan func(names []string) int64
		// wantRetained is the bound directory afterwards: what it already held,
		// plus what this transaction wrote into it.
		wantRetained []string
	}{
		{
			name:                "atlas layout",
			dirFormat:           migrator.MigrationDirFormatAtlas,
			boundFile:           atlasEmptyMigrationFileName(29990101000001, "seed"),
			impostorFile:        atlasEmptyMigrationFileName(29991231235959, "impostor"),
			wantVersion:         29990101000002,
			wantPathnameVersion: 29991231235960,
			pathnameScan: func(names []string) int64 {
				return nextAvailableAtlasVersion(names, nextAtlasMigrationVersion())
			},
			wantRetained: []string{
				atlasEmptyMigrationFileName(29990101000001, "seed"),
				atlasEmptyMigrationFileName(29990101000002, "added"),
				"atlas.sum",
			},
		},
		{
			name:                "paired layout",
			dirFormat:           migrator.MigrationDirFormatPtah,
			boundFile:           migrator.GenerateMigrationFileName(3000000005, "seed", "up"),
			impostorFile:        migrator.GenerateMigrationFileName(3999999999, "impostor", "up"),
			wantVersion:         3000000006,
			wantPathnameVersion: 4000000000,
			pathnameScan: func(names []string) int64 {
				return nextAvailablePtahVersion(names, migrator.GetNextMigrationVersion(), "added")
			},
			wantRetained: []string{
				migrator.GenerateMigrationFileName(3000000005, "seed", "up"),
				migrator.GenerateMigrationFileName(3000000006, "added", "down"),
				migrator.GenerateMigrationFileName(3000000006, "added", "up"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := t.TempDir()
			selected := filepath.Join(root, "migrations")
			aside := filepath.Join(root, "renamed-aside")
			c.Assert(os.MkdirAll(selected, 0o755), qt.IsNil)
			c.Assert(os.WriteFile(
				filepath.Join(selected, test.boundFile), []byte("SELECT 1;\n"), 0o600,
			), qt.IsNil)

			// The hostile step, staged in the one window the binding defends:
			// after the directory is bound and before the transaction reads
			// anything through it. The bound directory is renamed aside, keeping
			// its contents, and a fresh directory holding a far higher version
			// takes the pathname the writer was selected by.
			afterMigrationWriterBound = func() {
				c.Assert(os.Rename(selected, aside), qt.IsNil)
				c.Assert(os.MkdirAll(selected, 0o755), qt.IsNil)
				c.Assert(os.WriteFile(
					filepath.Join(selected, test.impostorFile), []byte("SELECT 1;\n"), 0o600,
				), qt.IsNil)
			}
			defer func() { afterMigrationWriterBound = nil }()

			files, err := GenerateEmptyMigration(EmptyMigrationOptions{
				MigrationName:     "added",
				OutputDir:         selected,
				AllowedOutputRoot: root,
				DirFormat:         test.dirFormat,
			})

			c.Assert(err, qt.IsNil)
			c.Assert(files, qt.IsNotNil)
			c.Assert(files.Files, qt.HasLen, 1)
			c.Assert(files.Files[0].Version, qt.Equals, test.wantVersion)
			c.Assert(generatorDirNames(c, aside), qt.DeepEquals, test.wantRetained)
			c.Assert(generatorDirNames(c, selected), qt.DeepEquals, []string{test.impostorFile})
			c.Assert(
				test.pathnameScan(migrationDirFileNames(selected)),
				qt.Equals,
				test.wantPathnameVersion,
			)
		})
	}
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
