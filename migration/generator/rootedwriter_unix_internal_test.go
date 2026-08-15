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
	"time"

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
	pathnameVersion, pathnameErr := nextAvailablePtahVersion(
		migrationDirFileNames(writer.Path()), 100, "add_email",
	)
	c.Assert(pathnameErr, qt.IsNil)
	c.Assert(pathnameVersion, qt.Equals, int64(901))
}

// heldDirectoryPlan stages the shape the two publication tests below measure
// and stops where they diverge: a plan that has bound, captured and scanned a
// migration directory which a different one, carrying a higher version, has
// since taken the pathname of. It returns the plan and the three paths, with
// the bound directory living at aside and the impostor at selected.
//
// The shape is the one an operator can picture: no symlink anywhere, just a
// directory renamed aside while a plan holds it. The scan is asserted here
// because it is the same measurement in both tests, in the order PlanMigration
// runs it: bind, capture, scan, publish. The impostor carries a *higher*
// version than the directory the plan holds, so the two readings cannot agree
// by accident -- 106 beside the 105 the plan bound, against 901 beside the 900
// the impostor holds, and the last assertion here is what proves the fixture
// separates them.
func heldDirectoryPlan(c *qt.C) (plan *MigrationPlan, root, selected, aside string) {
	c.Helper()
	const upSQL = "CREATE TABLE users (id INTEGER);\n"
	const downSQL = "DROP TABLE users;\n"
	root = c.TempDir()
	selected = filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(selected, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(selected, migrator.GenerateMigrationFileName(105, "add_email", "up")),
		[]byte(upSQL), 0o600,
	), qt.IsNil)

	writer, err := bindPlannedMigrationDir("", selected)
	c.Assert(err, qt.IsNil)
	plannedContents, err := captureMigrationDirectoryContents(writer)
	c.Assert(err, qt.IsNil)

	// The hostile step: the bound directory is renamed aside and a different
	// one, holding a higher version, takes the pathname.
	aside = filepath.Join(root, "renamed-aside")
	c.Assert(os.Rename(selected, aside), qt.IsNil)
	c.Assert(os.MkdirAll(selected, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(selected, migrator.GenerateMigrationFileName(900, "decoy", "up")),
		[]byte(upSQL), 0o600,
	), qt.IsNil)

	version, err := nextAvailableMigrationVersion(writer, 100, "add_email")
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(106))
	pathnameVersion, pathnameErr := nextAvailablePtahVersion(
		migrationDirFileNames(writer.Path()), 100, "add_email",
	)
	c.Assert(pathnameErr, qt.IsNil)
	c.Assert(pathnameVersion, qt.Equals, int64(901))

	plan = &MigrationPlan{
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
	c.Cleanup(plan.release)
	return plan, root, selected, aside
}

// TestMigrationPlanRefusesPublicationWhileTheImpostorHoldsThePathname carries
// the scan next door through to publication, in the state a plan that
// revalidated by pathname would pass while publishing into the impostor: the
// batch must be refused outright, and the impostor must not receive one byte of
// it.
//
// //go:build unix because the hostile step is a rename of a directory the run
// holds open, which Win32 refuses without FILE_SHARE_DELETE -- on Windows there
// would be no step to perform rather than a step expected to fail.
func TestMigrationPlanRefusesPublicationWhileTheImpostorHoldsThePathname(t *testing.T) {
	c := qt.New(t)
	plan, _, selected, aside := heldDirectoryPlan(c)

	files, err := plan.WriteFilesContext(t.Context())

	c.Assert(err, qt.ErrorIs, ErrMigrationDirectoryChanged)
	c.Assert(files, qt.IsNil)
	c.Assert(generatorDirNames(c, aside), qt.DeepEquals, []string{
		migrator.GenerateMigrationFileName(105, "add_email", "up"),
	})
	c.Assert(generatorDirNames(c, selected), qt.DeepEquals, []string{
		migrator.GenerateMigrationFileName(900, "decoy", "up"),
	})
}

// TestMigrationPlanPublishesIntoTheHeldDirectoryWhenThePathnameReturns is the
// other half of the same claim, and it is the half the number alone does not
// carry: a version chosen from the held directory and then published somewhere
// else would be no better than a version chosen from the pathname.
//
// The pathname is handed back before publication, so the batch must land in the
// retained object under the number the scan chose. This fails if the plan gave
// up its claim, or renumbered against the impostor.
func TestMigrationPlanPublishesIntoTheHeldDirectoryWhenThePathnameReturns(t *testing.T) {
	c := qt.New(t)
	plan, root, selected, aside := heldDirectoryPlan(c)

	impostor := filepath.Join(root, "impostor")
	c.Assert(os.Rename(selected, impostor), qt.IsNil)
	c.Assert(os.Rename(aside, selected), qt.IsNil)

	files, err := plan.WriteFilesContext(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(files.Files, qt.HasLen, 1)
	c.Assert(files.Files[0].Version, qt.Equals, int64(106))
	c.Assert(generatorDirNames(c, selected), qt.DeepEquals, []string{
		migrator.GenerateMigrationFileName(105, "add_email", "up"),
		migrator.GenerateMigrationFileName(106, "add_email", "down"),
		migrator.GenerateMigrationFileName(106, "add_email", "up"),
	})
	c.Assert(generatorDirNames(c, impostor), qt.DeepEquals, []string{
		migrator.GenerateMigrationFileName(900, "decoy", "up"),
	})
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
		// selectedDir is the directory the run is pointed at, relative to the
		// temporary root, and it is staged before the run.
		selectedDir string
		// replacedDir is what the swap replaces once the directory is bound,
		// relative to the same root: the migration directory itself, or an
		// ancestor of it. retainedSubdir is what stands between that directory
		// and the migration directory, so the row names where the files must
		// land when the swap happens above the bound handle.
		replacedDir    string
		retainedSubdir string
		// decoySubdir is what the row stages inside the decoy: nothing, except
		// the ancestor row, which needs a migrations directory for the swapped
		// symlink to resolve through at all.
		decoySubdir string
		// wantFiles is how many files the layout writes: two for the Atlas
		// layout (the migration and atlas.sum) and two for the paired layout
		// (the up and down halves).
		wantFiles int
		// wantDecoyEntries is what the decoy must still hold afterwards, which
		// is exactly what the row staged in it and nothing the run added.
		wantDecoyEntries int
	}{
		{
			name:        "the migration directory is replaced, under an allowed root",
			allowedRoot: func(root string) string { return root },
			dirFormat:   migrator.MigrationDirFormatAtlas,
			selectedDir: "migrations",
			replacedDir: "migrations",
			wantFiles:   2,
		},
		{
			name:        "the migration directory is replaced, with no allowed root",
			allowedRoot: func(string) string { return "" },
			dirFormat:   migrator.MigrationDirFormatAtlas,
			selectedDir: "migrations",
			replacedDir: "migrations",
			wantFiles:   2,
		},
		{
			name:        "the migration directory is replaced, paired layout",
			allowedRoot: func(root string) string { return root },
			dirFormat:   migrator.MigrationDirFormatPtah,
			selectedDir: "migrations",
			replacedDir: "migrations",
			wantFiles:   2,
		},
		{
			name:             "an ancestor directory is replaced",
			allowedRoot:      func(root string) string { return root },
			dirFormat:        migrator.MigrationDirFormatAtlas,
			selectedDir:      filepath.Join("nest", "migrations"),
			replacedDir:      "nest",
			retainedSubdir:   "migrations",
			decoySubdir:      "migrations",
			wantFiles:        2,
			wantDecoyEntries: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := t.TempDir()
			decoy := t.TempDir()
			selected := filepath.Join(root, test.selectedDir)
			c.Assert(os.MkdirAll(selected, 0o755), qt.IsNil)
			c.Assert(os.MkdirAll(filepath.Join(decoy, test.decoySubdir), 0o755), qt.IsNil)

			var retained string
			afterMigrationWriterBound = func() {
				retained = filepath.Join(
					replaceWithSymlink(c, filepath.Join(root, test.replacedDir), decoy),
					test.retainedSubdir,
				)
			}
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
		// atlasClock is the instant the Atlas-layout stamp is frozen at for the
		// row. The paired layout stamps a Unix second instead and never reads
		// it, so every row carries one rather than the body choosing.
		atlasClock time.Time
		// wantVersion is the version the scan must choose over the bound
		// directory. wantPathnameVersion is what the same scan answers over the
		// pathname; the two differ, which is what makes the row a measurement.
		wantVersion         int64
		wantPathnameVersion int64
		// pathnameScan re-runs this layout's scan over a set of names, so the
		// row can state what reading the pathname would have produced.
		pathnameScan func(names []string) (int64, error)
		// wantRetained is the bound directory afterwards: what it already held,
		// plus what this transaction wrote into it.
		wantRetained []string
	}{
		{
			// The Atlas layout stamps the clock and only steps past a version
			// that is already taken (stokaro/ptah#938), so the fixture freezes
			// the clock and puts the frozen second in the BOUND directory only.
			// The bound reading has to step past it; the pathname reading, where
			// that second is free, does not.
			name:                "atlas layout",
			dirFormat:           migrator.MigrationDirFormatAtlas,
			boundFile:           atlasEmptyMigrationFileName(29990101000001, "seed"),
			impostorFile:        atlasEmptyMigrationFileName(29991231235959, "impostor"),
			atlasClock:          time.Date(2999, time.January, 1, 0, 0, 1, 0, time.UTC),
			wantVersion:         29990101000002,
			wantPathnameVersion: 29990101000001,
			pathnameScan: func(names []string) (int64, error) {
				return firstFreeAtlasVersion(names, nextAtlasMigrationVersion())
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
			atlasClock:          time.Date(2999, time.January, 1, 0, 0, 1, 0, time.UTC),
			wantVersion:         3000000006,
			wantPathnameVersion: 4000000000,
			pathnameScan: func(names []string) (int64, error) {
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
			atlasVersionClock = func() time.Time { return test.atlasClock }
			defer func() { atlasVersionClock = func() time.Time { return time.Now().UTC() } }()
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
			pathnameVersion, pathnameErr := test.pathnameScan(migrationDirFileNames(selected))
			c.Assert(pathnameErr, qt.IsNil)
			c.Assert(pathnameVersion, qt.Equals, test.wantPathnameVersion)
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
// genuinely lives outside the boundary the caller configured, and the bind has
// to refuse it.
//
// Which step refuses depends on the toolchain, so the assertion below pins the
// refusal rather than its wording. Through Go 1.26.5 os.Root.MkdirAll answered
// fs.ErrExist for an existing escaping component, rootMkdirAll tolerated that,
// and the rooted open was left to say "outside allowed root". Go 1.26.6 makes
// MkdirAll stat the component it found, so the escape surfaces at the create as
// "path escapes from parent" instead. Both are the same fail-closed answer, and
// the invariant that matters -- nothing was written outside -- is asserted
// separately and is unchanged.
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

	c.Assert(err, qt.ErrorMatches, `.*(outside allowed root|path escapes from parent).*`)
	c.Assert(plan, qt.IsNil)
	c.Assert(generatorDirNames(c, filepath.Join(outside, "migrations")), qt.HasLen, 0)
}
