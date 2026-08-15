//go:build unix

package generator

// White-box testing required: the two moments these fixtures measure -- after
// the output directory is bound and before the transaction writes through it,
// and after the final file names are chosen and before the exclusive create --
// have no exported name, because nothing in the product needs one. A test that
// replaces the directory, or pre-places a file at the final name, before
// calling the exported API measures the open instead of the commit, which is
// the distinction stokaro/ptah#1118 is about. Everything asserted below is
// otherwise observable: the files on disk and the contents of the directory the
// replacement pointed at.

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/migrationsnapshot"
	"go.5x5.cz/ptah/migration/migrator"
)

// These are the checkpoint and data-migration writers' half of the rooted-writer
// regressions stokaro/ptah#1118 asks for, and the counterpart to the
// TestGenerateEmptyMigration* rows next door.
//
// Measured on master at 69c5b2ea, the same replacement staged against these
// three writers returned a nil error and split one transaction across two
// directories: `WriteAtlasCheckpointFile` left the checkpoint in the retained
// directory and wrote atlas.sum into the directory that took over the pathname,
// and both pair writers left the up half in the retained directory while the
// down half and ptah.sum went to the impostor. Every one of those outcomes is a
// directory no reader will accept -- the retained one uncovered, the impostor
// carrying a checksum for a snapshot it never held.
//
// The file is //go:build unix because Win32 refuses to rename a directory the
// run holds open without FILE_SHARE_DELETE, so on Windows there is no hostile
// step to perform rather than a step that is expected to fail.

const (
	checkpointWriterVersion      = 2099010100
	atlasCheckpointWriterVersion = 20990101000000
)

func TestAuthorizedCheckpointWritersDoNotHashAConcurrentHistoryEdit(t *testing.T) {
	tests := []struct {
		name       string
		priorNames []string
		write      func(string, fs.FS) error
		checkpoint func(string) ([]string, error)
		sumName    string
	}{
		{
			name:       "atlas checkpoint",
			priorNames: []string{"20200101000000_init.sql"},
			write: func(dir string, authorized fs.FS) error {
				_, err := WriteAtlasCheckpointFileWithOptions(
					dir,
					atlasCheckpointWriterVersion,
					"squash",
					"CREATE TABLE users (id integer);\n",
					CheckpointWriteOptions{AuthorizedMigrationsFS: authorized},
				)
				return err
			},
			checkpoint: func(dir string) ([]string, error) {
				return filepath.Glob(filepath.Join(dir, "*_squash.sql"))
			},
			sumName: "atlas.sum",
		},
		{
			name: "paired checkpoint",
			priorNames: []string{
				"0000000001_init.up.sql",
				"0000000001_init.down.sql",
			},
			write: func(dir string, authorized fs.FS) error {
				_, _, err := WriteCheckpointFilesWithOptions(
					dir,
					checkpointWriterVersion,
					"squash",
					"CREATE TABLE users (id integer);\n",
					"DROP TABLE users;\n",
					CheckpointWriteOptions{AuthorizedMigrationsFS: authorized},
				)
				return err
			},
			checkpoint: func(dir string) ([]string, error) {
				return filepath.Glob(filepath.Join(dir, "*.checkpoint.*.sql"))
			},
			sumName: "ptah.sum",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			for _, name := range test.priorNames {
				c.Assert(os.WriteFile(filepath.Join(dir, name), []byte("SELECT 1;\n"), 0o600), qt.IsNil)
			}
			authorized, err := migrationsnapshot.CaptureDirectory(dir)
			c.Assert(err, qt.IsNil)
			prior := filepath.Join(dir, test.priorNames[0])
			afterMigrationFileNamesChosen = func([]string) {
				afterMigrationFileNamesChosen = nil
				c.Assert(os.WriteFile(prior, []byte("SELECT 2;\n"), 0o600), qt.IsNil)
			}
			defer func() { afterMigrationFileNamesChosen = nil }()

			err = test.write(dir, authorized)

			c.Assert(err, qt.ErrorIs, ErrMigrationDirectoryChanged)
			checkpoints, globErr := test.checkpoint(dir)
			c.Assert(globErr, qt.IsNil)
			c.Assert(checkpoints, qt.HasLen, 0)
			contents, readErr := os.ReadFile(prior)
			c.Assert(readErr, qt.IsNil)
			c.Assert(string(contents), qt.Equals, "SELECT 2;\n")
			_, statErr := os.Stat(filepath.Join(dir, test.sumName))
			c.Assert(os.IsNotExist(statErr), qt.IsTrue)
		})
	}
}

// TestCheckpointWritersReplacedDirectoryCannotRedirectTheWrite asserts both
// halves the acceptance criteria ask for, for each of the three writers: every
// artifact of the transaction lands in the directory the run bound, and the
// directory the replacement pointed at is left completely untouched.
//
// The second half is the one that separates a rooted writer from one that
// merely errored out somewhere along the way.
func TestCheckpointWritersReplacedDirectoryCannotRedirectTheWrite(t *testing.T) {
	tests := []struct {
		name string
		// write runs the exported writer against the selected directory.
		write func(dir string) error
		// wantRetained is every entry the transaction must leave in the object it
		// bound, sorted as os.ReadDir returns them.
		wantRetained []string
	}{
		{
			name: "atlas checkpoint",
			write: func(dir string) error {
				_, err := WriteAtlasCheckpointFile(dir, atlasCheckpointWriterVersion, "squash", "CREATE TABLE users (id integer);")
				return err
			},
			wantRetained: []string{"20990101000000_squash.sql", "atlas.sum"},
		},
		{
			name: "paired checkpoint",
			write: func(dir string) error {
				_, _, err := WriteCheckpointFiles(dir, checkpointWriterVersion, "squash", "CREATE TABLE users (id integer);\n", "DROP TABLE users;\n")
				return err
			},
			wantRetained: []string{
				"2099010100_squash.checkpoint.down.sql",
				"2099010100_squash.checkpoint.up.sql",
				"ptah.sum",
			},
		},
		{
			name: "data migration",
			write: func(dir string) error {
				_, _, err := WriteDataMigrationFiles(dir, checkpointWriterVersion, "seed", "INSERT INTO users VALUES (1);\n", "DELETE FROM users;\n")
				return err
			},
			wantRetained: []string{
				"2099010100_seed.down.sql",
				"2099010100_seed.up.sql",
				"ptah.sum",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := t.TempDir()
			decoy := t.TempDir()
			selected := filepath.Join(root, "migrations")
			c.Assert(os.MkdirAll(selected, 0o755), qt.IsNil)

			// The hostile step, staged in the window the binding defends: after the
			// directory is bound and before the transaction writes through it.
			var retained string
			afterMigrationWriterBound = func() { retained = replaceWithSymlink(c.TB, selected, decoy) }
			defer func() { afterMigrationWriterBound = nil }()

			err := test.write(selected)

			c.Assert(err, qt.IsNil)
			c.Assert(generatorDirNames(c.TB, retained), qt.DeepEquals, test.wantRetained)
			c.Assert(generatorDirNames(c.TB, decoy), qt.HasLen, 0)
		})
	}
}

// TestCheckpointWritersCreateAMissingDirectoryThroughTheBoundParent covers the
// initially-missing directory the acceptance criteria name, for the two verbs
// that were still resolving it by pathname.
//
// The directory is materialized through the parent handle bound for the
// transaction, so replacing the parent's name afterwards cannot move where it is
// created. Before the change the mkdir, the file creates and the checksum commit
// each resolved the pathname separately, so this row's swap sent all three into
// the decoy.
func TestCheckpointWritersCreateAMissingDirectoryThroughTheBoundParent(t *testing.T) {
	tests := []struct {
		name         string
		write        func(dir string) error
		wantRetained []string
	}{
		{
			name: "atlas checkpoint",
			write: func(dir string) error {
				_, err := WriteAtlasCheckpointFile(dir, atlasCheckpointWriterVersion, "squash", "CREATE TABLE users (id integer);")
				return err
			},
			wantRetained: []string{"20990101000000_squash.sql", "atlas.sum"},
		},
		{
			name: "data migration",
			write: func(dir string) error {
				_, _, err := WriteDataMigrationFiles(dir, checkpointWriterVersion, "seed", "INSERT INTO users VALUES (1);\n", "DELETE FROM users;\n")
				return err
			},
			wantRetained: []string{
				"2099010100_seed.down.sql",
				"2099010100_seed.up.sql",
				"ptah.sum",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := t.TempDir()
			decoy := t.TempDir()
			c.Assert(os.MkdirAll(filepath.Join(root, "nest"), 0o755), qt.IsNil)
			selected := filepath.Join(root, "nest", "migrations")

			var retained string
			afterMigrationWriterBound = func() {
				retained = replaceWithSymlink(c.TB, filepath.Join(root, "nest"), decoy)
			}
			defer func() { afterMigrationWriterBound = nil }()

			err := test.write(selected)

			c.Assert(err, qt.IsNil)
			c.Assert(
				generatorDirNames(c.TB, filepath.Join(retained, "migrations")),
				qt.DeepEquals, test.wantRetained,
			)
			c.Assert(generatorDirNames(c.TB, decoy), qt.HasLen, 0)
		})
	}
}

// TestMigrationWritersRefuseADestinationTakenAfterTheNameWasChosen is the
// final-migration-filename commit point the acceptance criteria name, measured
// inside the window rather than before it.
//
// The distinction matters. A fixture that pre-places a file at the final name
// and then calls the writer proves only that the writer looked before it wrote;
// the binding answers that case at open time. What has to hold is that the
// create itself is the conditional commit -- that a file appearing between the
// moment the name was chosen and the moment it is created is reported and never
// overwritten. That is what the hook stages here, and the surviving bytes are
// the assertion: the writer's own body would be a different string.
//
// The three writers answer differently on purpose, and each answer is the
// correct one for its verb:
//
//   - the skeleton writer advances to the next free version and succeeds, because
//     `ptah migrations create` chooses the version itself and a taken one is not
//     a failure;
//   - both explicit-version writers refuse, because the caller resolved the
//     version and a collision means that resolution is no longer true.
func TestMigrationWritersRefuseADestinationTakenAfterTheNameWasChosen(t *testing.T) {
	const intruder = "-- written by someone else\n"

	tests := []struct {
		name string
		// occupy is the name the intruder takes in the bound directory, of the
		// names the writer just chose.
		occupy func(names []string) string
		// write runs the exported writer against the selected directory.
		write func(dir string) error
		// check asserts what the writer's one attempt returned.
		check func(c *qt.C, err error)
		// wantDir is the directory afterwards, including the intruder's file.
		wantDir []string
	}{
		{
			name:   "atlas checkpoint refuses",
			occupy: func(names []string) string { return names[0] },
			write: func(dir string) error {
				_, err := WriteAtlasCheckpointFile(dir, atlasCheckpointWriterVersion, "squash", "CREATE TABLE users (id integer);")
				return err
			},
			check: func(c *qt.C, err error) {
				c.Assert(err, qt.ErrorMatches, `.*checkpoint file .*20990101000000_squash\.sql already exists`)
			},
			wantDir: []string{"20990101000000_squash.sql"},
		},
		{
			name:   "paired checkpoint refuses when the up half is taken",
			occupy: func(names []string) string { return names[0] },
			write: func(dir string) error {
				_, _, err := WriteCheckpointFiles(dir, checkpointWriterVersion, "squash", "CREATE TABLE users (id integer);\n", "DROP TABLE users;\n")
				return err
			},
			check: func(c *qt.C, err error) {
				c.Assert(err, qt.ErrorMatches, `checkpoint files for version 2099010100 already exist`)
			},
			wantDir: []string{"2099010100_squash.checkpoint.up.sql"},
		},
		{
			// The down half is the interesting one: the up half is created first
			// and has to be withdrawn again, or the directory is left holding half
			// a migration that ptah.sum does not cover.
			name:   "data migration withdraws the up half when the down half is taken",
			occupy: func(names []string) string { return names[1] },
			write: func(dir string) error {
				_, _, err := WriteDataMigrationFiles(dir, checkpointWriterVersion, "seed", "INSERT INTO users VALUES (1);\n", "DELETE FROM users;\n")
				return err
			},
			check: func(c *qt.C, err error) {
				c.Assert(err, qt.ErrorMatches, `migration files for version 2099010100 already exist`)
			},
			wantDir: []string{"2099010100_seed.down.sql"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			selected := filepath.Join(t.TempDir(), "migrations")
			c.Assert(os.MkdirAll(selected, 0o755), qt.IsNil)

			var occupied string
			afterMigrationFileNamesChosen = func(names []string) {
				occupied = test.occupy(names)
				c.Assert(os.WriteFile(
					filepath.Join(selected, occupied), []byte(intruder), 0o600,
				), qt.IsNil)
			}
			defer func() { afterMigrationFileNamesChosen = nil }()

			err := test.write(selected)

			test.check(c, err)
			c.Assert(generatorDirNames(c.TB, selected), qt.DeepEquals, test.wantDir)
			// The protected state, not the message: the intruder's bytes survive.
			body, readErr := os.ReadFile(filepath.Join(selected, occupied))
			c.Assert(readErr, qt.IsNil)
			c.Assert(string(body), qt.Equals, intruder)
		})
	}
}

// TestGenerateEmptyMigrationStepsPastADestinationTakenAfterTheNameWasChosen is
// the skeleton writer's answer at the same commit point, and it is the opposite
// answer to the row above on purpose.
//
// `ptah migrations create` chooses its own version, so a name taken inside the
// window is not a failure -- it is a collision to step past. What must not
// happen either way is the overwrite, so the assertion is the same shape: the
// intruder's bytes survive, and the migration this run reports is a different
// file. Without a conditional create the intruder would simply be replaced and
// the run would report the version it first chose.
//
// The hook clears itself so only the first attempt is intercepted; a hook that
// occupied every candidate would exhaust the retry rather than measure it.
func TestGenerateEmptyMigrationStepsPastADestinationTakenAfterTheNameWasChosen(t *testing.T) {
	const intruder = "-- written by someone else\n"
	c := qt.New(t)
	selected := filepath.Join(t.TempDir(), "migrations")
	c.Assert(os.MkdirAll(selected, 0o755), qt.IsNil)
	atlasVersionClock = func() time.Time {
		return time.Date(2999, time.January, 1, 0, 0, 1, 0, time.UTC)
	}
	defer func() { atlasVersionClock = func() time.Time { return time.Now().UTC() } }()

	var occupied string
	afterMigrationFileNamesChosen = func(names []string) {
		afterMigrationFileNamesChosen = nil
		occupied = names[0]
		c.Assert(os.WriteFile(
			filepath.Join(selected, occupied), []byte(intruder), 0o600,
		), qt.IsNil)
	}
	defer func() { afterMigrationFileNamesChosen = nil }()

	files, err := GenerateEmptyMigration(EmptyMigrationOptions{
		MigrationName: "added",
		OutputDir:     selected,
		DirFormat:     migrator.MigrationDirFormatAtlas,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(occupied, qt.Equals, atlasEmptyMigrationFileName(29990101000001, "added"))
	c.Assert(files.Files, qt.HasLen, 1)
	c.Assert(files.Files[0].Version, qt.Equals, int64(29990101000002))
	c.Assert(generatorDirNames(c.TB, selected), qt.DeepEquals, []string{
		atlasEmptyMigrationFileName(29990101000001, "added"),
		atlasEmptyMigrationFileName(29990101000002, "added"),
		"atlas.sum",
	})
	body, readErr := os.ReadFile(filepath.Join(selected, occupied))
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(body), qt.Equals, intruder)
}
