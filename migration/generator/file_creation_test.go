package generator_test

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/generator"
)

// TestGenerateEmptyMigrationNeverOverwritesAnExistingHalf is the file-level
// counterpart: the writer's creates are exclusive, so an existing file keeps its
// contents whatever the version scan concluded.
func TestGenerateEmptyMigrationNeverOverwritesAnExistingHalf(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	first, err := generator.GenerateEmptyMigration(generator.EmptyMigrationOptions{
		MigrationName: "constraint drift",
		OutputDir:     dir,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(first.Files, qt.HasLen, 1)
	c.Assert(os.WriteFile(first.Files[0].DownFile, []byte("SELECT old_down;\n"), 0600), qt.IsNil)

	second, err := generator.GenerateEmptyMigration(generator.EmptyMigrationOptions{
		MigrationName: "constraint drift",
		OutputDir:     dir,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(second.Files, qt.HasLen, 1)
	c.Assert(second.Files[0].DownFile, qt.Not(qt.Equals), first.Files[0].DownFile)

	oldDownBytes, err := os.ReadFile(first.Files[0].DownFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(oldDownBytes), qt.Equals, "SELECT old_down;\n")
}

// TestGenerateEmptyMigrationCreatesMissingParents replaces
// TestCreateMigrationFilesRequiresExistingParent, which pinned the opposite
// behavior.
//
// This changes behavior; pre-v1, so no compatibility with Ptah's own past is
// owed (AGENTS.md). The requirement came from parity: measured on the pinned
// Atlas community binary v1.3.0, `migrate new addcol --dir file://a/b` in an
// empty directory creates a, a/b, the migration and atlas.sum and exits 0,
// where this refused with `parent directory "…/a" is not available` and wrote
// nothing (stokaro/ptah#1241 item 4). Ptah's `migrate diff` writer already
// created parents, so the two writers also disagreed with each other.
func TestGenerateEmptyMigrationCreatesMissingParents(t *testing.T) {
	c := qt.New(t)
	// The reported path is the one the output directory resolved to, so the
	// expectation resolves the temporary root the same way rather than comparing
	// against a path that is a symlink on this platform.
	base, err := filepath.EvalSymlinks(t.TempDir())
	c.Assert(err, qt.IsNil)
	dir := filepath.Join(base, "missing", "levels", "migrations")

	files, err := generator.GenerateEmptyMigration(generator.EmptyMigrationOptions{
		MigrationName: "init",
		OutputDir:     dir,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(files.Files, qt.HasLen, 1)
	c.Assert(filepath.Dir(files.Files[0].UpFile), qt.Equals, dir)

	upBytes, err := os.ReadFile(files.Files[0].UpFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(upBytes), qt.Contains, "-- Direction: UP\n")
}

// TestGenerateEmptyMigrationRefusesParentThatIsAFile is the control for the test
// above: creating parents must not mean creating them through something that is
// not a directory. The pinned binary refuses that path too
// (`stat a/b: not a directory`, exit 1), so both stay refusals.
func TestGenerateEmptyMigrationRefusesParentThatIsAFile(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	blocker := filepath.Join(root, "a")
	c.Assert(os.WriteFile(blocker, []byte("not a directory\n"), 0600), qt.IsNil)

	_, err := generator.GenerateEmptyMigration(generator.EmptyMigrationOptions{
		MigrationName: "init",
		OutputDir:     filepath.Join(blocker, "b"),
	})
	// The sentinel, not its wording: "not a directory" is the Unix rendering of
	// ENOTDIR, and Windows answers the same condition in its own words.
	c.Assert(err, qt.ErrorIs, syscall.ENOTDIR)

	blockerBytes, err := os.ReadFile(blocker)
	c.Assert(err, qt.IsNil)
	c.Assert(string(blockerBytes), qt.Equals, "not a directory\n")
}

func TestGenerateEmptyMigrationCreatesSkeletonPair(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	files, err := generator.GenerateEmptyMigration(generator.EmptyMigrationOptions{
		MigrationName: "Add User Preferences",
		OutputDir:     dir,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(files.Files, qt.HasLen, 1)
	pair := files.Files[0]
	c.Assert(pair.Version, qt.Not(qt.Equals), int64(0))
	c.Assert(filepath.Base(pair.UpFile), qt.Matches, `[0-9]+_add_user_preferences\.up\.sql`)
	c.Assert(filepath.Base(pair.DownFile), qt.Matches, `[0-9]+_add_user_preferences\.down\.sql`)

	upBytes, err := os.ReadFile(pair.UpFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(upBytes), qt.Contains, "-- Migration: Add User Preferences\n")
	c.Assert(string(upBytes), qt.Contains, "-- Direction: UP\n")
	c.Assert(string(upBytes), qt.Contains, "-- Add your migration SQL here.\n")

	downBytes, err := os.ReadFile(pair.DownFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(downBytes), qt.Contains, "-- Migration: Add User Preferences\n")
	c.Assert(string(downBytes), qt.Contains, "-- Direction: DOWN\n")
	c.Assert(string(downBytes), qt.Contains, "-- Add your migration SQL here.\n")
}

func TestGenerateEmptyMigrationSkipsExistingVersion(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	first, err := generator.GenerateEmptyMigration(generator.EmptyMigrationOptions{
		MigrationName: "same second",
		OutputDir:     dir,
	})
	c.Assert(err, qt.IsNil)

	second, err := generator.GenerateEmptyMigration(generator.EmptyMigrationOptions{
		MigrationName: "same second",
		OutputDir:     dir,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(second.Files, qt.HasLen, 1)
	c.Assert(first.Files, qt.HasLen, 1)
	c.Assert(second.Files[0].Version > first.Files[0].Version, qt.IsTrue)
	c.Assert(second.Files[0].UpFile, qt.Not(qt.Equals), first.Files[0].UpFile)
	c.Assert(second.Files[0].DownFile, qt.Not(qt.Equals), first.Files[0].DownFile)
}

func TestGenerateEmptyMigrationValidation(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	outside := filepath.Join(root, "..", "outside")

	_, err := generator.GenerateEmptyMigration(generator.EmptyMigrationOptions{
		MigrationName: "",
		OutputDir:     root,
	})
	c.Assert(err, qt.ErrorMatches, `migration name is required`)

	_, err = generator.GenerateEmptyMigration(generator.EmptyMigrationOptions{
		MigrationName: "init",
		OutputDir:     "",
	})
	c.Assert(err, qt.ErrorMatches, `output directory is required`)

	_, err = generator.GenerateEmptyMigration(generator.EmptyMigrationOptions{
		MigrationName: "!!!",
		OutputDir:     root,
	})
	c.Assert(err, qt.ErrorMatches, `migration name must contain letters, digits, or underscores`)

	_, err = generator.GenerateEmptyMigration(generator.EmptyMigrationOptions{
		MigrationName:     "init",
		OutputDir:         outside,
		AllowedOutputRoot: root,
	})
	c.Assert(err, qt.ErrorMatches, `error validating output directory: .*outside allowed root.*`)
}

func TestGenerateMigrationRejectsOutputOutsideAllowedRoot(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	outside := filepath.Join(root, "..", "outside")

	_, err := generator.GenerateMigration(context.Background(), generator.GenerateMigrationOptions{
		GoEntitiesDir:     root,
		OutputDir:         outside,
		AllowedOutputRoot: root,
	})
	c.Assert(err, qt.ErrorMatches, `error validating output directory: .*outside allowed root.*`)
}
