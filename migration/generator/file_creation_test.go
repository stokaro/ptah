package generator

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/migrator"
)

func TestCreateMigrationFilesSkipsVersionWhenEitherDirectionExists(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	version := int64(42)
	name := "constraint_drift"

	oldUp := filepath.Join(dir, migrator.GenerateMigrationFileName(version, name, "up"))
	oldDown := filepath.Join(dir, migrator.GenerateMigrationFileName(version, name, "down"))
	c.Assert(os.WriteFile(oldUp, nil, 0600), qt.IsNil)
	c.Assert(os.WriteFile(oldDown, []byte("SELECT old_down;\n"), 0600), qt.IsNil)

	files, err := createMigrationFiles(dir, version, name, "SELECT up;\n", "SELECT down;\n")
	c.Assert(err, qt.IsNil)
	c.Assert(files.Version, qt.Equals, version+1)

	oldDownBytes, err := os.ReadFile(oldDown)
	c.Assert(err, qt.IsNil)
	c.Assert(string(oldDownBytes), qt.Equals, "SELECT old_down;\n")

	upBytes, err := os.ReadFile(files.UpFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(upBytes), qt.Equals, "SELECT up;\n")
	downBytes, err := os.ReadFile(files.DownFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(downBytes), qt.Equals, "SELECT down;\n")
}

// TestCreateMigrationFilesCreatesMissingParents replaces
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
func TestCreateMigrationFilesCreatesMissingParents(t *testing.T) {
	c := qt.New(t)
	dir := filepath.Join(t.TempDir(), "missing", "levels", "migrations")

	files, err := createMigrationFiles(dir, 1, "init", "SELECT 1;\n", "SELECT 2;\n")
	c.Assert(err, qt.IsNil)
	c.Assert(filepath.Dir(files.UpFile), qt.Equals, dir)

	upBytes, err := os.ReadFile(files.UpFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(upBytes), qt.Equals, "SELECT 1;\n")
}

// TestCreateMigrationFilesRefusesParentThatIsAFile is the control for the test
// above: creating parents must not mean creating them through something that is
// not a directory. The pinned binary refuses that path too
// (`stat a/b: not a directory`, exit 1), so both stay refusals.
func TestCreateMigrationFilesRefusesParentThatIsAFile(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	blocker := filepath.Join(root, "a")
	c.Assert(os.WriteFile(blocker, []byte("not a directory\n"), 0600), qt.IsNil)

	_, err := createMigrationFiles(filepath.Join(blocker, "b"), 1, "init", "SELECT 1;\n", "SELECT 2;\n")
	c.Assert(err, qt.ErrorMatches, `failed to create output directory: .*not a directory`)

	blockerBytes, err := os.ReadFile(blocker)
	c.Assert(err, qt.IsNil)
	c.Assert(string(blockerBytes), qt.Equals, "not a directory\n")
}

func TestGenerateEmptyMigrationCreatesSkeletonPair(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	files, err := GenerateEmptyMigration(EmptyMigrationOptions{
		MigrationName: "Add User Preferences",
		OutputDir:     dir,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(files.Version, qt.Not(qt.Equals), int64(0))
	c.Assert(filepath.Base(files.UpFile), qt.Matches, `[0-9]+_add_user_preferences\.up\.sql`)
	c.Assert(filepath.Base(files.DownFile), qt.Matches, `[0-9]+_add_user_preferences\.down\.sql`)

	upBytes, err := os.ReadFile(files.UpFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(upBytes), qt.Contains, "-- Migration: Add User Preferences\n")
	c.Assert(string(upBytes), qt.Contains, "-- Direction: UP\n")
	c.Assert(string(upBytes), qt.Contains, "-- Add your migration SQL here.\n")

	downBytes, err := os.ReadFile(files.DownFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(downBytes), qt.Contains, "-- Migration: Add User Preferences\n")
	c.Assert(string(downBytes), qt.Contains, "-- Direction: DOWN\n")
	c.Assert(string(downBytes), qt.Contains, "-- Add your migration SQL here.\n")
}

func TestGenerateEmptyMigrationSkipsExistingVersion(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()

	first, err := GenerateEmptyMigration(EmptyMigrationOptions{
		MigrationName: "same second",
		OutputDir:     dir,
	})
	c.Assert(err, qt.IsNil)

	second, err := GenerateEmptyMigration(EmptyMigrationOptions{
		MigrationName: "same second",
		OutputDir:     dir,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(second.Version > first.Version, qt.IsTrue)
	c.Assert(second.UpFile, qt.Not(qt.Equals), first.UpFile)
	c.Assert(second.DownFile, qt.Not(qt.Equals), first.DownFile)
}

func TestGenerateEmptyMigrationValidation(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	outside := filepath.Join(root, "..", "outside")

	_, err := GenerateEmptyMigration(EmptyMigrationOptions{
		MigrationName: "",
		OutputDir:     root,
	})
	c.Assert(err, qt.ErrorMatches, `migration name is required`)

	_, err = GenerateEmptyMigration(EmptyMigrationOptions{
		MigrationName: "init",
		OutputDir:     "",
	})
	c.Assert(err, qt.ErrorMatches, `output directory is required`)

	_, err = GenerateEmptyMigration(EmptyMigrationOptions{
		MigrationName: "!!!",
		OutputDir:     root,
	})
	c.Assert(err, qt.ErrorMatches, `migration name must contain letters, digits, or underscores`)

	_, err = GenerateEmptyMigration(EmptyMigrationOptions{
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

	_, err := GenerateMigration(context.Background(), GenerateMigrationOptions{
		GoEntitiesDir:     root,
		OutputDir:         outside,
		AllowedOutputRoot: root,
	})
	c.Assert(err, qt.ErrorMatches, `error validating output directory: .*outside allowed root.*`)
}
