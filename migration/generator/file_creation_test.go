package generator

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/migrator"
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

func TestCreateMigrationFilesRequiresExistingParent(t *testing.T) {
	c := qt.New(t)
	dir := filepath.Join(t.TempDir(), "missing", "migrations")

	_, err := createMigrationFiles(dir, 1, "init", "SELECT 1;\n", "SELECT 2;\n")
	c.Assert(err, qt.ErrorMatches, `failed to create output directory: parent directory .* is not available: .*`)
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
