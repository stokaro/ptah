package atlas_test

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/atlascompat"
	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestMigrateApplyWithAtlasProjectLinearSkipSemantics(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	writeAtlasApplyProjectMigration(c.TB, "migrations", "1_create_v1.sql", "CREATE TABLE widgets_v1 (id INTEGER PRIMARY KEY);\n")
	writeAtlasApplyProjectMigration(c.TB, "migrations", "2_create_v2.sql", "CREATE TABLE widgets_v2 (id INTEGER PRIMARY KEY);\n")
	writeAtlasApplyProjectMigration(c.TB, "migrations", "3_create_v3.sql", "CREATE TABLE widgets_v3 (id INTEGER PRIMARY KEY);\n")
	writeAtlasApplyProjectSum(c.TB, "migrations")
	dbPath := filepath.Join(root, "apply.db")
	writeAtlasApplyProjectConfig(c.TB, dbPath, "atlas", "LINEAR_SKIP")

	output, err := executeAtlasProjectCommand("migrate", "set", "2", "--env", "local")
	c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", output))

	output, err = executeAtlasProjectCommand("migrate", "apply", "--env", "local")

	c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", output))
	c.Assert(sqliteTableCount(c.TB, dbPath, "widgets_v1"), qt.Equals, 0)
	c.Assert(sqliteTableCount(c.TB, dbPath, "widgets_v2"), qt.Equals, 0)
	c.Assert(sqliteTableCount(c.TB, dbPath, "widgets_v3"), qt.Equals, 1)
}

func TestMigrateApplyExecutesGooseProjectUpSectionOnly(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	writeAtlasApplyProjectMigration(c.TB, "migrations", "1_create_widgets.sql", `-- +goose Up
CREATE TABLE widgets (id INTEGER PRIMARY KEY);
-- +goose Down
DROP TABLE widgets;
`)
	hashConvertedApplyDir(c.TB, "migrations", "goose")
	dbPath := filepath.Join(root, "apply.db")
	writeAtlasApplyProjectConfig(c.TB, dbPath, "goose", "LINEAR")

	output, err := executeAtlasProjectCommand("migrate", "apply", "--env", "local")

	c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", output))
	// The Up section executed; the Down section (DROP TABLE widgets) did not,
	// so the table still exists.
	c.Assert(sqliteTableCount(c.TB, dbPath, "widgets"), qt.Equals, 1)
}

func TestMigrateApplyRejectsUnknownProjectFormatBeforeOpeningDatabase(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	writeAtlasApplyProjectMigration(c.TB, "migrations", "1_create_widgets.sql", "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n")
	dbPath := filepath.Join(root, "apply.db")
	writeAtlasApplyProjectConfig(c.TB, dbPath, "custom", "LINEAR")

	output, err := executeAtlasProjectCommand("migrate", "apply", "--env", "local")

	c.Assert(
		err,
		qt.ErrorMatches,
		// A format named by atlas.hcl is refused with the same community-binary
		// wording as one named on the command line; see
		// migrate_dir_format_error.go.
		`unknown dir format "custom"`,
		qt.Commentf("command output:\n%s", output),
	)
	_, statErr := os.Stat(dbPath)
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}

func TestMigrateApplyExecutesGooseFormatFromURL(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	writeAtlasApplyProjectMigration(c.TB, "migrations", "1_create_widgets.sql", "-- +goose Up\nCREATE TABLE widgets (id INTEGER PRIMARY KEY);\n-- +goose Down\nDROP TABLE widgets;\n")
	hashConvertedApplyDir(c.TB, "migrations", "goose")
	dbPath := filepath.Join(root, "apply.db")

	cmd := atlas.NewCompatCommand("atlas")
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://migrations?format=goose",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", output.String()))
	c.Assert(sqliteTableCount(c.TB, dbPath, "widgets"), qt.Equals, 1)
}

func TestMigrateApplyURLFormatOverridesProjectDefault(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	writeAtlasApplyProjectMigration(c.TB, "migrations", "1_create_widgets.sql", "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n")
	writeAtlasApplyProjectSum(c.TB, "migrations")
	dbPath := filepath.Join(root, "apply.db")
	writeAtlasApplyProjectConfig(c.TB, dbPath, "goose", "LINEAR")

	output, err := executeAtlasProjectCommand(
		"migrate", "apply",
		"--env", "local",
		"--dir", "file://migrations?format=atlas",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", output))
	c.Assert(sqliteTableCount(c.TB, dbPath, "widgets"), qt.Equals, 1)
}

func TestMigrateApplyProjectDirURLFormatOverridesProjectDefault(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	writeAtlasApplyProjectMigration(c.TB, "migrations", "1_create_widgets.sql", "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n")
	writeAtlasApplyProjectSum(c.TB, "migrations")
	dbPath := filepath.Join(root, "apply.db")
	writeAtlasApplyProjectConfigWithDir(c.TB, dbPath, "file://migrations?format=", "goose", "LINEAR")

	output, err := executeAtlasProjectCommand("migrate", "apply", "--env", "local")

	c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", output))
	c.Assert(sqliteTableCount(c.TB, dbPath, "widgets"), qt.Equals, 1)
}

func TestMigrateApplyProjectDirURLFormatOverridesExplicitEmptyProjectFormat(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	writeAtlasApplyProjectMigration(c.TB, "migrations", "1_create_widgets.sql", "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n")
	writeAtlasApplyProjectSum(c.TB, "migrations")
	dbPath := filepath.Join(root, "apply.db")
	writeAtlasApplyProjectConfigWithDir(c.TB, dbPath, "file://migrations?format=atlas", "", "LINEAR")

	output, err := executeAtlasProjectCommand("migrate", "apply", "--env", "local")

	c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", output))
	c.Assert(sqliteTableCount(c.TB, dbPath, "widgets"), qt.Equals, 1)
}

func TestMigrateApplyProjectDirURLFormatSelectsGoose(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	writeAtlasApplyProjectMigration(c.TB, "migrations", "1_create_widgets.sql", "-- +goose Up\nCREATE TABLE widgets (id INTEGER PRIMARY KEY);\n-- +goose Down\nDROP TABLE widgets;\n")
	hashConvertedApplyDir(c.TB, "migrations", "goose")
	dbPath := filepath.Join(root, "apply.db")
	writeAtlasApplyProjectConfigWithDir(c.TB, dbPath, "file://migrations?format=goose", "atlas", "LINEAR")

	output, err := executeAtlasProjectCommand("migrate", "apply", "--env", "local")

	c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", output))
	c.Assert(sqliteTableCount(c.TB, dbPath, "widgets"), qt.Equals, 1)
}

func writeAtlasApplyProjectConfig(tb testing.TB, dbPath, format, execOrder string) {
	c := qt.New(tb)
	c.Helper()
	writeAtlasApplyProjectConfigWithDir(c.TB, dbPath, "file://migrations", format, execOrder)
}

func writeAtlasApplyProjectConfigWithDir(tb testing.TB, dbPath, dir, format, execOrder string) {
	c := qt.New(tb)
	c.Helper()
	c.Assert(os.WriteFile("atlas.hcl", fmt.Appendf(nil, `env "local" {
  url = "sqlite://%s"
  migration {
    dir        = "%s"
    format     = "%s"
    exec_order = %s
    tx_mode    = "file"
  }
}
`, dbPath, dir, format, execOrder), 0o600), qt.IsNil)
}

func executeAtlasProjectCommand(args ...string) (string, error) {
	cmd := atlas.NewCompatCommand("atlas")
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return output.String(), err
}

func writeAtlasApplyProjectMigration(tb testing.TB, dir, name, sql string) {
	c := qt.New(tb)
	c.Helper()
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(sql), 0o600), qt.IsNil)
}

func writeAtlasApplyProjectSum(tb testing.TB, dir string) {
	c := qt.New(tb)
	c.Helper()
	sum, err := atlascompat.ComputeSum(os.DirFS(dir), migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, atlascompat.AtlasSumFileName), sum.Bytes(), 0o600), qt.IsNil)
}
