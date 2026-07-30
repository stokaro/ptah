//go:build !windows

package atlas_test

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/atlas"
)

func TestCompatCommandMigrateDownRejectsProjectDirectorySymlinkEscapeBeforeOpeningDatabase(t *testing.T) {
	c := qt.New(t)
	projectDir := t.TempDir()
	outsideDir := t.TempDir()
	c.Assert(
		os.WriteFile(filepath.Join(outsideDir, "1_init.sql"), []byte("SELECT 'outside';\n"), 0o600),
		qt.IsNil,
	)
	c.Assert(os.Symlink(outsideDir, filepath.Join(projectDir, "migrations")), qt.IsNil)
	dbPath := filepath.Join(projectDir, "must-not-exist.db")
	c.Assert(os.WriteFile(filepath.Join(projectDir, "atlas.hcl"), []byte(`env "local" {
  url = "sqlite://`+dbPath+`"
  migration {
    dir = "file://migrations"
  }
}
`), 0o600), qt.IsNil)

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "down",
		"--config", "file://" + filepath.ToSlash(filepath.Join(projectDir, "atlas.hcl")),
		"--env", "local",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `.*invalid migrations directory: .*outside allowed root.*`)
	_, statErr := os.Stat(dbPath)
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}

func TestMigrateApplyRejectsSymlinkEscapeBeforeOpeningDatabase(t *testing.T) {
	tests := []struct {
		name     string
		format   string
		filename string
	}{
		{name: "atlas", format: "atlas", filename: "1_init.sql"},
		{name: "golang-migrate", format: "golang-migrate", filename: "1_init.up.sql"},
		{name: "goose", format: "goose", filename: "1_init.sql"},
		{name: "flyway", format: "flyway", filename: "V1__init.sql"},
		{name: "liquibase", format: "liquibase", filename: "1_init.sql"},
		{name: "dbmate", format: "dbmate", filename: "1_init.sql"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			migrationsDir := filepath.Join(dir, "migrations")
			c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
			outsideMigration := filepath.Join(dir, "outside.sql")
			c.Assert(
				os.WriteFile(outsideMigration, []byte("CREATE TABLE escaped (id INTEGER PRIMARY KEY);"), 0o600),
				qt.IsNil,
			)
			c.Assert(
				os.Symlink(outsideMigration, filepath.Join(migrationsDir, test.filename)),
				qt.IsNil,
			)
			dbPath := filepath.Join(dir, "must-not-exist.db")

			cmd := atlas.NewCompatCommand("atlas")
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs([]string{
				"migrate", "apply",
				"--url", "sqlite://" + dbPath,
				"--dir", "file://" + migrationsDir + "?format=" + test.format,
			})

			err := cmd.Execute()

			c.Assert(err, qt.ErrorMatches, `(?s)atlas migrate apply --dir: capture migrations directory:.*`)
			_, statErr := os.Stat(dbPath)
			c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
		})
	}
}

func TestMigrateApplyRejectsProjectDirectorySymlinkEscapeBeforeOpeningDatabase(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	t.Chdir(root)
	outside := t.TempDir()
	writeAtlasApplyProjectMigration(c, outside, "1_create_widgets.sql", "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n")
	c.Assert(os.Symlink(outside, "migrations"), qt.IsNil)
	dbPath := filepath.Join(root, "apply.db")
	writeAtlasApplyProjectConfig(c, dbPath, "atlas", "LINEAR")

	output, err := executeAtlasProjectCommand("migrate", "apply", "--env", "local")

	c.Assert(
		err,
		qt.ErrorMatches,
		`atlas migrate apply --dir: invalid migrations directory: .*outside allowed root.*`,
		qt.Commentf("command output:\n%s", output),
	)
	_, statErr := os.Stat(dbPath)
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}
