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

// TestMigrateApplyExecutesExternalFormatsUpOnly is the per-format black-box
// SQLite apply contract required by stokaro/ptah#742. Each source directory
// encodes its up section as CREATE TABLE up_ran and its down/rollback/undo
// section as CREATE TABLE down_ran. A correct format-aware apply executes only
// the up section, so up_ran must exist and down_ran must not.
func TestMigrateApplyExecutesExternalFormatsUpOnly(t *testing.T) {
	tests := []struct {
		name   string
		format string
		files  map[string]string
	}{
		{
			name:   "goose",
			format: "goose",
			files: map[string]string{
				"1_init.sql": "-- +goose Up\nCREATE TABLE up_ran (id INTEGER PRIMARY KEY);\n-- +goose Down\nCREATE TABLE down_ran (id INTEGER PRIMARY KEY);",
			},
		},
		{
			name:   "dbmate",
			format: "dbmate",
			files: map[string]string{
				"1_init.sql": "-- migrate:up\nCREATE TABLE up_ran (id INTEGER PRIMARY KEY);\n-- migrate:down\nCREATE TABLE down_ran (id INTEGER PRIMARY KEY);",
			},
		},
		{
			name:   "liquibase",
			format: "liquibase",
			files: map[string]string{
				"1_init.sql": "--liquibase formatted sql\n--changeset app:1\nCREATE TABLE up_ran (id INTEGER PRIMARY KEY);\n--rollback CREATE TABLE down_ran (id INTEGER PRIMARY KEY);",
			},
		},
		{
			name:   "golang-migrate",
			format: "golang-migrate",
			files: map[string]string{
				"1_init.up.sql":   "CREATE TABLE up_ran (id INTEGER PRIMARY KEY);",
				"1_init.down.sql": "CREATE TABLE down_ran (id INTEGER PRIMARY KEY);",
			},
		},
		{
			name:   "flyway",
			format: "flyway",
			files: map[string]string{
				"V1__init.sql": "CREATE TABLE up_ran (id INTEGER PRIMARY KEY);",
				"U1__init.sql": "CREATE TABLE down_ran (id INTEGER PRIMARY KEY);",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			migrationsDir := filepath.Join(dir, "migrations")
			for name, content := range tt.files {
				writeAtlasApplyProjectMigration(c, migrationsDir, name, content)
			}
			dbPath := filepath.Join(dir, "apply.db")

			cmd := atlas.NewAtlasCommand()
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			cmd.SetArgs([]string{
				"migrate", "apply",
				"--url", "sqlite://" + dbPath,
				"--dir", "file://" + migrationsDir + "?format=" + tt.format,
			})

			err := cmd.Execute()

			c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", out.String()))
			c.Assert(sqliteTableCount(c, dbPath, "up_ran"), qt.Equals, 1)
			c.Assert(sqliteTableCount(c, dbPath, "down_ran"), qt.Equals, 0)
		})
	}
}

func TestMigrateApplyFormatOutputRendersFromConvertedFilesystem(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyProjectMigration(c, migrationsDir, "1_init.sql",
		"-- +goose Up\nCREATE TABLE up_ran (id INTEGER PRIMARY KEY);\n-- +goose Down\nCREATE TABLE down_ran (id INTEGER PRIMARY KEY);")
	dbPath := filepath.Join(dir, "apply.db")

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir + "?format=goose",
		"--format", "{{ json . }}",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", out.String()))
	// The --format report renders applied statements from the converted,
	// up-only filesystem, so it reports the Goose up section and never the down
	// section it would see if it re-read the raw source directory.
	c.Assert(out.String(), qt.Contains, "CREATE TABLE up_ran")
	c.Assert(out.String(), qt.Not(qt.Contains), "down_ran")
}

func TestMigrateApplyRejectsUnknownURLFormatBeforeOpeningDatabase(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyProjectMigration(c, migrationsDir, "1_init.sql", "CREATE TABLE never_created (id INTEGER PRIMARY KEY);")
	dbPath := filepath.Join(dir, "unknown-url-format.db")

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir + "?format=custom",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `atlas migrate apply --dir: unknown Atlas migration directory format "custom": expected atlas, golang-migrate, goose, flyway, liquibase, or dbmate`)
	// The failure happens before the database is opened, so SQLite never creates
	// the database file.
	_, statErr := os.Stat(dbPath)
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}
