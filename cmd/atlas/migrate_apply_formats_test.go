package atlas_test

import (
	"bytes"
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/atlas"
	"github.com/stokaro/ptah/dbschema"
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

func TestMigrateApplyFlywayMajorMinorVersionsExecuteInOrder(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	// V1.5 creates the table; V2 alters it. Flyway orders 1.5 < 2, so the CREATE
	// must run before the ALTER. Under the old digit-stripping parser V1.5 became
	// 15 (> 2), inverting the order and making the ALTER fail with "no such table".
	writeAtlasApplyProjectMigration(c, migrationsDir, "V1.5__create.sql", "CREATE TABLE widgets (id INTEGER PRIMARY KEY);")
	writeAtlasApplyProjectMigration(c, migrationsDir, "V2__extend.sql", "ALTER TABLE widgets ADD COLUMN label TEXT;")
	dbPath := filepath.Join(dir, "apply.db")

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir + "?format=flyway",
	})

	err := cmd.Execute()

	// A successful apply proves the ALTER (V2) ran after the CREATE (V1.5).
	c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", out.String()))
	c.Assert(sqliteTableCount(c, dbPath, "widgets"), qt.Equals, 1)
}

func TestMigrateApplyDBMateTransactionDirectiveOptionUpOnly(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	// "-- migrate:up transaction:false" must not leak "transaction:false" into the
	// executable SQL, and only the up section must run.
	writeAtlasApplyProjectMigration(c, migrationsDir, "1_init.sql",
		"-- migrate:up transaction:false\nCREATE TABLE up_ran (id INTEGER PRIMARY KEY);\n-- migrate:down\nCREATE TABLE down_ran (id INTEGER PRIMARY KEY);")
	dbPath := filepath.Join(dir, "apply.db")

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir + "?format=dbmate",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", out.String()))
	c.Assert(sqliteTableCount(c, dbPath, "up_ran"), qt.Equals, 1)
	c.Assert(sqliteTableCount(c, dbPath, "down_ran"), qt.Equals, 0)
}

func TestMigrateApplyGooseStatementBlockExecutesUpOnly(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	// The StatementBegin/End block contains an internal semicolon; its whole body
	// must execute and the down section must not run.
	writeAtlasApplyProjectMigration(c, migrationsDir, "1_init.sql",
		"-- +goose Up\n-- +goose StatementBegin\nCREATE TABLE up_ran (id INTEGER PRIMARY KEY);\nINSERT INTO up_ran (id) VALUES (1);\n-- +goose StatementEnd\n-- +goose Down\nCREATE TABLE down_ran (id INTEGER PRIMARY KEY);")
	dbPath := filepath.Join(dir, "apply.db")

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir + "?format=goose",
	})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", out.String()))
	c.Assert(sqliteTableCount(c, dbPath, "up_ran"), qt.Equals, 1)
	c.Assert(sqliteRowCount(c, dbPath, "up_ran"), qt.Equals, 1)
	c.Assert(sqliteTableCount(c, dbPath, "down_ran"), qt.Equals, 0)
}

func TestMigrateApplyRejectsFlywayVersionCollisionBeforeOpeningDatabase(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyProjectMigration(c, migrationsDir, "V1.5__a.sql", "CREATE TABLE a (id INTEGER PRIMARY KEY);")
	writeAtlasApplyProjectMigration(c, migrationsDir, "V1_5__b.sql", "CREATE TABLE b (id INTEGER PRIMARY KEY);")
	dbPath := filepath.Join(dir, "collision.db")

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir + "?format=flyway",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `atlas migrate apply --dir: Flyway migrations .* resolve to the same version 1[._]5`)
	_, statErr := os.Stat(dbPath)
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}

func TestMigrateApplyRejectsDuplicateConvertedVersionBeforeOpeningDatabase(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyProjectMigration(c, migrationsDir, "1_init.sql", "-- +goose Up\nCREATE TABLE a (id INTEGER PRIMARY KEY);")
	writeAtlasApplyProjectMigration(c, migrationsDir, "01_init.sql", "-- +goose Up\nCREATE TABLE b (id INTEGER PRIMARY KEY);")
	dbPath := filepath.Join(dir, "dup-version.db")

	cmd := atlas.NewAtlasCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir + "?format=goose",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `atlas migrate apply --dir: migration files .* map to the same version 1`)
	_, statErr := os.Stat(dbPath)
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}

func TestMigrateApplyRejectsMissingUpDirectiveBeforeOpeningDatabase(t *testing.T) {
	tests := []struct {
		name    string
		format  string
		content string
	}{
		{
			name:    "goose",
			format:  "goose",
			content: "CREATE TABLE never_created (id INTEGER PRIMARY KEY);\nDROP TABLE never_created;",
		},
		{
			name:    "dbmate",
			format:  "dbmate",
			content: "CREATE TABLE never_created (id INTEGER PRIMARY KEY);",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			migrationsDir := filepath.Join(dir, "migrations")
			writeAtlasApplyProjectMigration(c, migrationsDir, "1_init.sql", tt.content)
			dbPath := filepath.Join(dir, "missing-directive.db")

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

			c.Assert(err, qt.ErrorMatches, `atlas migrate apply --dir: migration file 1_init\.sql has no ".*" section`)
			_, statErr := os.Stat(dbPath)
			c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
		})
	}
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

func sqliteRowCount(c *qt.C, dbPath, table string) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	c.Assert(conn.QueryRowContext(context.Background(), "SELECT count(*) FROM "+table).Scan(&count), qt.IsNil)
	return count
}
