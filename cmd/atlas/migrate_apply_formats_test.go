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
			hashConvertedApplyDir(c, migrationsDir, tt.format)
			dbPath := filepath.Join(dir, "apply.db")

			cmd := atlas.NewCompatCommand("atlas")
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
	hashConvertedApplyDir(c, migrationsDir, "goose")
	dbPath := filepath.Join(dir, "apply.db")

	cmd := atlas.NewCompatCommand("atlas")
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
	hashConvertedApplyDir(c, migrationsDir, "flyway")
	dbPath := filepath.Join(dir, "apply.db")

	cmd := atlas.NewCompatCommand("atlas")
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
	hashConvertedApplyDir(c, migrationsDir, "dbmate")
	dbPath := filepath.Join(dir, "apply.db")

	cmd := atlas.NewCompatCommand("atlas")
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
	hashConvertedApplyDir(c, migrationsDir, "goose")
	dbPath := filepath.Join(dir, "apply.db")

	cmd := atlas.NewCompatCommand("atlas")
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
	// One version token, two files. Atlas CE panics on such a directory rather
	// than executing it, so refusing is the oracle's own answer; V1.5 beside
	// V1_5 is NOT this case, because those are two distinct version strings CE
	// runs in walk order (stokaro/ptah#982).
	writeAtlasApplyProjectMigration(c, migrationsDir, "V1__a.sql", "CREATE TABLE a (id INTEGER PRIMARY KEY);")
	writeAtlasApplyProjectMigration(c, migrationsDir, "V1__b.sql", "CREATE TABLE b (id INTEGER PRIMARY KEY);")
	// Hashed first: the conversion refusal below is reachable only once the
	// integrity gate has passed, because the gate now precedes the source-format
	// parse exactly as it does in Atlas CE (stokaro/ptah#973).
	hashConvertedApplyDir(c, migrationsDir, "flyway")
	dbPath := filepath.Join(dir, "collision.db")

	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir + "?format=flyway",
	})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `atlas migrate apply --dir: Flyway migrations V1__a\.sql and V1__b\.sql both carry the Atlas version "1"`)
	_, statErr := os.Stat(dbPath)
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}

func TestMigrateApplyRejectsDuplicateConvertedVersionBeforeOpeningDatabase(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyProjectMigration(c, migrationsDir, "1_init.sql", "-- +goose Up\nCREATE TABLE a (id INTEGER PRIMARY KEY);")
	writeAtlasApplyProjectMigration(c, migrationsDir, "01_init.sql", "-- +goose Up\nCREATE TABLE b (id INTEGER PRIMARY KEY);")
	hashConvertedApplyDir(c, migrationsDir, "goose")
	dbPath := filepath.Join(dir, "dup-version.db")

	cmd := atlas.NewCompatCommand("atlas")
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
			hashConvertedApplyDir(c, migrationsDir, tt.format)
			dbPath := filepath.Join(dir, "missing-directive.db")

			cmd := atlas.NewCompatCommand("atlas")
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

func TestMigrateApplyRejectsUnsupportedFormatFilesBeforeOpeningDatabase(t *testing.T) {
	tests := []struct {
		name    string
		format  string
		files   map[string]string
		wantErr string
	}{
		{
			name:   "Go-based Goose migration",
			format: "goose",
			files: map[string]string{
				"1_init.sql": "-- +goose Up\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n",
				"2_seed.go":  "package migrations\n",
			},
			wantErr: `atlas migrate apply --dir: Go-based Goose migration "2_seed\.go" is not supported \(SQL migrations only\)`,
		},
		{
			name:   "Liquibase XML changelog",
			format: "liquibase",
			files: map[string]string{
				"1_init.sql":    "--liquibase formatted sql\n--changeset ptah:1\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n",
				"changelog.xml": "<databaseChangeLog></databaseChangeLog>\n",
			},
			wantErr: `atlas migrate apply --dir: liquibase XML/YAML/JSON changelogs are not yet supported .* found changelog\.xml`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			migrationsDir := filepath.Join(dir, "migrations")
			for name, content := range test.files {
				writeAtlasApplyProjectMigration(c, migrationsDir, name, content)
			}
			hashConvertedApplyDir(c, migrationsDir, test.format)
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

			c.Assert(err, qt.ErrorMatches, test.wantErr)
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

	cmd := atlas.NewCompatCommand("atlas")
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

// TestMigrateApplyFlywayMidSequenceInsertionKeepsStableVersions is the
// regression guard for the position-based numbering bug: inserting a migration
// that sorts before an already-applied one must not renumber the others (which
// would make Atlas revision checksums point at different SQL and abort apply).
func TestMigrateApplyFlywayMidSequenceInsertionKeepsStableVersions(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	writeAtlasApplyProjectMigration(c, migrationsDir, "V1__base.sql", "CREATE TABLE t_v1 (id INTEGER PRIMARY KEY);")
	writeAtlasApplyProjectMigration(c, migrationsDir, "V1.5__minor.sql", "CREATE TABLE t_v15 (id INTEGER PRIMARY KEY);")
	writeAtlasApplyProjectMigration(c, migrationsDir, "V2__major.sql", "CREATE TABLE t_v2 (id INTEGER PRIMARY KEY);")
	hashConvertedApplyDir(c, migrationsDir, "flyway")
	dbPath := filepath.Join(dir, "apply.db")

	firstErr := runFlywayApply(migrationsDir, dbPath)

	c.Assert(firstErr, qt.IsNil)
	c.Assert(sqliteTableCount(c, dbPath, "t_v1"), qt.Equals, 1)
	c.Assert(sqliteTableCount(c, dbPath, "t_v15"), qt.Equals, 1)
	c.Assert(sqliteTableCount(c, dbPath, "t_v2"), qt.Equals, 1)
	before := sqliteAtlasRevisionVersions(c, dbPath)

	// Insert a migration that sorts in the middle (V1.6, between V1.5 and V2) and
	// re-apply.
	writeAtlasApplyProjectMigration(c, migrationsDir, "V1.6__hotfix.sql", "CREATE TABLE t_v16 (id INTEGER PRIMARY KEY);")
	// Adding a migration invalidates atlas.sum, so re-hash as a user would.
	hashConvertedApplyDir(c, migrationsDir, "flyway")
	secondErr := runFlywayApply(migrationsDir, dbPath)

	// No checksum mismatch: V2's recorded checksum still matches V2's SQL because
	// its Atlas version is unchanged. Re-running V1/V1.5/V2 would fail on "table
	// already exists", so a nil error also proves only V1.6 ran.
	c.Assert(secondErr, qt.IsNil)
	c.Assert(sqliteTableCount(c, dbPath, "t_v16"), qt.Equals, 1)
	after := sqliteAtlasRevisionVersions(c, dbPath)
	c.Assert(after, qt.HasLen, len(before)+1)
	// Every version recorded by the first apply is still present unchanged, so
	// the insertion renumbered nothing.
	for _, version := range before {
		c.Assert(after, qt.Contains, version,
			qt.Commentf("version %s was renumbered by the insertion; before=%v after=%v", version, before, after))
	}
}

func runFlywayApply(migrationsDir, dbPath string) error {
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "apply",
		"--url", "sqlite://" + dbPath,
		"--dir", "file://" + migrationsDir + "?format=flyway",
		// Out-of-order insertion is a normal Flyway workflow; non-linear applies a
		// pending migration whose version is below the current one.
		"--exec-order", "non-linear",
	})
	return cmd.Execute()
}

func sqliteAtlasRevisionVersions(c *qt.C, dbPath string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(context.Background(), "SELECT version FROM atlas_schema_revisions ORDER BY version")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	versions := make([]string, 0)
	for rows.Next() {
		var version string
		c.Assert(rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return versions
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
