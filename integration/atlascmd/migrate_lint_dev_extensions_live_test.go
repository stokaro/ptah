//go:build integration

package atlas_test

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/migratesum"
	"ptah.run/migration/migrationfile"
)

// A dev database's extensions are its environment. The pinned community binary
// v1.3.0 lints a directory whose first migration uses a type from an extension
// the dev database already has, without creating it (measured on PostgreSQL
// 18, 2026-09-24). Removing the extension before the replay made that
// migration fail, and on TimescaleDB, created again by the replay in the same
// session, the extension answered `schema "_timescaledb_functions" does not
// exist` (stokaro/ptah#3542).

func writeLintDirectory(c *qt.C, files map[string]string) string {
	c.Helper()
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	_, err := migratesum.WriteWithFormat(dir, migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return dir
}

func runCompatLint(c *qt.C, dir, devURL string) (string, error) {
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "lint", "--dir", "file://" + dir, "--dev-url", devURL, "--latest", "1"})
	err := cmd.Execute()
	return out.String(), err
}

func installedExtensions(c *qt.C, dbURL string) []string {
	c.Helper()
	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	rows, err := db.QueryContext(context.Background(), "SELECT extname FROM pg_extension ORDER BY extname")
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var names []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}

func TestMigrateLintKeepsTheDevDatabaseExtensionsLivePostgres(t *testing.T) {
	tests := []struct {
		name  string
		setup []string
		files map[string]string
		// wantExtensions is what the dev database holds after the run.
		wantExtensions []string
	}{
		{
			name:  "a migration uses a preinstalled extension it does not create",
			setup: []string{"CREATE EXTENSION hstore"},
			files: map[string]string{
				"1_t.sql": "CREATE TABLE t (id integer PRIMARY KEY, attrs hstore);\n",
				"2_u.sql": "CREATE TABLE u (id integer PRIMARY KEY);\n",
			},
			wantExtensions: []string{"hstore", "plpgsql"},
		},
		{
			// The control: the baseline is what the database held, so an
			// extension the replay created is the replay's and goes.
			name: "an extension the replay creates",
			files: map[string]string{
				"1_t.sql": "CREATE EXTENSION IF NOT EXISTS pg_trgm;\nCREATE TABLE t (id integer PRIMARY KEY);\n",
				"2_u.sql": "CREATE TABLE u (id integer PRIMARY KEY);\n",
			},
			wantExtensions: []string{"plpgsql"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			devURL := newCleanGateDatabase(c, dbtarget.URL(c, dbtarget.PostgreSQL), test.setup)
			dir := writeLintDirectory(c, test.files)

			out, err := runCompatLint(c, dir, devURL)

			c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
			c.Assert(installedExtensions(c, devURL), qt.DeepEquals, test.wantExtensions)
		})
	}
}

// TestMigrateLintOnATimescaleDBDevDatabaseLive is the shape akashi's
// `make migrate-lint` meets: a TimescaleDB dev database that already has the
// extension, and a first migration that creates it again and a hypertable.
func TestMigrateLintOnATimescaleDBDevDatabaseLive(t *testing.T) {
	c := qt.New(t)
	devURL := newCleanGateDatabase(c, dbtarget.URL(c, dbtarget.TimescaleDB),
		[]string{"CREATE EXTENSION IF NOT EXISTS timescaledb"})
	dir := writeLintDirectory(c, map[string]string{
		"1_metrics.sql": "CREATE EXTENSION IF NOT EXISTS timescaledb;\n" +
			"CREATE TABLE metrics (at timestamptz NOT NULL, value integer);\n" +
			"SELECT create_hypertable('metrics', 'at');\n",
		"2_u.sql": "CREATE TABLE u (id integer PRIMARY KEY);\n",
	})

	out, err := runCompatLint(c, dir, devURL)

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", out))
	c.Assert(installedExtensions(c, devURL), qt.DeepEquals, []string{"plpgsql", "timescaledb"})
}
