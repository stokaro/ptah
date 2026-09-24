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

// The version column is where Atlas and Ptah meet. The pinned community binary
// v1.3.0 applied 001_a.sql, 002_b.sql and 010_c.sql to an empty PostgreSQL 18
// database and recorded 001, 002 and 010 (measured 2026-09-24). It compares
// versions as strings, so rows written as 1, 2 and 10 read 2 as the current
// version, and on a longer history such as 001...111 it refuses the next file
// as added out of order.

// writeSpelledPostgresDirectory writes a hashed Atlas directory whose versions
// carry leading zeros, one file per name.
func writeSpelledPostgresDirectory(c *qt.C, dir string, names ...string) {
	c.Helper()
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	bodies := map[string]string{
		"001_a.sql": "CREATE TABLE spelled_a (id integer PRIMARY KEY);\n",
		"002_b.sql": "CREATE TABLE spelled_b (id integer PRIMARY KEY);\n",
		"010_c.sql": "CREATE TABLE spelled_c (id integer PRIMARY KEY);\n",
		"011_d.sql": "CREATE TABLE spelled_d (id integer PRIMARY KEY);\n",
	}
	for _, name := range names {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(bodies[name]), 0o600), qt.IsNil)
	}
	_, err := migratesum.WriteWithFormat(dir, migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
}

func applySpelledDirectory(c *qt.C, dir, dbURL string) string {
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "apply", "--dir", "file://" + dir, "--url", dbURL})
	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("output:\n%s", out.String()))
	return out.String()
}

// recordedVersions reads the version column in the order the rows were
// written, so a run that re-applied a migration shows up as a changed row set
// and one that wrote the wrong spelling as a changed value.
func recordedVersions(c *qt.C, dbURL string) []string {
	c.Helper()
	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	rows, err := db.QueryContext(
		context.Background(),
		"SELECT version FROM atlas_schema_revisions.atlas_schema_revisions ORDER BY executed_at, version",
	)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var versions []string
	for rows.Next() {
		var version string
		c.Assert(rows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return versions
}

func TestMigrateApplyRecordsTheFileNameVersionLivePostgres(t *testing.T) {
	c := qt.New(t)
	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	dbURL := newCleanGateDatabase(c, adminURL, nil)
	dir := filepath.Join(c.TempDir(), "migrations")

	writeSpelledPostgresDirectory(c, dir, "001_a.sql", "002_b.sql", "010_c.sql")
	applySpelledDirectory(c, dir, dbURL)
	c.Assert(recordedVersions(c, dbURL), qt.DeepEquals, []string{"001", "002", "010"})

	// The next run reads those rows back as the files they came from: only the
	// new file is pending, and nothing already applied runs again.
	writeSpelledPostgresDirectory(c, dir, "011_d.sql")
	out := applySpelledDirectory(c, dir, dbURL)
	c.Assert(out, qt.Contains, "Migrating to version 011")
	c.Assert(recordedVersions(c, dbURL), qt.DeepEquals, []string{"001", "002", "010", "011"})
}
