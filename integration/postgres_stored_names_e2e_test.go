//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/sqlext"
	"ptah.run/dbschema"
	"ptah.run/internal/sqlschema"
)

// longNames is a file whose table, column, index and foreign key names are
// past the 63 bytes PostgreSQL keeps, one of them in capitals unquoted and one
// ending in a two-byte character that straddles the limit.
var longNames = `CREATE TABLE owners (id bigint PRIMARY KEY);
CREATE TABLE "` + strings.Repeat("t", 70) + `" (
  id bigint PRIMARY KEY,
  "` + strings.Repeat("c", 62) + `é" bigint,
  ` + strings.Repeat("U", 64) + ` bigint,
  CONSTRAINT "` + strings.Repeat("k", 70) + `" FOREIGN KEY ("` + strings.Repeat("c", 62) + `é") REFERENCES owners (id)
);
CREATE INDEX "` + strings.Repeat("i", 70) + `" ON "` + strings.Repeat("t", 70) + `" ("` + strings.Repeat("c", 62) + `é");
`

// storedNamesOf reads sql as PostgreSQL and returns the names it cut, as
// stored.
func storedNamesOf(c *qt.C, sql string) ([]string, error) {
	c.Helper()
	var stored []string
	_, _, err := sqlschema.ReadWith(context.Background(), []byte(sql), platform.Postgres, nil, sqlschema.Options{
		Owners:      sqlext.None(),
		StoredNames: func(name sqlschema.StoredName) { stored = append(stored, name.Stored) },
	})
	return stored, err
}

// catalogNames is every table, column, index and constraint name in the
// public schema of the database at url.
func catalogNames(c *qt.C, url string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(c.Context(), `
		SELECT relname FROM pg_class WHERE relnamespace = 'public'::regnamespace
		UNION ALL SELECT conname FROM pg_constraint WHERE connamespace = 'public'::regnamespace
		UNION ALL SELECT attname FROM pg_attribute a JOIN pg_class r ON r.oid = a.attrelid
			WHERE r.relnamespace = 'public'::regnamespace AND a.attnum > 0`)
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

// TestPostgresStoredNamesMatchTheCatalogE2E runs the file on the server and
// holds each name the reader says the server keeps against the catalog.
func TestPostgresStoredNamesMatchTheCatalogE2E(t *testing.T) {
	c := qt.New(t)
	stored, err := storedNamesOf(c, longNames)
	c.Assert(err, qt.IsNil)
	c.Assert(stored, qt.HasLen, 5)

	catalog := catalogNames(c, databaseBuiltFrom(c, longNames))

	for _, name := range stored {
		c.Assert(catalog, qt.Contains, name, qt.Commentf("%q is not in the catalog %q", name, catalog))
	}
}

// TestPostgresStoredNameCollisionsAgreeWithTheServerE2E runs each file on the
// server and through the reader: the reader refuses exactly the files the
// server refuses, for a name cut to one already in use and for a name
// written twice.
func TestPostgresStoredNameCollisionsAgreeWithTheServerE2E(t *testing.T) {
	long := strings.Repeat("k", 63)
	tests := []struct {
		name    string
		sql     string
		refused bool
	}{
		{
			name: "two indexes in one schema",
			sql: "CREATE TABLE t (a int, b int);\n" + `CREATE INDEX "` + long + `_alpha" ON t (a);` + "\n" +
				`CREATE INDEX "` + long + `_beta" ON t (b);`,
			refused: true,
		},
		{
			name: "two foreign keys on one table",
			sql: "CREATE TABLE p (id int PRIMARY KEY);\n" + `CREATE TABLE c (id int PRIMARY KEY, a int, b int, CONSTRAINT "` + long +
				`_alpha" FOREIGN KEY (a) REFERENCES p (id), CONSTRAINT "` + long + `_beta" FOREIGN KEY (b) REFERENCES p (id));`,
			refused: true,
		},
		{
			name:    "an index cut to the name of a table",
			sql:     `CREATE TABLE "` + long + `" (a int);` + "\n" + `CREATE INDEX "` + long + `_x" ON "` + long + `" (a);`,
			refused: true,
		},
		{
			name: "a unique constraint and an index",
			sql: "CREATE TABLE t (a int, b int, CONSTRAINT \"" + long + "_alpha\" UNIQUE (a));\n" +
				`CREATE INDEX "` + long + `_beta" ON t (b);`,
			refused: true,
		},
		{
			name: "foreign keys cut to one name on two tables",
			sql: "CREATE TABLE p (id int PRIMARY KEY);\n" +
				`CREATE TABLE c1 (id int PRIMARY KEY, a int, CONSTRAINT "` + long + `_alpha" FOREIGN KEY (a) REFERENCES p (id));` + "\n" +
				`CREATE TABLE c2 (id int PRIMARY KEY, a int, CONSTRAINT "` + long + `_beta" FOREIGN KEY (a) REFERENCES p (id));`,
		},
		{
			name: "indexes cut to one name in two schemas",
			sql: "CREATE SCHEMA s1;\nCREATE SCHEMA s2;\nCREATE TABLE s1.t (a int);\nCREATE TABLE s2.t (a int);\n" +
				`CREATE INDEX "` + long + `_alpha" ON s1.t (a);` + "\n" + `CREATE INDEX "` + long + `_beta" ON s2.t (a);`,
		},
		{
			name: "an index guarded by IF NOT EXISTS",
			sql: "CREATE TABLE t (a int);\n" + `CREATE INDEX IF NOT EXISTS "` + long + `_alpha" ON t (a);` + "\n" +
				`CREATE INDEX IF NOT EXISTS "` + long + `_beta" ON t (a);`,
		},
		// A name written twice, which the reader used to merge
		// (stokaro/ptah#4914).
		{name: "an index created twice", sql: "CREATE TABLE t (a int, b int);\nCREATE INDEX ix ON t (a);\nCREATE INDEX ix ON t (b);", refused: true},
		{name: "an index named like a table", sql: "CREATE TABLE t (a int);\nCREATE INDEX t ON t (a);", refused: true},
		{name: "a view named like a table", sql: "CREATE TABLE t (a int);\nCREATE VIEW t AS SELECT 1 AS x;", refused: true},
		{name: "a sequence named like a table", sql: "CREATE TABLE t (a int);\nCREATE SEQUENCE t;", refused: true},
		{name: "a table named like a primary key's index", sql: "CREATE TABLE t (a int, CONSTRAINT t_pk PRIMARY KEY (a));\nCREATE TABLE t_pk (x int);", refused: true},
		{name: "a constraint added under a name in use", sql: "CREATE TABLE p (id int PRIMARY KEY);\nCREATE TABLE c (id int PRIMARY KEY, a int, CONSTRAINT fk FOREIGN KEY (a) REFERENCES p (id));\nALTER TABLE c ADD CONSTRAINT fk CHECK (a > 0);", refused: true},
		{name: "a column written twice", sql: "CREATE TABLE t (a int, a int);", refused: true},
		{name: "a table created twice", sql: "CREATE TABLE t (a int);\nCREATE TABLE t (b int);", refused: true},
		{name: "a table guarded by IF NOT EXISTS", sql: "CREATE TABLE t (a int);\nCREATE TABLE IF NOT EXISTS t (b int);"},
		{name: "a unique constraint written twice the same way", sql: "CREATE TABLE r3 (a int, CONSTRAINT r3_k UNIQUE (a), CONSTRAINT r3_k UNIQUE (a));"},
		{name: "a view replaced", sql: "CREATE TABLE t (a int);\nCREATE VIEW v AS SELECT a FROM t;\nCREATE OR REPLACE VIEW v AS SELECT a FROM t;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, _ := scratchReplayDatabase(c)
			conn, err := dbschema.ConnectToDatabase(c.Context(), target)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)

			_, serverErr := conn.ExecContext(c.Context(), test.sql)
			_, readErr := storedNamesOf(c, test.sql)

			c.Assert(serverErr != nil, qt.Equals, test.refused, qt.Commentf("server: %v", serverErr))
			c.Assert(readErr != nil, qt.Equals, test.refused, qt.Commentf("reader: %v", readErr))
		})
	}
}

// TestPostgresLongNamesRoundTripE2E applies the file through Ptah, reads the
// database back, and diffs it with the file: the names Ptah created are the
// names it compares with, so nothing changes.
func TestPostgresLongNamesRoundTripE2E(t *testing.T) {
	c := qt.New(t)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(longNames), 0o600), qt.IsNil)
	target, _ := scratchReplayDatabase(c)

	runPtahNative(c, "schema", "apply", "--schema-file", schema, "--db-url", target, "--auto-approve")
	out := runPtahNative(c, "schema", "diff", "--from", target, "--to", "file://"+schema)

	c.Assert(out, qt.Contains, "Schemas are synced")
	stored, err := storedNamesOf(c, longNames)
	c.Assert(err, qt.IsNil)
	catalog := catalogNames(c, target)
	for _, name := range stored {
		c.Assert(catalog, qt.Contains, name, qt.Commentf("%q is not in the catalog", name))
	}
}

// TestOpenMeterEntExportReadsWithoutADatabaseE2E reads the SQL OpenMeter's Ent
// exporter writes, pinned in testdata, with no database: its 40 foreign keys
// and 7 indexes named past 63 bytes are read under the names PostgreSQL
// keeps, which the catalog of a database the same SQL built confirms.
func TestOpenMeterEntExportReadsWithoutADatabaseE2E(t *testing.T) {
	c := qt.New(t)
	export, err := os.ReadFile(filepath.Join("testdata", "openmeter", "ent-export.sql"))
	c.Assert(err, qt.IsNil)

	stored, err := storedNamesOf(c, string(export))

	c.Assert(err, qt.IsNil)
	c.Assert(stored, qt.HasLen, 47)
	catalog := catalogNames(c, databaseBuiltFrom(c, string(export)))
	for _, name := range stored {
		c.Assert(catalog, qt.Contains, name, qt.Commentf("%q is not in the catalog", name))
	}
}
