//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// writeMigration writes a one-file migrations directory holding script and
// returns the directory.
func writeMigration(c *qt.C, script string) string {
	c.Helper()
	migrations := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(migrations, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "20240101000000_hash.up.sql"), []byte(script), 0o600), qt.IsNil)
	return migrations
}

// readOne returns the single text value query answers on database.
func readOne(c *qt.C, database, query string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), database)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var got string
	c.Assert(conn.QueryRowContext(c.Context(), query).Scan(&got), qt.IsNil)
	return got
}

// TestMigrationsUpRunsARoutineBelowAHashBannerE2E applies a MySQL-family
// migration that opens with a banner line written as a # comment holding an
// apostrophe, above a routine written between DELIMITER lines. The server
// reads every # as the start of a comment, so the apostrophe opens no literal
// and the routine is created, has inserted its rows, and reads back from the
// catalog.
func TestMigrationsUpRunsARoutineBelowAHashBannerE2E(t *testing.T) {
	engines := []struct {
		name   string
		target dbtarget.Engine
	}{
		{"mysql", dbtarget.MySQLAdmin},
		{"mariadb", dbtarget.MariaDBAdmin},
	}
	for _, test := range engines {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := mysqlFamilyScratchURL(c, test.target, "hash")
			migrations := writeMigration(c, "-- atlas:txmode none\n"+
				"#=========== setup: don't touch ===========\n"+
				"CREATE TABLE banner_rows (id INT PRIMARY KEY, n VARCHAR(40));\n"+
				"DELIMITER $$\n"+
				"CREATE PROCEDURE banner_p() BEGIN INSERT INTO banner_rows VALUES (1, 'x'); INSERT INTO banner_rows VALUES (2, 'it''s'); END$$\n"+
				"DELIMITER ;\n"+
				"CALL banner_p();\n")

			applied, err := runPtahNativeWithError("migrations", "up", "--db-url", database, "--migrations-dir", migrations)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
			c.Assert(readOne(c, database, "SELECT routine_name FROM information_schema.routines WHERE routine_schema = DATABASE()"),
				qt.Equals, "banner_p")
			c.Assert(readOne(c, database, "SELECT GROUP_CONCAT(n ORDER BY id) FROM banner_rows"), qt.Equals, "x,it's")
		})
	}
}

// TestMigrationsUpReadsHashAsAnOperatorOnPostgreSQLE2E applies a PostgreSQL
// migration that uses # as the XOR operator on a line that also holds a
// literal with an apostrophe, and creates an index CONCURRENTLY on the next
// line. # is an operator character there, so the line is SQL to its semicolon
// and the index statement is a statement of its own, which the server
// requires: it refuses CREATE INDEX CONCURRENTLY inside a string of several
// commands, and a # read as a comment would hide the semicolon and join them. The
// rows read back with the XOR value and the literal, and the index exists.
func TestMigrationsUpReadsHashAsAnOperatorOnPostgreSQLE2E(t *testing.T) {
	c := qt.New(t)
	database, _ := scratchReplayDatabase(c)
	migrations := writeMigration(c, "-- atlas:txmode none\n"+
		"CREATE TABLE xor_rows (id int PRIMARY KEY, v int, note text);\n"+
		"INSERT INTO xor_rows VALUES (2, 0, ('{\"a\":{\"b\":2}}'::jsonb #>> '{a,b}') || '''');\n"+
		"INSERT INTO xor_rows VALUES (1, 5 # 3, 'it''s');\n"+
		"CREATE INDEX CONCURRENTLY xor_rows_v ON xor_rows (v);\n")

	applied, err := runPtahNativeWithError("migrations", "up", "--db-url", database, "--migrations-dir", migrations)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
	c.Assert(readOne(c, database, "SELECT string_agg(v || ':' || note, ',' ORDER BY id) FROM xor_rows"), qt.Equals, "6:it's,0:2'")
	c.Assert(readOne(c, database, "SELECT indexname FROM pg_indexes WHERE tablename = 'xor_rows' AND indexname = 'xor_rows_v'"),
		qt.Equals, "xor_rows_v")
}
