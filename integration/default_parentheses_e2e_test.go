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

// precedenceDefaultSchema declares expression defaults each server stores in a
// spelling of its own. MySQL 26.10 stores (2 * 3 + 1) as ((2 * 3) + 1),
// (a + b * 2) as (`a` + (`b` * 2)), (-1) as -(1) and
// (CURRENT_TIMESTAMP + INTERVAL 1 DAY) as (now() + interval 1 day), and
// (concat('a', 'b')) as concat(_utf8mb4'a',_utf8mb4'b'), escaped. MariaDB
// 12.3 stores (7 % 4 * 2) as (7 MOD 4 * 2), (1+2) as (1 + 2), quotes the
// column references, writes current_timestamp() and concat('a','b'). q and u
// hold the parentheses their grouping needs. MySQL 8.4 and 26.10 report
// (uuid()) as uuid(), which the server refuses without the parentheses, and
// report the string 'now()' as now() beside the expression (now()).
const precedenceDefaultSchema = "CREATE TABLE tallies (\n" +
	"  id int PRIMARY KEY,\n" +
	"  a int DEFAULT 2,\n" +
	"  b int DEFAULT 3,\n" +
	"  p int DEFAULT (2 * 3 + 1),\n" +
	"  q int DEFAULT ((1 + 2) * 3),\n" +
	"  r int DEFAULT (7 % 4 * 2),\n" +
	"  s int DEFAULT (a + b * 2),\n" +
	"  u int DEFAULT (1 - (2 - 3)),\n" +
	"  v int DEFAULT (1+2),\n" +
	"  w int DEFAULT (-1),\n" +
	"  e datetime DEFAULT (CURRENT_TIMESTAMP + INTERVAL 1 DAY),\n" +
	"  t varchar(10) DEFAULT (concat('a', 'b')),\n" +
	"  g varchar(40) DEFAULT (uuid()),\n" +
	"  l varchar(30) DEFAULT 'now()',\n" +
	"  n varchar(30) DEFAULT (now())\n" +
	");\n"

// TestApplyConvergesOnAPrecedenceDefaultE2E applies precedenceDefaultSchema
// twice through `ptah schema apply`. The second apply plans nothing, which
// says the comparison reads the spelling the server stores as the default the
// file declares, and a row that takes every default reads back what the
// server computed for each.
func TestApplyConvergesOnAPrecedenceDefaultE2E(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, test.engine)
			name, _ := scratch.database(c, "default_precedence")
			database := replaceMySQLDatabaseName(c, scratch.adminURL, name)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(precedenceDefaultSchema), 0o600), qt.IsNil)
			apply := []string{"schema", "apply", "--db-url", database, "--schema-file", schema, "--auto-approve"}

			applied := runPtahIn(c, apply...)
			again := runPtahIn(c, apply...)

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(again.ExitCode, qt.Equals, 0, historyComment(again))
			c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			conn, err := dbschema.ConnectToDatabase(c.Context(), database)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)
			_, err = conn.ExecContext(c.Context(), "INSERT INTO tallies (id) VALUES (1)")
			c.Assert(err, qt.IsNil)
			var got string
			err = conn.QueryRowContext(c.Context(), "SELECT CONCAT_WS(',', p, q, r, s, u, v, w, e > NOW(), t, CHAR_LENGTH(g), l, n <> 'now()') FROM tallies").Scan(&got)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, "7,9,6,8,2,3,-1,1,ab,36,now(),1")
		})
	}
}

// TestDBReadReplaysAnExpressionDefaultE2E reads a database holding
// precedenceDefaultSchema with `ptah db read` and applies what it wrote to an
// empty database. The read writes each expression default as one, in
// parentheses: MySQL 26.10 reports concat(_utf8mb4\'a\',_utf8mb4\'b\') with
// the parentheses dropped and the quotes escaped, and a read that kept that
// text, or quoted it as a string, would not replay. The replayed database
// computes the defaults the source does, and a second apply plans nothing.
func TestDBReadReplaysAnExpressionDefaultE2E(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, test.engine)
			sourceName, _ := scratch.database(c, "default_read")
			source := replaceMySQLDatabaseName(c, scratch.adminURL, sourceName)
			replayName, _ := scratch.database(c, "default_replay")
			replay := replaceMySQLDatabaseName(c, scratch.adminURL, replayName)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(precedenceDefaultSchema), 0o600), qt.IsNil)
			created := runPtahIn(c, "schema", "apply", "--db-url", source, "--schema-file", schema, "--auto-approve")
			c.Assert(created.ExitCode, qt.Equals, 0, historyComment(created))

			read := runPtahIn(c, "db", "read", "--db-url", source)
			c.Assert(read.ExitCode, qt.Equals, 0, historyComment(read))
			readFile := filepath.Join(c.TempDir(), "read.sql")
			c.Assert(os.WriteFile(readFile, []byte(read.Stdout), 0o600), qt.IsNil)
			apply := []string{"schema", "apply", "--db-url", replay, "--schema-file", readFile, "--auto-approve"}
			replayed := runPtahIn(c, apply...)
			again := runPtahIn(c, apply...)

			c.Assert(replayed.ExitCode, qt.Equals, 0, historyComment(replayed))
			c.Assert(again.ExitCode, qt.Equals, 0, historyComment(again))
			c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
			conn, err := dbschema.ConnectToDatabase(c.Context(), replay)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)
			_, err = conn.ExecContext(c.Context(), "INSERT INTO tallies (id) VALUES (1)")
			c.Assert(err, qt.IsNil)
			var got string
			err = conn.QueryRowContext(c.Context(), "SELECT CONCAT_WS(',', p, q, r, s, u, v, w, e > NOW(), t, CHAR_LENGTH(g), l, n <> 'now()') FROM tallies").Scan(&got)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, "7,9,6,8,2,3,-1,1,ab,36,now(),1")
		})
	}
}
