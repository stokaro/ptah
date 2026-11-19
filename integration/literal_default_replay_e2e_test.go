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

// literalDefaultSchema holds a default in each literal form the MySQL family
// writes beside a plain string: a bit string, a hexadecimal string, MySQL's
// 0x and 0b forms on a binary, an integer and a bit column, a string
// expression on a varbinary, a call MariaDB prints by another name and calls
// each engine takes by another, a character set introducer, adjacent strings,
// a double-quoted string, hexadecimal and bit literals on text columns, one of
// them spelling a backslash and a quote, a string holding a line feed and a
// tab, and a string that starts and ends with a quote.
const literalDefaultSchema = "CREATE TABLE literals (\n" +
	"  id int PRIMARY KEY,\n" +
	"  b bit(3) DEFAULT b'101',\n" +
	"  x varbinary(8) DEFAULT X'6A6B',\n" +
	"  h varbinary(8) DEFAULT 0x61626364,\n" +
	"  n int DEFAULT 0x1F,\n" +
	"  o bit(3) DEFAULT 0b110,\n" +
	"  p varbinary(8) DEFAULT ('xy'),\n" +
	"  w varchar(30) DEFAULT (upper('abc')),\n" +
	"  u varchar(30) DEFAULT _utf8mb4'u',\n" +
	"  j varchar(30) DEFAULT 'a' 'b',\n" +
	"  d varchar(30) DEFAULT \"d'q\",\n" +
	"  t varchar(8) DEFAULT 0x41,\n" +
	"  s varchar(8) DEFAULT b'1100010',\n" +
	"  q varchar(8) DEFAULT 0x5C27,\n" +
	"  m int DEFAULT b'101',\n" +
	"  v varchar(30) DEFAULT (ucase('abc')),\n" +
	"  l varchar(30) DEFAULT (LCASE('ABC')),\n" +
	"  f varchar(8) DEFAULT 'a\\nb\\tc',\n" +
	"  k varchar(8) DEFAULT '''k'''\n" +
	");\n"

// TestDBReadReplaysALiteralDefaultE2E applies the schema twice, reads the
// database with `ptah db read`, applies what the read wrote to an empty
// database twice, and reads the stored values back. Every second apply plans
// nothing, and the replay stores what the schema declared.
func TestDBReadReplaysALiteralDefaultE2E(t *testing.T) {
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
			sourceName, _ := scratch.database(c, "literal_read")
			source := replaceMySQLDatabaseName(c, scratch.adminURL, sourceName)
			replayName, _ := scratch.database(c, "literal_replay")
			replay := replaceMySQLDatabaseName(c, scratch.adminURL, replayName)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(literalDefaultSchema), 0o600), qt.IsNil)
			created := runPtahIn(c, "schema", "apply", "--db-url", source, "--schema-file", schema, "--auto-approve")
			kept := runPtahIn(c, "schema", "apply", "--db-url", source, "--schema-file", schema, "--auto-approve")

			read := runPtahIn(c, "db", "read", "--db-url", source)
			readFile := filepath.Join(c.TempDir(), "read.sql")
			c.Assert(os.WriteFile(readFile, []byte(read.Stdout), 0o600), qt.IsNil)
			apply := []string{"schema", "apply", "--db-url", replay, "--schema-file", readFile, "--auto-approve"}
			replayed := runPtahIn(c, apply...)
			again := runPtahIn(c, apply...)

			c.Assert(created.ExitCode, qt.Equals, 0, historyComment(created))
			c.Assert(kept.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n", historyComment(kept))
			c.Assert(read.ExitCode, qt.Equals, 0, historyComment(read))
			c.Assert(replayed.ExitCode, qt.Equals, 0, historyComment(replayed))
			c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n", historyComment(again))
			conn, err := dbschema.ConnectToDatabase(c.Context(), replay)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)
			_, err = conn.ExecContext(c.Context(), "INSERT INTO literals (id) VALUES (1)")
			c.Assert(err, qt.IsNil)
			var got string
			query := "SELECT CONCAT_WS(',', b + 0, HEX(x), HEX(h), n, o + 0, HEX(p), w, u, j, d, t, s, HEX(q), m, v, l, HEX(f), HEX(k)) FROM literals"
			c.Assert(conn.QueryRowContext(c.Context(), query).Scan(&got), qt.IsNil)
			c.Assert(got, qt.Equals, "5,6A6B,61626364,31,6,7879,ABC,u,ab,d'q,A,b,5C27,5,ABC,abc,610A620963,276B27")
		})
	}
}
