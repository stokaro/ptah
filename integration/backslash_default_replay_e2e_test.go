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

// backslashDefaultSchema holds string defaults whose stored values carry a
// backslash, and one a quote too: a\b and a'b\c under MySQL's default sql_mode.
const backslashDefaultSchema = "CREATE TABLE notes (\n" +
	"  id int PRIMARY KEY,\n" +
	"  a varchar(30) DEFAULT 'a\\\\b',\n" +
	"  q varchar(30) DEFAULT 'a\\'b\\\\c'\n" +
	");\n"

// TestDBReadReplaysABackslashInAStringDefaultE2E creates the defaults, reads
// the database with `ptah db read`, applies what the read wrote to an empty
// database, and reads the stored values back in hexadecimal. A read that
// writes the stored backslash without doubling it replays a\b as a and a
// backspace, and a'b\c as a'bc.
func TestDBReadReplaysABackslashInAStringDefaultE2E(t *testing.T) {
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
			sourceName, _ := scratch.database(c, "backslash_read")
			source := replaceMySQLDatabaseName(c, scratch.adminURL, sourceName)
			replayName, _ := scratch.database(c, "backslash_replay")
			replay := replaceMySQLDatabaseName(c, scratch.adminURL, replayName)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(backslashDefaultSchema), 0o600), qt.IsNil)
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
			_, err = conn.ExecContext(c.Context(), "INSERT INTO notes (id) VALUES (1)")
			c.Assert(err, qt.IsNil)
			var got string
			c.Assert(conn.QueryRowContext(c.Context(), "SELECT CONCAT_WS(',', HEX(a), HEX(q)) FROM notes").Scan(&got), qt.IsNil)
			c.Assert(got, qt.Equals, "615C62,6127625C63")
		})
	}
}
