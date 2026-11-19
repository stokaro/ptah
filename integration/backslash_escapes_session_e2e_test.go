//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// noBackslashEscapesEngines are the MySQL-family servers a session under
// NO_BACKSLASH_ESCAPES is opened on.
var noBackslashEscapesEngines = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "mysql", engine: dbtarget.MySQLAdmin},
	{name: "mariadb", engine: dbtarget.MariaDBAdmin},
}

// noBackslashEscapesDatabase creates an empty database on engine's server and
// returns an address whose sessions add NO_BACKSLASH_ESCAPES to the server's
// sql_mode, after checking that the mode reached a session.
func noBackslashEscapesDatabase(c *qt.C, engine dbtarget.Engine) string {
	c.Helper()
	database := withSQLMode(c, mysqlFamilyScratchURL(c, engine, "no_backslash_escapes"), "CONCAT(@@sql_mode,',NO_BACKSLASH_ESCAPES')")
	c.Assert(sessionSQLMode(c, database), qt.Contains, "NO_BACKSLASH_ESCAPES")
	return database
}

// readNoBackslashEscapes answers query on a session of database.
func readNoBackslashEscapes(c *qt.C, database, query string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), database)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var got string
	c.Assert(conn.QueryRowContext(c.Context(), query).Scan(&got), qt.IsNil)
	return got
}

// writeNoBackslashEscapesSchema writes text to a schema file of its own and returns its path.
func writeNoBackslashEscapesSchema(c *qt.C, text string) string {
	c.Helper()
	file := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(file, []byte(text), 0o600), qt.IsNil)
	return file
}

// TestApplyRefusesABackslashLiteralWithoutBackslashEscapesE2E applies a Go
// model whose defaults, comment and enum value hold a backslash through a
// session under NO_BACKSLASH_ESCAPES. Ptah writes those literals for the
// default sql_mode, where `'a\\b'` holds one backslash, and the session would
// store two, so the apply is refused before anything runs: it names the
// literals and the mode, and the table is not created.
func TestApplyRefusesABackslashLiteralWithoutBackslashEscapesE2E(t *testing.T) {
	for _, row := range noBackslashEscapesEngines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			database := noBackslashEscapesDatabase(c, row.engine)

			applied := applyModel(c, database, escapedLiteralModel(` comment="c\\d"`, mysqlFamilyFields))

			c.Assert(applied.ExitCode, qt.Not(qt.Equals), 0, historyComment(applied))
			c.Assert(applied.Stderr, qt.Contains, "the session's sql_mode names NO_BACKSLASH_ESCAPES, and the plan writes a backslash in "+
				"the default of column notes.a, the comment of column notes.a, the default of column notes.q, the type of column notes.e, "+
				"the default of column notes.b;",
				historyComment(applied))
			c.Assert(readNoBackslashEscapes(c, database,
				"SELECT CAST(COUNT(*) AS CHAR) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE()"), qt.Equals, "0")
		})
	}
}

// TestApplyWithoutABackslashLiteralWithoutBackslashEscapesE2E applies the same
// shape of model, with a quote and no backslash, through a session under
// NO_BACKSLASH_ESCAPES. A doubled quote reads the same under both modes, so the
// server stores the characters the model declares and the second apply plans
// nothing.
func TestApplyWithoutABackslashLiteralWithoutBackslashEscapesE2E(t *testing.T) {
	model := declaredModel{
		source: "package entities\n\n" +
			"//ptah:schema:table name=\"notes\"\n" +
			"type Note struct {\n" +
			"\t//ptah:schema:field name=\"id\" type=\"INT\" primary=\"true\"\n" +
			"\tID int\n" +
			"\t//ptah:schema:field name=\"a\" type=\"VARCHAR(30)\" default=\"it's;x\" comment=\"c'd\"\n" +
			"\tA string\n" +
			"\t//ptah:schema:field name=\"e\" type=\"ENUM\" enum=\"e'f,g\" default=\"g\"\n" +
			"\tE string\n" +
			"}\n",
		file: "notes.go",
		flag: "--root-dir",
	}
	const read = "SELECT CONCAT_WS(',', HEX(a), (SELECT HEX(COLUMN_COMMENT) FROM information_schema.COLUMNS " +
		"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'notes' AND COLUMN_NAME = 'a'), (SELECT HEX(COLUMN_TYPE) " +
		"FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'notes' AND COLUMN_NAME = 'e')) FROM notes"
	for _, row := range noBackslashEscapesEngines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			database := noBackslashEscapesDatabase(c, row.engine)

			got := applyEscapedLiteralModel(c, database, model, read)

			c.Assert(got, qt.Equals, "697427733B78,632764,656E756D282765272766272C27672729")
		})
	}
}

// TestApplySendsASchemaFileBodyAsWrittenWithoutBackslashEscapesE2E applies a
// schema file whose function and view bodies hold a backslash through a
// session under NO_BACKSLASH_ESCAPES. A body is the user's own SQL, so it
// reaches the server byte for byte and the session reads it: `'C:\path'` is
// C, a colon, a backslash and path, and `'a\\b'` holds two backslashes.
func TestApplySendsASchemaFileBodyAsWrittenWithoutBackslashEscapesE2E(t *testing.T) {
	schema := "CREATE TABLE notes (id int PRIMARY KEY);\n" +
		"CREATE FUNCTION path_of() RETURNS varchar(30) DETERMINISTIC RETURN 'C:\\path';\n" +
		"CREATE VIEW slashes AS SELECT 'a\\\\b' AS x;\n"
	for _, row := range noBackslashEscapesEngines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			database := noBackslashEscapesDatabase(c, row.engine)

			applied := runPtahIn(c, "schema", "apply", "--db-url", database, "--schema-file", writeNoBackslashEscapesSchema(c, schema), "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(readNoBackslashEscapes(c, database, "SELECT CONCAT_WS(',', HEX(path_of()), (SELECT HEX(x) FROM slashes))"),
				qt.Equals, "433A5C70617468,615C5C62")
		})
	}
}

// TestApplySendsEditedSQLAsWrittenWithoutBackslashEscapesE2E applies a schema
// file with --edit through a session under NO_BACKSLASH_ESCAPES, with an
// editor that appends a statement setting a default of 'C:\p'. Edited SQL is
// the user's own, so it reaches the server byte for byte and the column
// stores C, a colon, a backslash and p.
func TestApplySendsEditedSQLAsWrittenWithoutBackslashEscapesE2E(t *testing.T) {
	for _, row := range noBackslashEscapesEngines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			database := noBackslashEscapesDatabase(c, row.engine)
			dir := c.TempDir()
			appended := filepath.Join(dir, "appended.sql")
			c.Assert(os.WriteFile(appended, []byte("ALTER TABLE notes ALTER COLUMN u SET DEFAULT 'C:\\p';\n"), 0o600), qt.IsNil)
			editor := filepath.Join(dir, "append.sh")
			script := fmt.Sprintf("#!/bin/sh\ncat \"$1\" %q > \"$1.new\" && mv \"$1.new\" \"$1\"\n", appended)
			c.Assert(os.WriteFile(editor, []byte(script), 0o700), qt.IsNil) // #nosec G306 -- the editor must be executable
			env := append(os.Environ(), "VISUAL="+editor, "PTAH_ALLOW_NONINTERACTIVE_EDIT=1")
			schema := writeNoBackslashEscapesSchema(c, "CREATE TABLE notes (id int PRIMARY KEY, u varchar(30));\n")

			applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: env},
				"schema", "apply", "--db-url", database, "--schema-file", schema, "--edit", "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(insertDefaultsAndRead(c, database, "SELECT HEX(u) FROM notes"), qt.Equals, "433A5C70")
		})
	}
}
