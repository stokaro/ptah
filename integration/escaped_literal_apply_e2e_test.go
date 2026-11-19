//go:build integration

package integration_test

import (
	"net/url"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// escapedLiteralModel declares literal defaults as a Go struct tag writes
// them: a backslash (a\b), a backslash before a quote (it\'s;x), a quote
// (it's;x) and the word NULL, which a literal holds as a string. attributes
// follow the first of them, and fields are further fields of the struct.
func escapedLiteralModel(attributes, fields string) declaredModel {
	return declaredModel{
		source: "package entities\n\n" +
			"//ptah:schema:table name=\"notes\"\n" +
			"type Note struct {\n" +
			"\t//ptah:schema:field name=\"id\" type=\"INT\" primary=\"true\"\n" +
			"\tID int\n" +
			"\t//ptah:schema:field name=\"a\" type=\"VARCHAR(30)\" default=\"a\\\\b\"" + attributes + "\n" +
			"\tA string\n" +
			"\t//ptah:schema:field name=\"q\" type=\"VARCHAR(30)\" default=\"it\\'s;x\"\n" +
			"\tQ string\n" +
			"\t//ptah:schema:field name=\"r\" type=\"VARCHAR(30)\" default=\"it's;x\"\n" +
			"\tR string\n" +
			"\t//ptah:schema:field name=\"n\" type=\"VARCHAR(30)\" default=\"NULL\"\n" +
			"\tN string\n" +
			fields +
			"}\n",
		file: "notes.go",
		flag: "--root-dir",
	}
}

// mysqlFamilyFields declares an enum whose first value holds a backslash, and
// a default that holds two backslashes in a row (a\\b).
const mysqlFamilyFields = `	//ptah:schema:field name="e" type="ENUM" enum="e\\f,g" default="g"
	E string
	//ptah:schema:field name="b" type="VARCHAR(30)" default="a\\\\b"
	B string
`

// escapedLiteralMySQLRead reads the stored defaults of the MySQL family in
// hexadecimal, with the comment of column a, the type of enum column e and
// the default of column b.
const escapedLiteralMySQLRead = "SELECT CONCAT_WS(',', HEX(a), HEX(q), HEX(r), n, (SELECT HEX(COLUMN_COMMENT) FROM information_schema.COLUMNS " +
	"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'notes' AND COLUMN_NAME = 'a'), (SELECT HEX(COLUMN_TYPE) " +
	"FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'notes' AND COLUMN_NAME = 'e'), HEX(b)) FROM notes"

// The sql_mode values an escaped literal row sets on its session: the
// server's own, under which a backslash in a string is an escape, and the
// same with NO_BACKSLASH_ESCAPES, under which it is the character.
const (
	serverSQLMode          = "@@sql_mode"
	noBackslashEscapesMode = "CONCAT(@@sql_mode,',NO_BACKSLASH_ESCAPES')"
)

// TestApplyKeepsAnEscapedLiteralDefaultE2E applies a Go model whose literal
// defaults hold a backslash, a quote or the word NULL, with a column comment
// and an enum value holding a backslash, twice, and reads what the server
// stored back in hexadecimal. Each row sets the session's sql_mode. Under the
// server's own mode a backslash in a string is an escape, so the renderer
// doubles it; under NO_BACKSLASH_ESCAPES it is the character, so the apply
// sends it once. The reader writes a stored backslash back doubled under
// either mode. Each server stores the characters the model declares, and the
// second apply plans nothing.
func TestApplyKeepsAnEscapedLiteralDefaultE2E(t *testing.T) {
	const want = "615C62,69745C27733B78,697427733B78,NULL,635C64,656E756D2827655C5C66272C27672729,615C5C62"
	rows := []struct {
		name    string
		engine  dbtarget.Engine
		sqlMode string
		// noBackslashEscapes is whether the session's sql_mode names
		// NO_BACKSLASH_ESCAPES, read back from the session.
		noBackslashEscapes bool
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin, sqlMode: serverSQLMode},
		{name: "mysql without backslash escapes", engine: dbtarget.MySQLAdmin, sqlMode: noBackslashEscapesMode, noBackslashEscapes: true},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin, sqlMode: serverSQLMode},
		{name: "mariadb without backslash escapes", engine: dbtarget.MariaDBAdmin, sqlMode: noBackslashEscapesMode, noBackslashEscapes: true},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			database := withSQLMode(c, mysqlFamilyScratchURL(c, row.engine, "escaped_default"), row.sqlMode)

			got := applyEscapedLiteralModel(c, database, escapedLiteralModel(` comment="c\\d"`, mysqlFamilyFields), escapedLiteralMySQLRead)

			c.Assert(strings.Contains(sessionSQLMode(c, database), "NO_BACKSLASH_ESCAPES"), qt.Equals, row.noBackslashEscapes)
			c.Assert(got, qt.Equals, want)
		})
	}
}

// TestApplyKeepsAnEscapedLiteralDefaultOnPostgreSQLE2E is the control on an
// engine that reads a backslash in a string as itself: the same model stores
// the same characters, and the second apply plans nothing.
func TestApplyKeepsAnEscapedLiteralDefaultOnPostgreSQLE2E(t *testing.T) {
	c := qt.New(t)
	database := postgresScratchDevURL(c, "escaped_default")
	const read = "SELECT concat_ws(',', encode(convert_to(a, 'UTF8'), 'hex'), encode(convert_to(q, 'UTF8'), 'hex'), " +
		"encode(convert_to(r, 'UTF8'), 'hex'), n, encode(convert_to(col_description('notes'::regclass, 2), 'UTF8'), 'hex')) FROM notes"

	got := applyEscapedLiteralModel(c, database, escapedLiteralModel(` comment="c\\d"`, ""), read)

	c.Assert(got, qt.Equals, "615c62,69745c27733b78,697427733b78,NULL,635c64")
}

// TestApplyKeepsAnEscapedLiteralDefaultOnClickHouseE2E applies the model to
// ClickHouse, which reads a backslash in a string as an escape and reports a
// quote inside a default behind a backslash. The comparison reads a text
// default as the string it stores, so the second apply plans nothing.
func TestApplyKeepsAnEscapedLiteralDefaultOnClickHouseE2E(t *testing.T) {
	c := qt.New(t)
	database := createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.ClickHouse), "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
	const read = "SELECT concat(hex(assumeNotNull(a)), ',', hex(assumeNotNull(q)), ',', hex(assumeNotNull(r)), ',', " +
		"assumeNotNull(n)) FROM notes"

	got := applyEscapedLiteralModel(c, database, escapedLiteralModel("", ""), read)

	c.Assert(got, qt.Equals, "615C62,69745C27733B78,697427733B78,NULL")
}

// applyEscapedLiteralModel applies model to database twice, asserts that the
// first apply succeeds and the second plans nothing, inserts a row that takes
// every default, and returns what read answers for it.
func applyEscapedLiteralModel(c *qt.C, database string, model declaredModel, read string) string {
	c.Helper()
	created := applyModel(c, database, model)
	again := applyModel(c, database, model)
	c.Assert(created.ExitCode, qt.Equals, 0, historyComment(created))
	c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n", historyComment(again))
	conn, err := dbschema.ConnectToDatabase(c.Context(), database)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), "INSERT INTO notes (id) VALUES (1)")
	c.Assert(err, qt.IsNil)
	var got string
	c.Assert(conn.QueryRowContext(c.Context(), read).Scan(&got), qt.IsNil)
	return got
}

// sessionSQLMode returns the sql_mode of a session opened on database, which
// shows whether a parameter the address carries reached the session.
func sessionSQLMode(c *qt.C, database string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), database)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var sqlMode string
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT @@SESSION.sql_mode").Scan(&sqlMode), qt.IsNil)
	return sqlMode
}

// withSQLMode returns database with a query parameter the driver sets on
// every session it opens: sqlMode is the expression the session's sql_mode
// is set to. The address may name its host as tcp(host:port), which
// url.Parse refuses, so only the query is parsed.
func withSQLMode(c *qt.C, database, sqlMode string) string {
	c.Helper()
	address, rawQuery, _ := strings.Cut(database, "?")
	query, err := url.ParseQuery(rawQuery)
	c.Assert(err, qt.IsNil)
	query.Set("sql_mode", sqlMode)
	return address + "?" + query.Encode()
}

// mysqlFamilyScratchURL creates an empty database on engine's server and
// returns the address that connects to it.
func mysqlFamilyScratchURL(c *qt.C, engine dbtarget.Engine, prefix string) string {
	c.Helper()
	scratch := newMySQLFamilyScratch(c, engine)
	name, _ := scratch.database(c, prefix)
	return replaceMySQLDatabaseName(c, scratch.adminURL, name)
}
