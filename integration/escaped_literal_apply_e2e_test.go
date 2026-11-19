//go:build integration

package integration_test

import (
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

// mysqlFamilyEnumField declares an enum whose first value holds a backslash.
const mysqlFamilyEnumField = `	//ptah:schema:field name="e" type="ENUM" enum="e\\f,g" default="g"
	E string
`

// TestApplyKeepsAnEscapedLiteralDefaultE2E applies a Go model whose literal
// defaults hold a backslash, a quote or the word NULL, twice, and reads the
// stored defaults back in hexadecimal, with a column comment holding a
// backslash on MariaDB and PostgreSQL, and an enum value holding one on
// MariaDB. MariaDB and ClickHouse
// read a backslash in a string as an escape, so the renderer doubles it, and
// the comparison reads a text default as the string it stores, which
// ClickHouse spells with the quote behind a backslash. PostgreSQL reads a
// backslash as itself. Each server stores the characters the model declares,
// and the second apply plans nothing.
func TestApplyKeepsAnEscapedLiteralDefaultE2E(t *testing.T) {
	const mysqlRead = "SELECT CONCAT_WS(',', HEX(a), HEX(q), HEX(r), n, (SELECT HEX(COLUMN_COMMENT) FROM information_schema.COLUMNS " +
		"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'notes' AND COLUMN_NAME = 'a'), (SELECT HEX(COLUMN_TYPE) " +
		"FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'notes' AND COLUMN_NAME = 'e')) FROM notes"
	rows := []struct {
		name     string
		database func(c *qt.C) string
		model    declaredModel
		read     string
		want     string
	}{
		{
			name:     "mariadb",
			database: func(c *qt.C) string { return mysqlFamilyScratchURL(c, dbtarget.MariaDBAdmin, "escaped_default") },
			model:    escapedLiteralModel(` comment="c\\d"`, mysqlFamilyEnumField),
			read:     mysqlRead,
			want:     "615C62,69745C27733B78,697427733B78,NULL,635C64,656E756D2827655C5C66272C27672729",
		},
		{
			name:     "postgres",
			database: func(c *qt.C) string { return postgresScratchDevURL(c, "escaped_default") },
			model:    escapedLiteralModel(` comment="c\\d"`, ""),
			read: "SELECT concat_ws(',', encode(convert_to(a, 'UTF8'), 'hex'), encode(convert_to(q, 'UTF8'), 'hex'), " +
				"encode(convert_to(r, 'UTF8'), 'hex'), n, encode(convert_to(col_description('notes'::regclass, 2), 'UTF8'), 'hex')) FROM notes",
			want: "615c62,69745c27733b78,697427733b78,NULL,635c64",
		},
		{
			name: "clickhouse",
			database: func(c *qt.C) string {
				return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.ClickHouse), "DROP DATABASE IF EXISTS %s SYNC", renameInPath)
			},
			model: escapedLiteralModel("", ""),
			read: "SELECT concat(hex(assumeNotNull(a)), ',', hex(assumeNotNull(q)), ',', hex(assumeNotNull(r)), ',', " +
				"assumeNotNull(n)) FROM notes",
			want: "615C62,69745C27733B78,697427733B78,NULL",
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			database := row.database(c)

			created := applyModel(c, database, row.model)
			again := applyModel(c, database, row.model)

			c.Assert(created.ExitCode, qt.Equals, 0, historyComment(created))
			c.Assert(again.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n", historyComment(again))
			conn, err := dbschema.ConnectToDatabase(c.Context(), database)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)
			_, err = conn.ExecContext(c.Context(), "INSERT INTO notes (id) VALUES (1)")
			c.Assert(err, qt.IsNil)
			var got string
			c.Assert(conn.QueryRowContext(c.Context(), row.read).Scan(&got), qt.IsNil)
			c.Assert(got, qt.Equals, row.want)
		})
	}
}

// mysqlFamilyScratchURL creates an empty database on engine's server and
// returns the address that connects to it.
func mysqlFamilyScratchURL(c *qt.C, engine dbtarget.Engine, prefix string) string {
	c.Helper()
	scratch := newMySQLFamilyScratch(c, engine)
	name, _ := scratch.database(c, prefix)
	return replaceMySQLDatabaseName(c, scratch.adminURL, name)
}
