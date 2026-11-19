//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// Each column defaults to the four-character string NULL, which MySQL reports
// in COLUMN_DEFAULT as the bare text NULL and MariaDB as 'NULL'.
const (
	nullStringDefaultSchema = "CREATE TABLE notes (\n" +
		"  id int PRIMARY KEY,\n" +
		"  a varchar(30) DEFAULT 'NULL',\n" +
		"  e enum('NULL','b') DEFAULT 'NULL'\n" +
		");\n"
	noStringDefaultSchema = "CREATE TABLE notes (\n" +
		"  id int PRIMARY KEY,\n" +
		"  a varchar(30),\n" +
		"  e enum('NULL','b')\n" +
		");\n"
)

// TestApplyKeepsAStringDefaultThatSpellsNULLE2E applies a schema without
// defaults and one whose defaults are the string NULL, in both directions. The
// second apply changes the defaults and a third plans nothing; read as no
// default, the string is written on every apply and never dropped.
func TestApplyKeepsAStringDefaultThatSpellsNULLE2E(t *testing.T) {
	rows := []struct {
		name          string
		engine        dbtarget.Engine
		first, second string
		want          string
	}{
		{name: "mysql, no default to the string", engine: dbtarget.MySQLAdmin, first: noStringDefaultSchema, second: nullStringDefaultSchema, want: "NULL,NULL"},
		{name: "mysql, the string to no default", engine: dbtarget.MySQLAdmin, first: nullStringDefaultSchema, second: noStringDefaultSchema, want: "-,-"},
		{name: "mariadb, no default to the string", engine: dbtarget.MariaDBAdmin, first: noStringDefaultSchema, second: nullStringDefaultSchema, want: "NULL,NULL"},
		{name: "mariadb, the string to no default", engine: dbtarget.MariaDBAdmin, first: nullStringDefaultSchema, second: noStringDefaultSchema, want: "-,-"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, row.engine)
			name, _ := scratch.database(c, "null_string_default")
			database := replaceMySQLDatabaseName(c, scratch.adminURL, name)

			got := applyBothDefaultSchemas(c, database, row.first, row.second,
				"SELECT CONCAT_WS(',', COALESCE(a, '-'), COALESCE(e, '-')) FROM notes")

			c.Assert(got, qt.Equals, row.want)
		})
	}
}

// One schema defaults the column to the empty string, the other declares no
// default.
const (
	emptyStringDefaultSchema = "CREATE TABLE notes (\n" +
		"  id int PRIMARY KEY,\n" +
		"  a varchar(30) DEFAULT ''\n" +
		");\n"
	noEmptyStringDefaultSchema = "CREATE TABLE notes (\n" +
		"  id int PRIMARY KEY,\n" +
		"  a varchar(30)\n" +
		");\n"
)

// TestApplyTellsAnEmptyStringFromNoDefaultE2E applies a schema without a
// default and one whose default is the empty string, in both directions. The
// second apply changes the default and a third plans nothing; compared with
// its quotes taken off, the empty string is no default and the second apply
// says the schema is synced.
func TestApplyTellsAnEmptyStringFromNoDefaultE2E(t *testing.T) {
	rows := []struct {
		name          string
		engine        dbtarget.Engine
		first, second string
		want          string
	}{
		{name: "mysql, no default to the empty string", engine: dbtarget.MySQLAdmin, first: noEmptyStringDefaultSchema, second: emptyStringDefaultSchema, want: ""},
		{name: "mysql, the empty string to no default", engine: dbtarget.MySQLAdmin, first: emptyStringDefaultSchema, second: noEmptyStringDefaultSchema, want: "-"},
		{name: "mariadb, no default to the empty string", engine: dbtarget.MariaDBAdmin, first: noEmptyStringDefaultSchema, second: emptyStringDefaultSchema, want: ""},
		{name: "mariadb, the empty string to no default", engine: dbtarget.MariaDBAdmin, first: emptyStringDefaultSchema, second: noEmptyStringDefaultSchema, want: "-"},
		{name: "postgres, no default to the empty string", engine: dbtarget.PostgreSQL, first: noEmptyStringDefaultSchema, second: emptyStringDefaultSchema, want: ""},
		{name: "postgres, the empty string to no default", engine: dbtarget.PostgreSQL, first: emptyStringDefaultSchema, second: noEmptyStringDefaultSchema, want: "-"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			database := emptyStringDatabase(c, row.engine)

			got := applyBothDefaultSchemas(c, database, row.first, row.second, "SELECT COALESCE(a, '-') FROM notes")

			c.Assert(got, qt.Equals, row.want)
		})
	}
}

// emptyStringDatabase returns the address of an empty scratch database on
// engine.
func emptyStringDatabase(c *qt.C, engine dbtarget.Engine) string {
	c.Helper()
	if engine == dbtarget.PostgreSQL {
		return postgresScratchDevURL(c, "empty_string_default")
	}
	scratch := newMySQLFamilyScratch(c, engine)
	name, _ := scratch.database(c, "empty_string_default")
	return replaceMySQLDatabaseName(c, scratch.adminURL, name)
}
