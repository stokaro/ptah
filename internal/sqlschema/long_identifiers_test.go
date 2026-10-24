package sqlschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/sqlschema"
)

// longNames is a table, a column, a foreign key and an index, each named past
// 63 bytes. The column ends in a two-byte character that straddles the limit.
func longNames() (table, column, key, index, sql string) {
	table = strings.Repeat("t", 70)
	column = strings.Repeat("c", 62) + "é"
	key = strings.Repeat("k", 70)
	index = strings.Repeat("i", 70)
	sql = "CREATE TABLE p (id int PRIMARY KEY);\n" +
		`CREATE TABLE "` + table + `" ("` + column + `" int, CONSTRAINT "` + key + `" FOREIGN KEY ("` + column + `") REFERENCES p (id));` + "\n" +
		`CREATE INDEX "` + index + `" ON "` + table + `" ("` + column + `");` + "\n"
	return table, column, key, index, sql
}

// readNames returns the table, column, foreign key and index names the reader
// recorded for the long-named table.
func readNames(c *qt.C, dialect, sql string) []string {
	c.Helper()
	db, _, err := sqlschema.Read([]byte(sql), dialect)
	c.Assert(err, qt.IsNil)
	var names []string
	for _, table := range db.Tables {
		if table.Name != "p" {
			names = append(names, table.Name)
		}
	}
	for _, field := range db.Fields {
		if field.Name != "id" {
			names = append(names, field.Name)
		}
	}
	for _, constraint := range db.Constraints {
		if constraint.Type == "FOREIGN KEY" {
			names = append(names, constraint.Name)
		}
	}
	for _, field := range db.Fields {
		if field.ForeignKeyName != "" {
			names = append(names, field.ForeignKeyName)
		}
	}
	for _, index := range db.Indexes {
		if index.Name != "" && !strings.HasSuffix(index.Name, "_pkey") {
			names = append(names, index.Name)
		}
	}
	return names
}

// TestRead_LongIdentifiers_CutAsTheServerCutsThem reads names past the limit
// on a server that cuts them. Run on PostgreSQL 15.19, the file creates the
// table, foreign key and index under their first 63 bytes, and the column
// under 62, because the character at the limit is two bytes long. Ent's schema
// export writes such names, and read whole they were refused as over the
// limit.
func TestRead_LongIdentifiers_CutAsTheServerCutsThem(t *testing.T) {
	c := qt.New(t)
	table, column, key, index, sql := longNames()

	names := readNames(c, platform.Postgres, sql)

	for _, want := range []string{table[:63], column[:62], key[:63], index[:63]} {
		c.Assert(names, qt.Contains, want)
	}
	for _, name := range names {
		c.Assert(len(name) <= 63, qt.IsTrue, qt.Commentf("%q", name))
	}
}

// TestRead_LongIdentifiers_KeptWholeWhereTheServerKeepsThem is the control:
// CockroachDB v26.1.8 keeps a 70-byte table, constraint and index name whole.
func TestRead_LongIdentifiers_KeptWholeWhereTheServerKeepsThem(t *testing.T) {
	c := qt.New(t)
	table, _, key, index, sql := longNames()

	names := readNames(c, platform.CockroachDB, sql)

	for _, want := range []string{table, key, index} {
		c.Assert(names, qt.Contains, want)
	}
}
