package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/schemadiff"
)

// mysqlCharsetDefaults is the part of a MySQL 8.4 read's
// CHARACTER_SETS the tests below name.
var mysqlCharsetDefaults = map[string]string{
	"latin1":  "latin1_swedish_ci",
	"utf8mb3": "utf8mb3_general_ci",
	"utf8mb4": "utf8mb4_0900_ai_ci",
}

// heldCollations is the one-column table a read reports: its default collation
// and the collation its column c holds.
type heldCollations struct {
	table, column string
}

// compareInherited compares the one-table SQL declaration with the table a
// read reports after building before and setting the held collations,
// carrying defaults as the read's character set defaults.
func compareInherited(c *qt.C, dialect, declaration, before string, held heldCollations, defaults map[string]string) *difftypes.SchemaDiff {
	c.Helper()
	runtime := builtintest.Runtime()
	read := func(source string) schemamodel.Database {
		database, _, err := sqlschema.ReadWith(c.Context(), []byte(source), dialect, nil, sqlschema.Options{Owners: runtime.SQL()})
		c.Assert(err, qt.IsNil)
		return database
	}
	declared, built := read(declaration), read(before)
	current, err := goschematodb.ToDBSchema(c.Context(), &built, dialect, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(current.Tables, qt.HasLen, 1)
	table := &current.Tables[0]
	table.Collate = held.table
	for i := range table.Columns {
		if table.Columns[i].Name == "c" {
			table.Columns[i].IsNullable, table.Columns[i].EffectiveCollate = "YES", held.column
		}
	}
	current.CharsetDefaultCollations = defaults
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), &declared, current, catalog.ServerInfo{Dialect: dialect}, nil, runtime)
	c.Assert(err, qt.IsNil)
	return diff
}

// TestCompareInheritedCollation_ResetsAColumnThatHoldsAnother compares a
// column that declares no collation with the one it inherits: from the
// table's declared collation, from its declared character set, or from the
// character set the column names. One that holds another collation is reset,
// and the change names the collation it inherits.
func TestCompareInheritedCollation_ResetsAColumnThatHoldsAnother(t *testing.T) {
	tests := []struct {
		name        string
		declaration string
		held        heldCollations
		want        string
	}{
		{
			name:        "the table's collation",
			declaration: "CREATE TABLE t (id int PRIMARY KEY, c varchar(20)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;",
			held:        heldCollations{table: "utf8mb4_unicode_ci", column: "utf8mb4_bin"},
			want:        "utf8mb4_bin -> utf8mb4_unicode_ci",
		},
		{
			name:        "the default of the table's character set",
			declaration: "CREATE TABLE t (id int PRIMARY KEY, c varchar(20)) DEFAULT CHARSET=utf8mb4;",
			held:        heldCollations{table: "utf8mb4_0900_ai_ci", column: "utf8mb4_bin"},
			want:        "utf8mb4_bin -> utf8mb4_0900_ai_ci",
		},
		{
			name:        "the default of the column's character set",
			declaration: "CREATE TABLE t (id int PRIMARY KEY, c varchar(20) CHARACTER SET latin1) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;",
			held:        heldCollations{table: "utf8mb4_unicode_ci", column: "latin1_bin"},
			want:        "latin1_bin -> latin1_swedish_ci",
		},
		{
			name:        "the stored name of an aliased character set",
			declaration: "CREATE TABLE t (id int PRIMARY KEY, c varchar(20) CHARACTER SET utf8) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;",
			held:        heldCollations{table: "utf8mb4_unicode_ci", column: "utf8mb3_bin"},
			want:        "utf8mb3_bin -> utf8mb3_general_ci",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareInherited(c, "mysql", test.declaration,
				"CREATE TABLE t (id int PRIMARY KEY, c varchar(20));", test.held, mysqlCharsetDefaults)

			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].CollationChange, qt.IsNil)
			c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified[0].Changes, qt.DeepEquals, map[string]string{"collation": test.want})
			c.Assert(diff.TablesModified[0].ColumnsModified[0].Desired.Collate, qt.Equals, "")
		})
	}
}

// TestCompareInheritedCollation_LeavesAColumnThatHoldsIt is the control: a
// column that holds the collation it inherits, one whose character set the
// read does not report, an NCHAR column that holds the default of the national
// character set, a column the current side reports no collation for, and a
// column of a table that declares no default are not changed.
func TestCompareInheritedCollation_LeavesAColumnThatHoldsIt(t *testing.T) {
	tests := []struct {
		name        string
		declaration string
		held        heldCollations
		defaults    map[string]string
	}{
		{
			name:        "the column's character set",
			declaration: "CREATE TABLE t (id int PRIMARY KEY, c varchar(20) CHARACTER SET latin1) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;",
			held:        heldCollations{table: "utf8mb4_unicode_ci", column: "latin1_swedish_ci"},
			defaults:    mysqlCharsetDefaults,
		},
		{
			name:        "a read that reports no character set defaults",
			declaration: "CREATE TABLE t (id int PRIMARY KEY, c varchar(20) CHARACTER SET latin1) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;",
			held:        heldCollations{table: "utf8mb4_unicode_ci", column: "latin1_bin"},
		},
		{
			name:        "NCHAR",
			declaration: "CREATE TABLE t (id int PRIMARY KEY, c nchar(3)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;",
			held:        heldCollations{table: "utf8mb4_unicode_ci", column: "utf8mb3_general_ci"},
			defaults:    mysqlCharsetDefaults,
		},
		{
			name:        "a current side that holds no collation, as a projected document",
			declaration: "CREATE TABLE t (id int PRIMARY KEY, c varchar(20)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;",
			held:        heldCollations{table: "utf8mb4_unicode_ci"},
			defaults:    mysqlCharsetDefaults,
		},
		{
			name:        "a table that declares no default",
			declaration: "CREATE TABLE t (id int PRIMARY KEY, c varchar(20));",
			held:        heldCollations{table: "utf8mb4_unicode_ci", column: "utf8mb4_bin"},
			defaults:    mysqlCharsetDefaults,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareInherited(c, "mysql", test.declaration, test.declaration, test.held, test.defaults)

			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}

// TestCompareInheritedCollation_ChangesTheTableDefault compares a table's
// declared default collation with the one it holds. A changed one is a
// change of the table, and a column that takes the table's default and holds
// the old one is reset with it, on both engines of the family.
func TestCompareInheritedCollation_ChangesTheTableDefault(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			diff := compareInherited(c, dialect,
				"CREATE TABLE t (id int PRIMARY KEY, c varchar(20)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;",
				"CREATE TABLE t (id int PRIMARY KEY, c varchar(20)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;",
				heldCollations{table: "utf8mb4_bin", column: "utf8mb4_bin"}, mysqlCharsetDefaults)

			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].CollationChange, qt.DeepEquals,
				&difftypes.CollationChange{Current: "utf8mb4_bin", Desired: "utf8mb4_general_ci"})
			c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified[0].Changes, qt.DeepEquals,
				map[string]string{"collation": "utf8mb4_bin -> utf8mb4_general_ci"})
		})
	}
}

// TestCompareInheritedCollation_ChangesTheTableCharset compares a table whose
// declared default collation belongs to another character set. The table's
// change and the reset of its column carry both character sets, which the
// planner writes and safety rates. A column that names its own character set
// keeps it, and carries none.
func TestCompareInheritedCollation_ChangesTheTableCharset(t *testing.T) {
	c := qt.New(t)

	diff := compareInherited(c, "mysql",
		"CREATE TABLE t (id int PRIMARY KEY, c varchar(20)) DEFAULT CHARSET=latin1 COLLATE=latin1_bin;",
		"CREATE TABLE t (id int PRIMARY KEY, c varchar(20)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;",
		heldCollations{table: "utf8mb4_bin", column: "utf8mb4_bin"}, mysqlCharsetDefaults)

	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].CollationChange, qt.DeepEquals, &difftypes.CollationChange{Current: "utf8mb4_bin", Desired: "latin1_bin",
		Charset: &difftypes.CharsetChange{Current: "utf8mb4", Desired: "latin1"}})
	c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified[0].CharsetChange, qt.DeepEquals, &difftypes.CharsetChange{Current: "utf8mb4", Desired: "latin1"})
}

// TestCompareInheritedCollation_KeepsTheCharset is the control: a change of
// collation within one character set, and one whose character set the read
// does not report, carry no character set change.
func TestCompareInheritedCollation_KeepsTheCharset(t *testing.T) {
	tests := []struct {
		name     string
		defaults map[string]string
	}{
		{name: "a read that reports the character sets", defaults: mysqlCharsetDefaults},
		{name: "a read that reports none"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareInherited(c, "mysql",
				"CREATE TABLE t (id int PRIMARY KEY, c varchar(20)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;",
				"CREATE TABLE t (id int PRIMARY KEY, c varchar(20)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;",
				heldCollations{table: "utf8mb4_bin", column: "utf8mb4_bin"}, test.defaults)

			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].CollationChange.Charset, qt.IsNil)
			c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified[0].CharsetChange, qt.IsNil)
		})
	}
}

// TestCompareInheritedCollation_KeepsTheTableDefault is the control: a table
// that holds its declared default, written in another case, is not changed,
// and neither is one whose declaration names no default.
func TestCompareInheritedCollation_KeepsTheTableDefault(t *testing.T) {
	tests := []struct {
		name        string
		declaration string
	}{
		{name: "the same name", declaration: "CREATE TABLE t (id int PRIMARY KEY) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"},
		{name: "another case", declaration: "CREATE TABLE t (id int PRIMARY KEY) DEFAULT CHARSET=utf8mb4 COLLATE=UTF8MB4_BIN;"},
		{name: "no default", declaration: "CREATE TABLE t (id int PRIMARY KEY);"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := compareInherited(c, "mysql", test.declaration, "CREATE TABLE t (id int PRIMARY KEY);",
				heldCollations{table: "utf8mb4_bin"}, mysqlCharsetDefaults)

			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}
