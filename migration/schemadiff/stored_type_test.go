package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestCompareStoredType_AcceptsTheBinaryType compares a character column of
// the binary character set with the binary type MySQL and MariaDB store it
// as, which a read reports, and with the declared type, which a side
// projected from a schema file holds. Neither is a change.
func TestCompareStoredType_AcceptsTheBinaryType(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range []struct {
			name, declared, current string
		}{
			{name: "varchar read back", declared: "varchar(10)", current: "varbinary(10)"},
			{name: "char read back", declared: "char(4)", current: "binary(4)"},
			{name: "text read back", declared: "text", current: "blob"},
			{name: "projected from a file", declared: "varchar(10)", current: "varchar(10) CHARACTER SET binary"},
		} {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				declaration := "CREATE TABLE t (id int PRIMARY KEY, c " + test.declared + " CHARACTER SET binary NULL) DEFAULT CHARSET=utf8mb4;"
				before := "CREATE TABLE t (id int PRIMARY KEY, c " + test.current + " NULL) DEFAULT CHARSET=utf8mb4;"

				diff := compareInherited(c, dialect, declaration, before, heldCollations{table: "utf8mb4_0900_ai_ci"}, mysqlCharsetDefaults)

				c.Assert(diff.TablesModified, qt.HasLen, 0)
			})
		}
	}
}

// TestCompareStoredType_ReportsAChangedLength is the control: a binary
// column of another length than the declared one is a type change.
func TestCompareStoredType_ReportsAChangedLength(t *testing.T) {
	c := qt.New(t)

	diff := compareInherited(c, "mysql",
		"CREATE TABLE t (id int PRIMARY KEY, c varchar(20) CHARACTER SET binary NULL) DEFAULT CHARSET=utf8mb4;",
		"CREATE TABLE t (id int PRIMARY KEY, c varbinary(10) NULL) DEFAULT CHARSET=utf8mb4;",
		heldCollations{table: "utf8mb4_0900_ai_ci"}, mysqlCharsetDefaults)

	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified[0].ColumnName, qt.Equals, "c")
	c.Assert(diff.TablesModified[0].ColumnsModified[0].Changes["type"], qt.Matches, `varbinary\(10\) -> varchar.*`)
}
