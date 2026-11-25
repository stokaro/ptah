package mysql

// White-box testing required: what MySQL answers for the character set of a
// binary ENUM or SET column is a difference in the CATALOG, visible only where
// readColumnsByTable turns a row into a column, as the rows in
// column_default_internal_test.go are. The rows scripted here are the ones the
// engines report, so the result is measured against them rather than against
// an internal shape.

import (
	"database/sql/driver"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dialect/mysql/mysqlschema"
)

// TestReadColumns_ReadsABinaryEnumOrSet reads the character set and the
// collation of a column from the row each engine reports for it. Measured on
// MySQL 8.4 and MariaDB 11.8 for ENUM('ok ','x') CHARACTER SET binary and
// SET('m ','n') CHARACTER SET binary: MySQL reports NULL for both, MariaDB
// binary. The other rows are the controls that keep a NULL from being read
// as binary for every column.
func TestReadColumns_ReadsABinaryEnumOrSet(t *testing.T) {
	tests := []struct {
		name, columnType         string
		charset, collation       driver.Value
		wantCharset, wantCollate string
	}{
		{name: "MySQL's binary ENUM", columnType: "enum('ok ','x')", wantCharset: "binary", wantCollate: "binary"},
		{name: "MySQL's binary SET", columnType: "set('m ','n')", wantCharset: "binary", wantCollate: "binary"},
		{
			name: "MariaDB's binary ENUM", columnType: "enum('ok ','x')", charset: "binary", collation: "binary",
			wantCharset: "binary", wantCollate: "binary",
		},
		{
			name: "an ENUM of another character set", columnType: "enum('ok','x')",
			charset: "utf8mb4", collation: "utf8mb4_bin", wantCharset: "utf8mb4", wantCollate: "utf8mb4_bin",
		},
		{name: "a binary type, which has no character set", columnType: "varbinary(3)"},
		{name: "a number", columnType: "int"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			row := []driver.Value{
				"c", "b", test.columnType, test.columnType, "YES",
				nil, nil, nil, nil, int64(1),
				test.charset, test.collation, "", nil, "",
			}
			reader := NewMySQLReader(columnsDB(c, [][]driver.Value{row}).SQL, "app")

			columnsByTable, err := reader.readColumnsByTable(t.Context(), "app", literalsUnquoted, expressionsEscaped)

			c.Assert(err, qt.IsNil)
			c.Assert(columnsByTable["c"], qt.HasLen, 1)
			settings, _, err := mysqlschema.Settings(columnsByTable["c"][0].Facets)
			c.Assert(err, qt.IsNil)
			c.Assert(settings.Charset, qt.Equals, test.wantCharset)
			c.Assert(columnsByTable["c"][0].Collate, qt.Equals, test.wantCollate)
		})
	}
}
