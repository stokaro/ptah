package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/schemadiff"
)

// A SQL schema declaring NCHAR converges with the catalog form the server
// reports after building that declaration.
func TestCompareNCharDeclaration_UsesTheCatalogForm(t *testing.T) {
	tests := []struct {
		dialect, held, collation string
	}{
		{"postgres", "character(5)", ""},
		{"yugabytedb", "character(5)", ""},
		{"mysql", "char(5)", "utf8mb3_general_ci"},
		{"mariadb", "char(5)", "utf8mb3_general_ci"},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			runtime := builtintest.Runtime()
			declared, _, err := sqlschema.ReadWith(c.Context(),
				[]byte("CREATE TABLE t (c nchar(5));"), test.dialect, nil,
				sqlschema.Options{Owners: runtime.SQL()})
			c.Assert(err, qt.IsNil)
			c.Assert(declared.Fields, qt.HasLen, 1)
			current, err := goschematodb.ToDBSchema(c.Context(), &declared, test.dialect, runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(current.Tables, qt.HasLen, 1)
			c.Assert(current.Tables[0].Columns, qt.HasLen, 1)
			column := &current.Tables[0].Columns[0]
			column.DataType, column.ColumnType = test.held, test.held
			column.IsNullable, column.Collate = "YES", test.collation
			diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), &declared, current,
				catalog.ServerInfo{Dialect: test.dialect}, nil, runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}
