package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// A national character declaration must match the column the server builds
// from it, while a different width or character set remains a type change.
func TestTableColumns_NationalCharacterAlias(t *testing.T) {
	tests := []struct {
		name, dialect, declared, held, collation string
		change                                   bool
	}{
		{"PostgreSQL alias", "postgres", "nchar(5)", "character(5)", "", false},
		{"YugabyteDB alias", "yugabytedb", "nchar(5)", "character(5)", "", false},
		{"MySQL national charset", "mysql", "nchar(5)", "char(5)", "utf8mb3_general_ci", false},
		{"MariaDB national charset", "mariadb", "nchar(5)", "char(5)", "utf8mb3_general_ci", false},
		{"MariaDB utf8 alias", "mariadb", "nchar(5)", "char(5)", "utf8_general_ci", false},
		{"PostgreSQL bare national char", "postgres", "nchar", "character(1)", "", false},
		{"PostgreSQL changed width", "postgres", "nchar(5)", "character(6)", "", true},
		{"MySQL changed charset", "mysql", "nchar(5)", "char(5)", "latin1_swedish_ci", true},
		{"MySQL four-byte charset", "mysql", "nchar(5)", "char(5)", "utf8mb4_0900_ai_ci", true},
		{"MySQL unknown charset", "mysql", "nchar(5)", "char(5)", "", true},
		{"SQL Server distinct national type", "sqlserver", "nchar(5)", "char(5)", "", true},
		{"Oracle distinct national type", "oracle", "nchar(5)", "char(5)", "", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{Fields: []schemamodel.Field{{
				StructName: "T", Name: "c", Type: test.declared, Nullable: true,
			}}}
			current := catalog.Table{Name: "t", Columns: []catalog.Column{{
				Name: "c", DataType: test.held, ColumnType: test.held,
				IsNullable: "YES", Collate: test.collation, TypeIsDeclaredText: test.dialect == "sqlserver",
			}}}
			diff := compare.TableColumnsWithDialect(schemamodel.Table{StructName: "T", Name: "t"}, current, desired, test.dialect)
			c.Assert(len(diff.ColumnsModified) > 0, qt.Equals, test.change)
		})
	}
}
