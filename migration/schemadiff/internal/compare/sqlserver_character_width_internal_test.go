package compare

// White-box testing required: SQL Server's portable type mapping and catalog
// type marker meet in a package-local comparison function.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
)

func TestPortableColumnTypeChange_KeepsSQLServerCharacterWidth(t *testing.T) {
	tests := []struct {
		name     string
		declared schemamodel.Field
		catalog  string
		want     string
	}{
		{name: "varchar to nvarchar", declared: schemamodel.Field{Type: "NVARCHAR(50)"}, catalog: "VARCHAR(50)", want: "VARCHAR(50) -> NVARCHAR(50)"},
		{name: "nvarchar to native varchar", declared: schemamodel.Field{Type: "VARCHAR(50)", TypeRawSQL: true}, catalog: "NVARCHAR(50)", want: "NVARCHAR(50) -> VARCHAR(50)"},
		{name: "portable varchar writes nvarchar", declared: schemamodel.Field{Type: "VARCHAR(50)"}, catalog: "NVARCHAR(50)"},
		{name: "same native varchar", declared: schemamodel.Field{Type: "VARCHAR(50)", TypeRawSQL: true}, catalog: "VARCHAR(50)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got := portableColumnTypeChange(test.declared, catalog.Column{TypeIsDeclaredText: true}, test.catalog, platform.SQLServer)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestTableColumnsWithDialect_KeepsCharacterWidthDifferences(t *testing.T) {
	tests := []struct {
		name, dialect, declared, held string
		raw                           bool
		wantChange                    bool
	}{
		{name: "SQL Server byte to Unicode", dialect: platform.SQLServer, declared: "NVARCHAR(50)", held: "VARCHAR(50)", wantChange: true},
		{name: "SQL Server Unicode to native byte", dialect: platform.SQLServer, declared: "VARCHAR(50)", held: "NVARCHAR(50)", raw: true, wantChange: true},
		{name: "SQL Server portable varchar", dialect: platform.SQLServer, declared: "VARCHAR(50)", held: "NVARCHAR(50)"},
		{name: "Oracle bytes to characters", dialect: platform.Oracle, declared: "VARCHAR2(100 CHAR)", held: "VARCHAR2(100)", wantChange: true},
		{name: "Oracle characters to bytes", dialect: platform.Oracle, declared: "VARCHAR2(100)", held: "VARCHAR2(100 CHAR)", wantChange: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{Fields: []schemamodel.Field{{
				StructName: "T", Name: "value", Type: test.declared, TypeRawSQL: test.raw, Nullable: true,
			}}}
			current := catalog.Table{Name: "t", Columns: []catalog.Column{{
				Name: "value", DataType: test.held, ColumnType: test.held, IsNullable: "YES",
				TypeIsDeclaredText: test.dialect == platform.SQLServer,
			}}}
			diff := TableColumnsWithDialect(schemamodel.Table{StructName: "T", Name: "t"}, current, desired, test.dialect)
			c.Assert(len(diff.ColumnsModified) > 0, qt.Equals, test.wantChange)
		})
	}
}
