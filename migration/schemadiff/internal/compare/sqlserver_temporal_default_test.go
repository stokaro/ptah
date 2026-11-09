package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// SQL Server preserves a default's function but may change its case when it
// reads the constraint. GETDATE and CURRENT_TIMESTAMP are equivalent; the
// higher-precision SYSDATETIME remains a different default.
func TestColumns_SQLServerTemporalDefaultConverges(t *testing.T) {
	tests := []struct {
		name, declared, held string
		change               bool
	}{
		{"GETDATE case", "GETDATE()", "getdate()", false},
		{"CURRENT_TIMESTAMP case", "CURRENT_TIMESTAMP", "current_timestamp", false},
		{"SYSDATETIME case", "SYSDATETIME()", "sysdatetime()", false},
		{"ANSI synonym", "CURRENT_TIMESTAMP", "getdate()", false},
		{"precision changes", "SYSDATETIME()", "getdate()", true},
		{"precision changes back", "GETDATE()", "sysdatetime()", true},
		{"literal is not a function", "GETDATE()", "'getdate()'", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result := compare.ColumnsWithDialect(
				schemamodel.Field{Name: "stamp", Type: "DATETIME2", Nullable: true, DefaultExpr: test.declared},
				catalog.Column{Name: "stamp", DataType: "DATETIME2", ColumnType: "DATETIME2",
					TypeIsDeclaredText: true, IsNullable: "YES", ColumnDefault: &test.held},
				platform.SQLServer)
			c.Assert(len(result.Changes) > 0, qt.Equals, test.change)
		})
	}
}

func TestColumns_SQLServerTemporalDefaultDoesNotMatchText(t *testing.T) {
	c := qt.New(t)
	result := compare.ColumnsWithDialect(
		schemamodel.Field{Name: "stamp", Type: "DATETIME2", Nullable: true, Default: "GETDATE()"},
		catalog.Column{Name: "stamp", DataType: "DATETIME2", ColumnType: "DATETIME2",
			TypeIsDeclaredText: true, IsNullable: "YES", ColumnDefault: new("getdate()")},
		platform.SQLServer)
	c.Assert(result.Changes["default_expr"], qt.Not(qt.Equals), "")
}
