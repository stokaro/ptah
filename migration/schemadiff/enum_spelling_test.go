package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
)

// TestEnumDeclaredByName_ComparesAsWhatTheTargetStores pins the second spelling
// of an enum column.
//
// A column names its values two ways: inline on the field, or by naming an enum
// declared elsewhere. The renderer reads the second and the comparison read only
// the first, so a schema written as `//ptah:schema:enum` plus a column typed
// with that enum's name rendered as the target's inline model and compared as
// the enum's own name. Nothing converged.
//
// The live side of each case is what that dialect's renderer actually writes,
// so a case passing means a schema applied from this declaration compares equal
// to the database it produced.
func TestEnumDeclaredByName_ComparesAsWhatTheTargetStores(t *testing.T) {
	check := func(clause string) []catalog.Constraint {
		return []catalog.Constraint{{
			Name: "accounts_status_check", TableName: "accounts",
			Type: "CHECK", CheckClause: &clause,
		}}
	}

	tests := []struct {
		name        string
		dialect     string
		liveType    string
		constraints []catalog.Constraint
	}{
		{name: "sqlite stores TEXT and a check", dialect: platform.SQLite, liveType: "TEXT", constraints: check("status IN ('active', 'archived')")},
		{name: "oracle stores VARCHAR2 and a check", dialect: platform.Oracle, liveType: "VARCHAR2(255)", constraints: check("status IN ('active', 'archived')")},
		{name: "sqlserver stores NVARCHAR and a check", dialect: platform.SQLServer, liveType: "NVARCHAR(255)", constraints: check("[status] IN ('active', 'archived')")},
		{name: "mysql stores a native enum, and no check", dialect: platform.MySQL, liveType: "enum('active','archived')", constraints: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Account", Name: "accounts"}},
				Fields: []schemamodel.Field{
					// The declaration names the enum rather than listing its
					// values, which is what `//ptah:schema:enum` produces.
					{StructName: "Account", Name: "status", Type: "status_kind", Nullable: true},
				},
				Enums: []schemamodel.Enum{{Name: "status_kind", Values: []string{"active", "archived"}}},
			}
			live := &catalog.Database{
				Tables: []catalog.Table{{Name: "accounts", Columns: []catalog.Column{
					{Name: "status", DataType: tt.liveType, IsNullable: "YES"},
				}}},
				Constraints: tt.constraints,
			}

			diff := schemadiff.CompareWithDialect(desired, live, tt.dialect)

			c.Assert(diff.TablesModified, qt.HasLen, 0,
				qt.Commentf("a column declared by enum name must compare as what %s stores", tt.dialect))
		})
	}
}
