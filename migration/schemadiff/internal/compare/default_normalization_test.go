package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
)

// TestColumns_ADefaultIsNormalizedUnderOneType pins that a default is compared
// in the terms of the type the column will have, not one in each.
//
// A default's meaning depends on the column's type -- `0` is `false` on a
// boolean and `0` on an integer -- so normalizing each side under its own type
// could answer "changed" for two spellings of one value. Measured on SQLite: a
// hand-made `b BOOLEAN DEFAULT 0` compared against Ptah's own description of
// it, whose rendered type is `integer`, reported `default_expr: 0 -> 0` beside
// the type change that was real (stokaro/ptah#2041).
func TestColumns_ADefaultIsNormalizedUnderOneType(t *testing.T) {
	tests := []struct {
		name    string
		genCol  goschema.Field
		dbCol   types.DBColumn
		want    map[string]string
		dialect string
	}{
		{
			// The row the issue is about. The type change is real and stays;
			// the default did not change and must not be reported.
			name:    "a boolean column whose declaration renders as integer",
			dialect: platform.SQLite,
			genCol:  goschema.Field{Name: "b", Type: "boolean", Nullable: true, DefaultExpr: "0"},
			dbCol:   types.DBColumn{Name: "b", DataType: "BOOLEAN", IsNullable: "YES", ColumnDefault: new("0")},
			want:    map[string]string{"type": "boolean -> integer"},
		},
		{
			// The control that keeps the fix from being a blanket silence: a
			// default that really did change is still reported.
			name:    "a default that changed",
			dialect: platform.SQLite,
			genCol:  goschema.Field{Name: "b", Type: "boolean", Nullable: true, DefaultExpr: "1"},
			dbCol:   types.DBColumn{Name: "b", DataType: "BOOLEAN", IsNullable: "YES", ColumnDefault: new("0")},
			want:    map[string]string{"type": "boolean -> integer", "default_expr": "0 -> 1"},
		},
		{
			// The other control: with the types agreeing, boolean
			// normalization still folds the two spellings of true.
			name:    "one value spelled two ways under one type",
			dialect: platform.Postgres,
			genCol:  goschema.Field{Name: "b", Type: "boolean", Nullable: true, DefaultExpr: "true"},
			dbCol:   types.DBColumn{Name: "b", DataType: "boolean", IsNullable: "YES", ColumnDefault: new("1")},
			want:    make(map[string]string),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result := compare.ColumnsWithDialect(test.genCol, test.dbCol, test.dialect)

			c.Assert(result.Changes, qt.DeepEquals, test.want)
		})
	}
}
