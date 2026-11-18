package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestColumns_ADefaultIsNormalizedUnderOneType pins that a default is compared
// in the terms of the type the column will have, not one in each.
//
// A default's meaning depends on the column's type -- `0` is `false` on a
// boolean and `0` on an integer -- so normalizing each side under its own type
// could answer "changed" for two spellings of one value. On SQLite a hand-made
// `b NUMERIC DEFAULT 0` compared against a declared `boolean`, whose rendered
// type is `integer`, would report `default_expr: 0 -> 0` beside the type change
// that is real (stokaro/ptah#2041).
func TestColumns_ADefaultIsNormalizedUnderOneType(t *testing.T) {
	tests := []struct {
		name    string
		genCol  schemamodel.Field
		dbCol   catalog.Column
		want    map[string]string
		dialect string
	}{
		{
			// The row the issue is about. The type change is real and stays;
			// the default did not change and must not be reported.
			name:    "a boolean column whose declaration renders as integer",
			dialect: platform.SQLite,
			genCol:  schemamodel.Field{Name: "b", Type: "boolean", Nullable: true, DefaultExpr: "0"},
			dbCol:   catalog.Column{Name: "b", DataType: "NUMERIC", IsNullable: "YES", ColumnDefault: new("0")},
			want:    map[string]string{"type": "NUMERIC -> boolean"},
		},
		{
			// The control that keeps the fix from being a blanket silence: a
			// default that really did change is still reported.
			name:    "a default that changed",
			dialect: platform.SQLite,
			genCol:  schemamodel.Field{Name: "b", Type: "boolean", Nullable: true, DefaultExpr: "1"},
			dbCol:   catalog.Column{Name: "b", DataType: "NUMERIC", IsNullable: "YES", ColumnDefault: new("0")},
			want:    map[string]string{"type": "NUMERIC -> boolean", "default_expr": "0 -> 1"},
		},
		{
			// The other control: with the types agreeing, boolean
			// normalization still folds the two spellings of true.
			name:    "one value spelled two ways under one type",
			dialect: platform.Postgres,
			genCol:  schemamodel.Field{Name: "b", Type: "boolean", Nullable: true, DefaultExpr: "true"},
			dbCol:   catalog.Column{Name: "b", DataType: "boolean", IsNullable: "YES", ColumnDefault: new("1")},
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

// TestColumns_AParenthesizedExpressionDefaultIsTheExpression pins that the
// parentheses around a whole expression default are not part of it. SQLite
// reports `DEFAULT (datetime('now'))` as `datetime('now')`, and a comparison
// that kept them reported the table it built as changed on every run.
func TestColumns_AParenthesizedExpressionDefaultIsTheExpression(t *testing.T) {
	tests := []struct {
		name     string
		genCol   schemamodel.Field
		reported string
		want     map[string]string
	}{
		{
			name:     "a call",
			genCol:   schemamodel.Field{Name: "d", Type: "text", Nullable: true, DefaultExpr: "(datetime('now'))"},
			reported: "datetime('now')",
			want:     make(map[string]string),
		},
		{
			name:     "an operator",
			genCol:   schemamodel.Field{Name: "n", Type: "integer", Nullable: true, DefaultExpr: "(5 - 2)"},
			reported: "5 - 2",
			want:     make(map[string]string),
		},
		{
			name:     "a concatenation",
			genCol:   schemamodel.Field{Name: "s", Type: "text", Nullable: true, DefaultExpr: "('a' || 'b')"},
			reported: "'a' || 'b'",
			want:     make(map[string]string),
		},
		{
			name:     "two groups",
			genCol:   schemamodel.Field{Name: "n", Type: "integer", Nullable: true, DefaultExpr: "((0))"},
			reported: "0",
			want:     make(map[string]string),
		},
		{
			// Two groups side by side are not one group around the whole.
			name:     "groups that do not enclose the whole",
			genCol:   schemamodel.Field{Name: "n", Type: "integer", Nullable: true, DefaultExpr: "(5) - (2)"},
			reported: "5 - 2",
			want:     map[string]string{"default_expr": "5 - 2 -> (5) - (2)"},
		},
		{
			name:     "an expression that changed",
			genCol:   schemamodel.Field{Name: "n", Type: "integer", Nullable: true, DefaultExpr: "(5 - 3)"},
			reported: "5 - 2",
			want:     map[string]string{"default_expr": "5 - 2 -> (5 - 3)"},
		},
		{
			// A literal is a value, and its text is the value.
			name:     "a string literal",
			genCol:   schemamodel.Field{Name: "s", Type: "text", Nullable: true, Default: "(x)", DefaultSet: true},
			reported: "'x'",
			want:     map[string]string{"default": "'x' -> (x)"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbCol := catalog.Column{Name: test.genCol.Name, DataType: test.genCol.Type, IsNullable: "YES", ColumnDefault: new(test.reported)}

			result := compare.ColumnsWithDialect(test.genCol, dbCol, platform.SQLite)

			c.Assert(result.Changes, qt.DeepEquals, test.want)
		})
	}
}
