package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
)

// uniqueExpressionSchema is one table whose second column declares uniqueness
// the way the caller asks for it.
//
// The table declares a primary key because ClickHouse refuses a MergeTree table
// without one, and that refusal would answer every row for a reason that has
// nothing to do with the column.
func uniqueExpressionSchema(unique bool, expression string) schemamodel.Database {
	return schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{
			{StructName: "T", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true},
			{
				StructName: "T", FieldName: "S", Name: "s", Type: "VARCHAR(32)", Nullable: true,
				Unique: unique, UniqueExpr: expression,
			},
		},
	}
}

// TestUniqueExpression_EveryTargetRefusesIt is stokaro/ptah#2611.
//
// `unique_expr` is an attribute internal/annotationmeta documents and no
// renderer read. Measured on PostgreSQL before this change: a column carrying
// only `unique_expr` rendered with no uniqueness at all, and one carrying
// `unique` beside it rendered `UNIQUE` on the raw column — uniqueness over `s`
// where the author asked for uniqueness over `lower(s)`. A different constraint
// is worse than a missing one, because the schema looks applied.
//
// Both rows matter. The first is the silent drop; the second is the silent
// substitution, and a refusal that only covered the first would leave it.
func TestUniqueExpression_EveryTargetRefusesIt(t *testing.T) {
	tests := []struct {
		name   string
		unique bool
	}{
		{name: "the expression alone", unique: false},
		{name: "the expression beside a column UNIQUE", unique: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, dialect := range capability.DefaultDialects() {
				t.Run(dialect, func(t *testing.T) {
					c := qt.New(t)

					schema := uniqueExpressionSchema(test.unique, "lower(s)")
					statements, err := renderer.GetOrderedCreateStatements(&schema, dialect)

					c.Assert(err, qt.IsNotNil)
					c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
					c.Assert(err.Error(), qt.Contains, `column "s" declares unique_expr "lower(s)"`)
					c.Assert(statements, qt.IsNil)
				})
			}
		})
	}
}

// TestUniqueExpression_AColumnWithoutOneStillRenders is the acceptance control:
// a refusal that fired for every column would pass the test above.
//
// The `unique` row is the one that matters, because it is the declaration the
// refusal has to leave alone — ordinary column uniqueness, with no expression.
func TestUniqueExpression_AColumnWithoutOneStillRenders(t *testing.T) {
	tests := []struct {
		name   string
		unique bool
	}{
		{name: "no uniqueness at all", unique: false},
		{name: "ordinary column uniqueness", unique: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			schema := uniqueExpressionSchema(test.unique, "")
			statements, err := renderer.GetOrderedCreateStatements(&schema, "postgres")

			c.Assert(err, qt.IsNil)
			c.Assert(len(statements) > 0, qt.IsTrue)
		})
	}
}

// TestUniqueExpression_TheRefusalIsAboutTheExpressionRatherThanTheColumn pins
// what the old behavior actually produced, so the reason for refusing rather
// than rendering stays legible: the column's own UNIQUE is a different
// constraint, and it is the one that used to be emitted.
func TestUniqueExpression_TheRefusalIsAboutTheExpressionRatherThanTheColumn(t *testing.T) {
	c := qt.New(t)

	schema := uniqueExpressionSchema(true, "")
	statements, err := renderer.GetOrderedCreateStatements(&schema, "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, `"s" VARCHAR(32) UNIQUE`)
}
