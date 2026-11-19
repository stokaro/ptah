package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// catalogDefault is one declared column default against the form a catalog
// stores it in, with no server spelling, as on CockroachDB.
type catalogDefault struct {
	name     string
	typeName string
	declared string
	stored   string
}

// catalogDefaultDiff compares the column c of type test.typeName, declared
// with test.declared, with the same column holding test.stored on dialect.
func catalogDefaultDiff(test catalogDefault, dialect string) *difftypes.SchemaDiff {
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "bigint", Primary: true},
			{StructName: "T", Name: "c", Type: test.typeName, Nullable: true, DefaultExpr: test.declared},
		},
	}
	current := &catalog.Database{Tables: []catalog.Table{{Name: "t", Type: "BASE TABLE", Columns: []catalog.Column{
		{Name: "id", DataType: "bigint", UDTName: "int8", IsNullable: "NO", IsPrimaryKey: true},
		{Name: "c", DataType: test.typeName, UDTName: test.typeName, IsNullable: "YES", ColumnDefault: optional(test.stored)},
	}}}}
	diff := &difftypes.SchemaDiff{}
	compare.TablesAndColumnsWithTableContext(desired, current, diff, dialect,
		identifier.ForDialect(dialect), compare.CoverageOf(desired, current),
		compare.TableContext{}, capability.Capabilities{})
	return diff
}

// TestColumns_ADefaultInTheCatalogsFormIsNoChange holds each default
// CockroachDB v26.4 stores in a form of its own to the default it was declared
// as: it puts a negative number in parentheses, writes CURRENT_DATE as a call,
// and stores an array literal as an ARRAY constructor.
func TestColumns_ADefaultInTheCatalogsFormIsNoChange(t *testing.T) {
	tests := []catalogDefault{
		{name: "a negative integer", typeName: "bigint", declared: "-1", stored: "(-1)"},
		{name: "a negative decimal", typeName: "numeric", declared: "-2.5", stored: "(-2.5)"},
		{name: "a negative float", typeName: "double precision", declared: "-2.5", stored: "(-2.5)"},
		{name: "CURRENT_DATE", typeName: "date", declared: "CURRENT_DATE", stored: "current_date()"},
		{name: "an empty array literal", typeName: "text[]", declared: "'{}'", stored: "ARRAY[]"},
		{name: "an empty array cast to its type", typeName: "text[]", declared: "ARRAY[]::text[]", stored: "ARRAY[]"},
		{name: "an array literal of strings", typeName: "text[]", declared: "'{a,b}'", stored: "ARRAY['a', 'b']"},
		{name: "an array constructor spaced otherwise", typeName: "text[]", declared: "ARRAY['a','b']", stored: "ARRAY['a', 'b']"},
		{name: "an array literal of numbers", typeName: "bigint[]", declared: "'{1,2}'", stored: "ARRAY[1, 2]"},
		{name: "an element that needs quotes", typeName: "text[]", declared: `'{"a b",NULL}'`, stored: "ARRAY['a b', NULL]"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := catalogDefaultDiff(test, platform.CockroachDB)

			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}

// TestColumns_ADefaultInTheCatalogsFormWithAnotherValueIsAChange holds a
// default whose value differs from the stored one to a changed default, in
// each of the forms above.
func TestColumns_ADefaultInTheCatalogsFormWithAnotherValueIsAChange(t *testing.T) {
	tests := []catalogDefault{
		{name: "another negative integer", typeName: "bigint", declared: "-2", stored: "(-1)"},
		{name: "a date literal", typeName: "date", declared: "'2020-01-01'", stored: "current_date()"},
		{name: "an array with an element", typeName: "text[]", declared: "'{a}'", stored: "ARRAY[]"},
		{name: "another element", typeName: "text[]", declared: "ARRAY['a','c']", stored: "ARRAY['a', 'b']"},
		{name: "the string NULL against no value", typeName: "text[]", declared: `'{"NULL"}'`, stored: "ARRAY[NULL]"},
		{name: "an element cast to another type", typeName: "numeric[]", declared: "ARRAY['1.5'::integer]", stored: "ARRAY[1.5]"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := catalogDefaultDiff(test, platform.CockroachDB)

			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified[0].Changes, qt.DeepEquals,
				map[string]string{"default_expr": test.stored + " -> " + test.declared})
		})
	}
}
