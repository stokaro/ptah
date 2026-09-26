package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

func qualifiedTypeDiff(declaredType, reportedType, reportedSchema string) *difftypes.SchemaDiff {
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "integer", Primary: true},
			{StructName: "T", Name: "m", Type: declaredType, Nullable: true},
		},
	}
	current := &catalog.Database{Tables: []catalog.Table{{Name: "t", Type: "BASE TABLE", Columns: []catalog.Column{
		{Name: "id", DataType: "integer", UDTName: "int4", IsNullable: "NO", IsPrimaryKey: true},
		{Name: "m", DataType: reportedType, UDTName: reportedType, UDTSchema: reportedSchema, IsNullable: "YES"},
	}}}}
	diff := &difftypes.SchemaDiff{}
	compare.TablesAndColumnsWithSemantics(desired, current, diff, "postgres",
		identifier.ForDialect("postgres"), compare.CoverageOf(desired, current))
	return diff
}

// A type written with its schema or in quotes names the type the catalog
// reports without them (stokaro/ptah#3620).
func TestQualifiedTypeName_NamesTheReportedType(t *testing.T) {
	tests := []struct {
		name, declared, reported, reportedSchema string
	}{
		{name: "the catalog's bare name and its schema", declared: `"s".mood`, reported: "mood", reportedSchema: "s"},
		{name: "quoted parts", declared: `"s"."mood"`, reported: "s.mood"},
		{name: "an array", declared: `"s_types".mood[]`, reported: "s_types.mood[]"},
		{name: "unquoted case folds", declared: "S.Mood", reported: "mood", reportedSchema: "s"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := qualifiedTypeDiff(test.declared, test.reported, test.reportedSchema)

			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}

// The control: two schemas, two names or two array depths are two types.
func TestQualifiedTypeName_ADifferentTypeStillPlans(t *testing.T) {
	tests := []struct {
		name, declared, reported, reportedSchema string
	}{
		{name: "another schema", declared: "a.mood", reported: "b.mood"},
		{name: "another schema, from udt_schema", declared: "a.mood", reported: "mood", reportedSchema: "b"},
		{name: "a schema nothing reports", declared: "a.mood", reported: "mood"},
		{name: "another name", declared: "s.mood", reported: "s.feeling"},
		{name: "an array against its element", declared: "s.mood[]", reported: "s.mood"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := qualifiedTypeDiff(test.declared, test.reported, test.reportedSchema)

			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified[0].Changes["type"], qt.Not(qt.Equals), "")
		})
	}
}
