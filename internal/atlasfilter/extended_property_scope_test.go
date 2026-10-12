package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mssql/mssqlproperty"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasfilter"
	"ptah.run/internal/featureselect"
	"ptah.run/migration/schemadiff"
)

// TestScope_AnExcludedTableTakesItsPropertiesOffBothSides pins that a scope
// excluding a table leaves its SQL Server extended properties out of the
// declaration as well as out of the database, so the comparison plans
// nothing for them.
func TestScope_AnExcludedTableTakesItsPropertiesOffBothSides(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	properties := []mssqlproperty.Property{
		{Schema: "dbo", Table: "orders", Name: "retention", Value: "90d"},
		{Schema: "dbo", Table: "lines", Name: "retention", Value: "30d"},
	}
	var observed, declared []schemaext.Object
	for _, property := range properties {
		observed = append(observed, mssqlproperty.ObservedObject(mssqlproperty.ObservedProperty{Property: property, ValueType: "nvarchar"}))
		declared = append(declared, mssqlproperty.DeclaredObject(mssqlproperty.DesiredProperty{Property: property}))
	}
	current := &catalog.Database{Schemas: []catalog.Schema{{Name: "dbo"}}, FeatureObjects: must.Must(schemaext.NewObjects(observed...))}
	desired := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(declared...))}
	for _, table := range []string{"orders", "lines"} {
		current.Tables = append(current.Tables, catalog.Table{Schema: "dbo", Name: table,
			Columns: []catalog.Column{{Name: "id", DataType: "int", ColumnType: "int", IsNullable: "NO", IsPrimaryKey: true}}})
		desired.Tables = append(desired.Tables, schemamodel.Table{StructName: table, Schema: "dbo", Name: table, PrimaryKey: []string{"id"}})
		desired.Fields = append(desired.Fields, schemamodel.Field{StructName: table, Name: "id", Type: "int", Primary: true})
	}
	schemamodel.Finalize(desired)
	bindings := must.Must(featureselect.CaptureBindings(c.Context(), runtime, "sqlserver", identifier.ForDialect("sqlserver"),
		featureselect.Side{Representation: schemaext.Desired, Objects: desired.FeatureObjects, Coverage: desired.FeatureCoverage},
		featureselect.Side{Representation: schemaext.Observed, Objects: current.FeatureObjects, Coverage: current.FeatureCoverage}))
	scope := atlasfilter.Scope{Exclude: []string{"orders"}, Bindings: bindings, Selections: runtime.Selections(), DefaultSchema: "dbo"}

	scopedCurrent := must.Must(atlasfilter.ScopeDatabase(current, scope))
	scopedDesired := must.Must(atlasfilter.ScopeGenerated(desired, scope))
	diff, err := schemadiff.CompareWithDialect(c.Context(), scopedDesired, scopedCurrent, "sqlserver", runtime)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
	c.Assert(scopedDesired.FeatureObjects.Refs(), qt.DeepEquals, []objectidentity.ID{properties[1].Ref()})
}

// TestScope_NamesAPropertyByItsType pins the SQL Server owner's selection
// through the bundled runtime: `extended_property` names a property in its
// schema, and a property on the database by its name alone, outside every
// schema.
func TestScope_NamesAPropertyByItsType(t *testing.T) {
	runtime := must.Must(builtin.New())
	properties := []mssqlproperty.Property{
		{Schema: "dbo", Name: "owner", Value: "a"},
		{Schema: "app", Name: "owner", Value: "b"},
		{Name: "release", Value: "c"},
	}
	var objects []schemaext.Object
	for _, property := range properties {
		objects = append(objects, mssqlproperty.ObservedObject(mssqlproperty.ObservedProperty{Property: property, ValueType: "nvarchar"}))
	}
	tests := []struct {
		name    string
		schemas []string
		include []string
		want    []objectidentity.ID
	}{
		{name: "a database property by name", include: []string{"release[type=extended_property]"}, want: []objectidentity.ID{properties[2].Ref()}},
		{name: "a schema property by name", include: []string{"app.owner[type=extended_property]"}, want: []objectidentity.ID{properties[1].Ref()}},
		{name: "one schema", schemas: []string{"app"}, want: []objectidentity.ID{properties[1].Ref(), properties[2].Ref()}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			current := &catalog.Database{Schemas: []catalog.Schema{{Name: "dbo"}, {Name: "app"}}, FeatureObjects: must.Must(schemaext.NewObjects(objects...))}

			scoped, err := atlasfilter.ScopeDatabase(current, atlasfilter.Scope{Schemas: test.schemas, Include: test.include,
				Selections: runtime.Selections(), DefaultSchema: "dbo"})

			c.Assert(err, qt.IsNil)
			c.Assert(scoped.FeatureObjects.Refs(), qt.ContentEquals, test.want)
		})
	}
}
