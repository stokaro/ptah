package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlproperty"
	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlschema"
)

// TestMSSQLPropertiesGoWithADroppedTable reads a DROP TABLE against an earlier
// document whose tables carry SQL Server extended properties, through the
// bundled runtime. The properties on the dropped table and on its column go
// with it, as SQL Server drops them; the ones on the other table and on the
// schema stay.
func TestMSSQLPropertiesGoWithADroppedTable(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	opts := sqlschema.Options{Owners: runtime.SQL(), Vocabulary: runtime.CoverageVocabulary(), Relations: runtime}
	earlier, _, err := sqlschema.ReadWith(c.Context(), []byte("CREATE TABLE dbo.a (id int);\nCREATE TABLE dbo.b (id int);\n"),
		platform.SQLServer, nil, opts)
	c.Assert(err, qt.IsNil)
	properties := []mssqlproperty.Property{
		{Schema: "dbo", Table: "a", Name: "on_a", Value: "x"},
		{Schema: "dbo", Table: "a", Column: "id", Name: "on_a_id", Value: "x"},
		{Schema: "dbo", Table: "b", Name: "on_b", Value: "x"},
		{Schema: "dbo", Name: "on_dbo", Value: "x"},
	}
	var objects []schemaext.Object
	for _, property := range properties {
		objects = append(objects, mssqlproperty.DeclaredObject(mssqlproperty.DesiredProperty{Property: property}))
	}
	earlier.FeatureObjects = must.Must(earlier.FeatureObjects.Merge(must.Must(schemaext.NewObjects(objects...))))

	_, _, err = sqlschema.ReadWith(c.Context(), []byte("DROP TABLE dbo.a;\n"), platform.SQLServer, sqlschema.NewDocument(&earlier), opts)

	c.Assert(err, qt.IsNil)
	c.Assert(earlier.Tables, qt.HasLen, 1)
	c.Assert(earlier.FeatureObjects.Refs(), qt.ContentEquals, []objectidentity.ID{properties[2].Ref(), properties[3].Ref()})
}
