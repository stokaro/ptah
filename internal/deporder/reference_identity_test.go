package deporder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/deporder"
)

func TestTablesForCreate_PreservesStructuralIdentity(t *testing.T) {
	c := qt.New(t)
	schema := referenceCollisionSchema()

	tables := deporder.TablesForCreate(schema, []string{"tenant.data", `"tenant.data"`})

	c.Assert(tables, qt.HasLen, 2)
	c.Assert(tables[0].StructName, qt.Equals, "Qualified")
	c.Assert(tables[1].StructName, qt.Equals, "Literal")
}

func TestGeneratedTableDependencies_PreservesStructuralIdentity(t *testing.T) {
	c := qt.New(t)
	schema := referenceCollisionSchema()
	schema.Tables = append(schema.Tables, schemamodel.Table{StructName: "Child", Name: "children"})
	schema.Fields = []schemamodel.Field{
		{StructName: "Child", Name: "literal_id", Type: "INTEGER", Foreign: `"tenant.data"(id)`},
		{StructName: "Child", Name: "qualified_id", Type: "INTEGER", Foreign: "tenant.data(id)"},
	}

	dependencies := deporder.GeneratedTableDependencies(schema)

	c.Assert(dependencies["children"], qt.DeepEquals, []string{`"tenant.data"`, "tenant.data"})
}

func referenceCollisionSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Literal", Name: "tenant.data"},
			{StructName: "Qualified", Schema: "tenant", Name: "data"},
		},
	}
}
