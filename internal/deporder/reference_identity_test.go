package deporder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/internal/deporder"
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
	schema.Tables = append(schema.Tables, goschema.Table{StructName: "Child", Name: "children"})
	schema.Fields = []goschema.Field{
		{StructName: "Child", Name: "literal_id", Type: "INTEGER", Foreign: `"tenant.data"(id)`},
		{StructName: "Child", Name: "qualified_id", Type: "INTEGER", Foreign: "tenant.data(id)"},
	}

	dependencies := deporder.GeneratedTableDependencies(schema)

	c.Assert(dependencies["children"], qt.DeepEquals, []string{`"tenant.data"`, "tenant.data"})
}

func referenceCollisionSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Literal", Name: "tenant.data"},
			{StructName: "Qualified", Schema: "tenant", Name: "data"},
		},
	}
}
