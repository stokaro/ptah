package builtin_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

func TestGetOrderedCreateStatements_ConstraintOwnerUsesExactTableIdentity(t *testing.T) {
	for _, order := range [][]string{{"app", ""}, {"", "app"}} {
		t.Run(strings.Join(order, "/"), func(t *testing.T) {
			c := qt.New(t)
			database := sameNamedForeignKeyTables(order)
			constraints := slices.Clone(database.Constraints)
			statements, err := builtin.GetOrderedCreateStatements(database, platform.Postgres)
			c.Assert(err, qt.IsNil)
			c.Assert(database.Constraints, qt.DeepEquals, constraints)
			sql := strings.Join(statements, "\n")
			c.Assert(strings.Count(sql, `ADD CONSTRAINT "children_parent"`), qt.Equals, 2)
			c.Assert(sql, qt.Contains, `ALTER TABLE "app"."children"`)
			c.Assert(sql, qt.Contains, `ALTER TABLE "children"`)
			c.Assert(sql, qt.Contains, `REFERENCES "app"."parents"("tenant_id", "id")`)
			c.Assert(sql, qt.Contains, `REFERENCES "parents"("tenant_id", "id")`)
		})
	}
}

func TestGetOrderedCreateStatements_ResolvesScopedConstraintShorthand(t *testing.T) {
	c := qt.New(t)
	database := sameNamedForeignKeyTables([]string{"app"})
	database.Constraints[0].Table = "children"
	statements, err := builtin.GetOrderedCreateStatements(database, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Count(strings.Join(statements, "\n"), `ADD CONSTRAINT "children_parent"`), qt.Equals, 1)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, `ALTER TABLE "app"."children"`)
	c.Assert(database.Constraints[0].Table, qt.Equals, "children")
}

func sameNamedForeignKeyTables(order []string) *schemamodel.Database {
	database := &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "app"}}}
	prefixes := map[string]string{"app": "App", "": "Public"}
	for _, schema := range order {
		parent, child := prefixes[schema]+"Parent", prefixes[schema]+"Child"
		parentTable := schemamodel.Table{Schema: schema, Name: "parents", StructName: parent, PrimaryKey: []string{"tenant_id", "id"}}
		childTable := schemamodel.Table{Schema: schema, Name: "children", StructName: child}
		database.Tables = append(database.Tables, parentTable, childTable)
		for _, structName := range []string{parent, child} {
			for _, column := range []string{"tenant_id", "id"} {
				database.Fields = append(database.Fields, schemamodel.Field{StructName: structName, Name: column, Type: "INTEGER"})
			}
		}
		database.Constraints = append(database.Constraints, schemamodel.Constraint{
			StructName: child, Table: childTable.QualifiedName(), Name: "children_parent", Type: "FOREIGN KEY",
			Columns: []string{"tenant_id", "id"}, ForeignTable: parentTable.QualifiedName(), ForeignColumns: []string{"tenant_id", "id"},
		})
	}
	return database
}
