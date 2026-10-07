package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/schemadiff/difftypes"
)

// referencingField is a column of children that references parents(id) with
// every clause a foreign key can carry.
func referencingField(name string) schemamodel.Field {
	return schemamodel.Field{
		StructName: "Child", Name: name, Type: "INTEGER", Nullable: true, Foreign: "parents(id)",
		OnDelete: "CASCADE", Deferrable: true, ForeignKeyMatch: "FULL", ForeignKeyNotEnforced: true,
	}
}

// planPostgres18 renders the plan for diff against desired on PostgreSQL 18.
func planPostgres18(c *qt.C, diff *difftypes.SchemaDiff, desired *schemamodel.Database) string {
	c.Helper()
	nodes, err := postgres.NewForDialect(platform.Postgres, capability.Postgres18()).
		GenerateMigrationAST(withDeclaredObjects(diff, desired))
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQLWithCapabilities(platform.Postgres, capability.Postgres18(), nodes...)
	c.Assert(err, qt.IsNil)
	return sql
}

// TestPlanner_AColumnsForeignKeyKeepsItsClauses adds a column's foreign key
// with every clause the field declares, in a table the plan creates and on a
// column it adds. A key built with the actions alone checks what the
// declaration said it does not (stokaro/ptah#3853).
func TestPlanner_AColumnsForeignKeyKeepsItsClauses(t *testing.T) {
	const want = `REFERENCES "parents"("id") MATCH FULL ON DELETE CASCADE DEFERRABLE NOT ENFORCED;`
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Parent", Name: "parents", PrimaryKey: []string{"id"}},
			{StructName: "Child", Name: "children", PrimaryKey: []string{"id"}},
		},
		Fields: []schemamodel.Field{
			{StructName: "Parent", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Child", Name: "id", Type: "INTEGER", Primary: true},
			referencingField("parent_id"),
		},
	}

	t.Run("a table the plan creates", func(t *testing.T) {
		c := qt.New(t)

		sql := planPostgres18(c, &difftypes.SchemaDiff{
			TablesAdded: difftypes.TableCreationsFor(desired, "parents", "children"),
		}, desired)

		c.Assert(sql, qt.Contains, `FOREIGN KEY ("parent_id") `+want)
	})
	t.Run("a column the plan adds", func(t *testing.T) {
		c := qt.New(t)

		sql := planPostgres18(c, &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
			TableName: "children", ColumnsAdded: difftypes.ColumnChanges{referencingField("other_id")},
		}}}, desired)

		c.Assert(sql, qt.Contains, `FOREIGN KEY ("other_id") `+want)
	})
}

// TestPlanner_AnAddedConstraintKeepsItsClauses adds a CHECK and a foreign key
// the comparison reports with the clauses the addition carries.
func TestPlanner_AnAddedConstraintKeepsItsClauses(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{ConstraintsAdded: difftypes.ConstraintAdditions{
		{Name: "t_n_positive", TableName: "t", Type: "CHECK", CheckExpression: "n > 0", NotEnforced: true},
		{
			Name: "t_p_fkey", TableName: "t", Type: "FOREIGN KEY", Columns: []string{"p"},
			ForeignTable: "parents", ForeignColumn: "id", Match: "FULL", NotEnforced: true,
		},
	}}

	sql := planPostgres18(c, diff, &schemamodel.Database{})

	c.Assert(sql, qt.Contains, `ADD CONSTRAINT "t_n_positive" CHECK (n > 0) NOT ENFORCED;`)
	c.Assert(sql, qt.Contains, `ADD CONSTRAINT "t_p_fkey" FOREIGN KEY ("p") REFERENCES "parents"("id") MATCH FULL NOT ENFORCED;`)
	c.Assert(strings.Count(sql, "ADD CONSTRAINT"), qt.Equals, 2)
}
