package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

func TestPlanner_LiteralDotAndQualifiedTablesRemainDistinct(t *testing.T) {
	c := qt.New(t)
	desired := literalDotAndQualifiedSchema()
	diff := &difftypes.SchemaDiff{
		// By table rather than by name, because a name is exactly what these two
		// tables share: `tenant.data` is the literal name of one and the
		// qualified name of the other, so asking for a creation by that string
		// is the ambiguity this test exists to keep apart.
		TablesAdded: difftypes.TableChanges{
			difftypes.TableCreationFor(desired, desired.Tables[0], `"tenant.data"`),
			difftypes.TableCreationFor(desired, desired.Tables[1], "tenant.data"),
		},
		IndexesAdded: []difftypes.IndexRef{
			{Name: "literal_lookup", TableName: `"tenant.data"`},
			{Name: "qualified_lookup", TableName: "tenant.data"},
		},
	}

	nodes, err := postgres.New().GenerateMigrationAST(diff, desired)
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)

	c.Assert(strings.Count(sql, "CREATE TABLE"), qt.Equals, 2)
	c.Assert(sql, qt.Contains, `CREATE TABLE "tenant.data"`)
	c.Assert(sql, qt.Contains, `CREATE TABLE "tenant"."data"`)
	c.Assert(sql, qt.Contains, `CREATE INDEX IF NOT EXISTS "literal_lookup" ON "tenant.data"`)
	c.Assert(sql, qt.Contains, `CREATE INDEX IF NOT EXISTS "qualified_lookup" ON "tenant"."data"`)
}

func TestPlanner_LiteralDotAndQualifiedTableRemovalsRemainDistinct(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TablesRemoved: []string{`"tenant.data"`, "tenant.data"},
	}

	nodes, err := postgres.New().GenerateMigrationAST(diff, literalDotAndQualifiedSchema())
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)

	c.Assert(strings.Count(sql, "DROP TABLE"), qt.Equals, 2)
	c.Assert(sql, qt.Contains, `DROP TABLE IF EXISTS "tenant.data"`)
	c.Assert(sql, qt.Contains, `DROP TABLE IF EXISTS "tenant"."data"`)
}

func literalDotAndQualifiedSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Literal", Name: "tenant.data"},
			{StructName: "Qualified", Schema: "tenant", Name: "data"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Literal", Name: "id", Type: "INTEGER"},
			{StructName: "Qualified", Name: "id", Type: "INTEGER"},
		},
		Indexes: []schemamodel.Index{
			{
				StructName: "Literal",
				Name:       "literal_lookup",
				TableName:  `"tenant.data"`,
				Fields:     []string{"id"},
			},
			{
				StructName: "Qualified",
				Name:       "qualified_lookup",
				TableName:  "tenant.data",
				Fields:     []string{"id"},
			},
		},
	}
}
