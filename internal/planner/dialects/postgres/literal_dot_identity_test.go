package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

func TestPlanner_LiteralDotAndQualifiedTablesRemainDistinct(t *testing.T) {
	c := qt.New(t)
	generated := literalDotAndQualifiedSchema()
	diff := &types.SchemaDiff{
		TablesAdded: []string{`"tenant.data"`, "tenant.data"},
		IndexesAdded: []types.IndexRef{
			{Name: "literal_lookup", TableName: `"tenant.data"`},
			{Name: "qualified_lookup", TableName: "tenant.data"},
		},
	}

	nodes, err := postgres.New().GenerateMigrationASTChecked(diff, generated)
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
	diff := &types.SchemaDiff{
		TablesRemoved: []string{`"tenant.data"`, "tenant.data"},
	}

	nodes, err := postgres.New().GenerateMigrationASTChecked(diff, literalDotAndQualifiedSchema())
	c.Assert(err, qt.IsNil)
	sql, err := renderer.RenderSQL("postgres", nodes...)
	c.Assert(err, qt.IsNil)

	c.Assert(strings.Count(sql, "DROP TABLE"), qt.Equals, 2)
	c.Assert(sql, qt.Contains, `DROP TABLE IF EXISTS "tenant.data"`)
	c.Assert(sql, qt.Contains, `DROP TABLE IF EXISTS "tenant"."data"`)
}

func literalDotAndQualifiedSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Literal", Name: "tenant.data"},
			{StructName: "Qualified", Schema: "tenant", Name: "data"},
		},
		Fields: []goschema.Field{
			{StructName: "Literal", Name: "id", Type: "INTEGER"},
			{StructName: "Qualified", Name: "id", Type: "INTEGER"},
		},
		Indexes: []goschema.Index{
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
