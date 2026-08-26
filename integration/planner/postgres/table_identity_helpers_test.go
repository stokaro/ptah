//go:build integration

package postgres_test

import (
	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/migration/generator"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
)

func downColumnTarget(tableSchema string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users", Schema: tableSchema}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "User", Name: "email", Type: "TEXT"},
		},
	}
}

func downColumnDatabase(tableSchema string) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{
			Name: "users", Schema: tableSchema, Type: "BASE TABLE",
			Columns: []catalog.Column{
				{Name: "id", DataType: "integer", IsNullable: "NO", OrdinalPosition: 1, IsPrimaryKey: true},
				{Name: "email", DataType: "text", IsNullable: "YES", OrdinalPosition: 2},
				{Name: "legacy_note", DataType: "text", IsNullable: "YES", OrdinalPosition: 3},
			},
		}},
	}
}

func planDownStatements(c *qt.C, generated *schemamodel.Database, database *catalog.Database) []string {
	c.Helper()
	diff := schemadiff.CompareWithDialect(generated, database, "postgres")
	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:    diff,
		Desired: generated,
		Current: database,
		Dialect: "postgres",
		Policy: generator.BidirectionalPlanPolicy{
			Create: generator.ConcurrentIndexDisabled,
			Drop:   generator.ConcurrentIndexDisabled,
		},
	})
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatements(
		plan.Reverse.Diff,
		dbschematogo.ConvertCatalogToSchema(database),
		"postgres",
	)
	c.Assert(err, qt.IsNil)
	return statements
}
