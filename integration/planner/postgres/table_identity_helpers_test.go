//go:build integration

package postgres_test

import (
	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	dbtypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/migration/generator"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
)

func downColumnTarget(tableSchema string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users", Schema: tableSchema}},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "User", Name: "email", Type: "TEXT"},
		},
	}
}

func downColumnDatabase(tableSchema string) *dbtypes.DBSchema {
	return &dbtypes.DBSchema{
		Tables: []dbtypes.DBTable{{
			Name: "users", Schema: tableSchema, Type: "BASE TABLE",
			Columns: []dbtypes.DBColumn{
				{Name: "id", DataType: "integer", IsNullable: "NO", OrdinalPosition: 1, IsPrimaryKey: true},
				{Name: "email", DataType: "text", IsNullable: "YES", OrdinalPosition: 2},
				{Name: "legacy_note", DataType: "text", IsNullable: "YES", OrdinalPosition: 3},
			},
		}},
	}
}

func planDownStatements(c *qt.C, generated *goschema.Database, database *dbtypes.DBSchema) []string {
	c.Helper()
	diff := schemadiff.CompareWithDialect(generated, database, "postgres")
	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: generated,
		CurrentSchema: database,
		Dialect:       "postgres",
		Policy: generator.BidirectionalPlanPolicy{
			Create: generator.ConcurrentIndexDisabled,
			Drop:   generator.ConcurrentIndexDisabled,
		},
	})
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatements(
		plan.Reverse.Diff,
		dbschematogo.ConvertDBSchemaToGoSchema(database),
		"postgres",
	)
	c.Assert(err, qt.IsNil)
	return statements
}
