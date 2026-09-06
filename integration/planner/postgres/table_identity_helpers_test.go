//go:build integration

package postgres_test

import (
	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/generator"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
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

func planDownStatements(c *qt.C, desired *schemamodel.Database, current *catalog.Database) []string {
	c.Helper()
	diff := schemadiff.CompareWithDialect(desired, current, "postgres")
	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:          diff,
		DesiredSchema: desired,
		CurrentSchema: current,
		Dialect:       "postgres",
		Policy: generator.BidirectionalPlanPolicy{
			Create: generator.ConcurrentIndexDisabled,
			Drop:   generator.ConcurrentIndexDisabled,
		},
	})
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatements(
		plan.Reverse.Diff,

		"postgres",
	)

	c.Assert(err, qt.IsNil)
	return statements
}
