package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
)

// A column made NOT NULL with no default is planned as SET NOT NULL and the
// comment saying it fails on a NULL row. Filled with a value its type
// suggests, 0 for an integer, a NULL row is rewritten and the plan reports
// success where the server and Atlas CE v1.3.0 refuse (stokaro/ptah#3648).
func TestGenerateSchemaDiffSQLStatements_SetNotNullWithoutDefault(t *testing.T) {
	c := qt.New(t)
	desired := schemamodel.Field{Name: "qty", Type: "INTEGER", StructName: "Flag"}

	got, err := planner.GenerateSchemaDiffSQLStatements(
		oneModifiedColumn(desired, map[string]string{"nullable": "true -> false"}), platform.Postgres,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []string{"-- Add/modify columns for table: flags --\n" +
		"-- Modify column flags.qty: nullable: true -> false --\n" +
		"-- ALTER statements: --\n" +
		"-- POSTGRES: SET NOT NULL fails if any row of \"flags\" holds NULL in \"qty\"; the column declares no default to fill it with.\n" +
		`ALTER TABLE "flags" ALTER COLUMN "qty" SET NOT NULL`})
}
