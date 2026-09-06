package generator

// White-box testing required: generateDownMigrationSQL is package-local, and
// the property is about what the reversal renders rather than about any
// exported entry point.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
)

// TestGenerateDownMigrationSQL_RestoresTheColumnThePriorDatabaseHeld is the
// direction half of the column-modification operand.
//
// A forward modification carries the column the change moves TO. Carrying that
// same operand across the reversal would make the rollback re-render the state
// being rolled back: the down of `INTEGER -> BIGINT` would say `TYPE BIGINT`,
// apply cleanly, and leave the column where the up migration put it.
//
// So the reversal resolves the operand against the PRE-CHANGE database instead,
// the way every other reversal in this file resolves its own.
//
// This asserts both directions of the one plan. Asserting only that the down
// says INTEGER would pass against a reversal that renders nothing at all, and
// asserting only that it does not say BIGINT would pass against an empty
// migration.
func TestGenerateDownMigrationSQL_RestoresTheColumnThePriorDatabaseHeld(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "M", Name: "meters"}},
		Fields: []schemamodel.Field{
			{StructName: "M", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "M", Name: "reading", Type: "BIGINT"},
		},
	}
	database := &catalog.Database{
		Tables: []catalog.Table{{
			Name: "meters", Type: "BASE TABLE",
			Columns: []catalog.Column{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "reading", DataType: "integer", IsNullable: "NO"},
			},
		}},
	}

	upDiff := schemadiff.CompareWithDialect(desired, database, platform.Postgres)
	c.Assert(upDiff.TablesModified, qt.HasLen, 1)
	c.Assert(upDiff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
	c.Assert(upDiff.TablesModified[0].ColumnsModified[0].Desired.Type, qt.Equals, "BIGINT")

	up, err := generateUpMigrationSQL(upDiff, desired, platform.Postgres, capability.Postgres17())
	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Contains, "BIGINT")

	down, err := generateDownMigrationSQL(upDiff, desired, database, platform.Postgres, capability.Postgres17())
	c.Assert(err, qt.IsNil)
	// The server's own spelling, because prior is the database read: a rollback
	// restores what the catalog reported, not what the declaration would have
	// written for the same type.
	c.Assert(down, qt.Contains, `ALTER COLUMN "reading" TYPE integer`,
		qt.Commentf("the rollback restores the type the database held:\n%s", down))
	// `TYPE BIGINT` rather than `BIGINT`: the rollback's human-readable comment
	// names both sides of the transition, correctly, and asserting on the bare
	// word would fail on the line that proves the display reversed.
	c.Assert(down, qt.Not(qt.Contains), "TYPE BIGINT",
		qt.Commentf("a rollback naming the forward operand undoes nothing:\n%s", down))
}
