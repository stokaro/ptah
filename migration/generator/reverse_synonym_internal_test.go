package generator

// White-box testing required: generateDownMigrationSQL is package-local, and
// the reversal it performs has no exported entry point that takes a diff.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestGenerateDownMigrationSQL_RecreatesASynonymTheUpMigrationDropped pins the
// half of the reversal that a name could not carry.
//
// A synonym IS its target -- there is nothing else to it -- and the down
// direction has to CREATE one the up direction dropped. Nothing covered that,
// and the conversion to a typed change (stokaro/ptah#2315) moves where the
// target comes from: the planner reads it off the change rather than looking
// the name up. Measured on the tree before that conversion, this passes there
// too, so it records a property the change had to keep rather than one it
// fixed -- which is why it is worth having.
//
// The target is asserted, not just the verb. A CREATE SYNONYM naming the wrong
// object rolls back to a schema that is the wrong shape while reading as a
// successful rollback.
func TestGenerateDownMigrationSQL_RecreatesASynonymTheUpMigrationDropped(t *testing.T) {
	c := qt.New(t)

	// The desired schema declares no synonym; the database holds one. That is
	// the shape that puts it in SynonymsRemoved.
	schema := &schemamodel.Database{}
	db := &catalog.Database{
		Synonyms: []catalog.Synonym{{
			Name:           "s_users",
			Schema:         "dbo",
			Target:         "[other].[dbo].[users]",
			TargetDatabase: "other",
			TargetSchema:   "dbo",
			TargetObject:   "users",
		}},
	}

	upDiff := schemadiff.CompareWithDialect(schema, db, "sqlserver")
	c.Assert(upDiff.SynonymsRemoved.Names(), qt.DeepEquals, []string{"dbo.s_users"})

	downSQL, err := generateDownMigrationSQL(upDiff, schema, db, "sqlserver")

	c.Assert(err, qt.IsNil)
	c.Assert(downSQL, qt.Contains, "CREATE SYNONYM",
		qt.Commentf("the rollback has to put back what the up migration dropped"))
	c.Assert(downSQL, qt.Contains, "[other].[dbo].[users]",
		qt.Commentf("and it has to point at the object the synonym named"))
}
