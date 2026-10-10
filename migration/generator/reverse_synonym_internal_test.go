package generator

// White-box testing required: generateDownMigrationSQL is package-local, and
// the reversal it performs has no exported entry point that takes a diff.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/synonym"
	"ptah.run/migration/schemadiff"
)

// TestGenerateDownMigrationSQL_RecreatesASynonymTheUpMigrationDropped pins the
// half of the reversal that a name could not carry.
//
// A synonym IS its target -- there is nothing else to it -- and the down
// direction has to CREATE one the up direction dropped. Nothing covered that,
// and the change has to carry the target, since the owner's reversal builds
// the CREATE from it rather than looking the name up (stokaro/ptah#2315).
//
// The target is asserted, not just the verb. A CREATE SYNONYM naming the wrong
// object rolls back to a schema that is the wrong shape while reading as a
// successful rollback.
func TestGenerateDownMigrationSQL_RecreatesASynonymTheUpMigrationDropped(t *testing.T) {
	c := qt.New(t)

	// The desired schema can declare synonyms and declares none; the database
	// holds one. That is the shape that drops it.
	complete := schemaext.Knowledge{State: schemaext.Complete}
	schema := &schemamodel.Database{FeatureCoverage: must.Must(synonym.Coverage(schemaext.Desired, complete, nil))}
	db := &catalog.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(synonym.ObservedObject(synonym.ObservedSynonym{
			Synonym: synonym.Synonym{Name: "s_users", Schema: "dbo", Target: "other.dbo.users"},
		}))),
		FeatureCoverage: must.Must(synonym.Coverage(schemaext.Observed, complete, nil)),
	}

	upDiff := must.Must(schemadiff.CompareWithDialect(t.Context(),
		schema, db, "sqlserver", must.Must(builtin.New()),
	))
	c.Assert(upDiff.FeatureChanges, qt.HasLen, 1)

	downSQL, err := generateDownMigrationSQL(t.Context(), must.Must(builtin.New()),
		upDiff, schema, db, "sqlserver")

	c.Assert(err, qt.IsNil)
	c.Assert(downSQL, qt.Contains, "CREATE SYNONYM",
		qt.Commentf("the rollback has to put back what the up migration dropped"))
	c.Assert(downSQL, qt.Contains, "[other].[dbo].[users]",
		qt.Commentf("and it has to point at the object the synonym named"))
}
