package schemadiff_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/featurejson"
	"ptah.run/migration/schemadiff"
)

// TestCompare_EquivalentInputsProduceIdenticalOutput holds the third of the
// three invariants stokaro/ptah#2315 lists as unheld.
//
// docs/architecture_boundaries.md records it as "nothing on the shipping path",
// its holder having been the removed `schemachange` determinism tests. It
// matters because a comparison feeds a migration FILE: a diff that reorders
// itself between runs writes a different migration for an unchanged schema, and
// two engineers generating from the same pair get different files.
//
// Two halves, and the second is the one that can fail.
//
// Repeating the same comparison exercises Go's randomized map iteration, so a
// comparator that ranged over a map without sorting drifts across runs. That
// half is real but weak on its own -- twenty runs can agree by luck on a small
// fixture.
//
// SHUFFLING THE INPUT is the control. Every family below is a slice, and slice
// order is caller-visible: a comparison that carried its input's order into its
// output would produce a different document for the same schema described in a
// different order, which is the same defect reached from the other side.
func TestCompare_EquivalentInputsProduceIdenticalOutput(t *testing.T) {
	// One object per family the bare fixture can populate, and two of several
	// so that ordering has something to vary.
	current := func() *catalog.Database {
		return &catalog.Database{
			Sequences:       []catalog.Sequence{{Name: "s_b"}, {Name: "s_a"}},
			FeatureCoverage: must.Must(pgtype.CompleteCoverage(schemaext.Observed)),
			FeatureObjects: must.Must(schemaext.NewObjects(
				schemaext.Object{Ref: pgtype.Ref(pgtype.DomainKind, "", "d_b"), Value: &pgtype.ObservedDomain{BaseType: "int4"}},
				schemaext.Object{Ref: pgtype.Ref(pgtype.DomainKind, "", "d_a"), Value: &pgtype.ObservedDomain{BaseType: "int4"}},
				schemaext.Object{Ref: pgtype.Ref(pgtype.CompositeKind, "", "c_b"),
					Value: &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "a", Type: "int4"}}}},
				schemaext.Object{Ref: pgtype.Ref(pgtype.CompositeKind, "", "c_a"),
					Value: &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "a", Type: "int4"}}}},
				schemaext.Object{Ref: pgtype.Ref(pgtype.RangeKind, "", "r_b"), Value: &pgtype.ObservedRange{Subtype: "int4"}},
				schemaext.Object{Ref: pgtype.Ref(pgtype.RangeKind, "", "r_a"), Value: &pgtype.ObservedRange{Subtype: "int4"}})),
		}
	}
	reversed := func() *catalog.Database {
		db := current()
		for _, swap := range []func(){
			func() { db.Sequences[0], db.Sequences[1] = db.Sequences[1], db.Sequences[0] },
			func() {
				objects := must.Must(db.FeatureObjects.All())
				slices.Reverse(objects)
				db.FeatureObjects = must.Must(schemaext.NewObjects(objects...))
			},
		} {
			swap()
		}
		return db
	}

	encode := func(c *qt.C, db *catalog.Database) string {
		c.Helper()
		encoded, err := featurejson.Marshal(c.Context(), must.Must(builtin.New()).Codecs(), schemaext.Desired, must.Must(schemadiff.CompareWithDialect(t.Context(), &schemamodel.Database{FeatureCoverage: must.Must(pgtype.CompleteCoverage(schemaext.Desired))}, db, "postgres", must.Must(builtin.New()))))
		c.Assert(err, qt.IsNil)
		return string(encoded)
	}

	tests := []struct {
		name  string
		input func() *catalog.Database
		why   string
	}{
		{
			name:  "the same description, compared again",
			input: current,
			why:   "map iteration is randomized per run, so a comparator ranging over one unsorted drifts here",
		},
		{
			name:  "the same description, in the opposite order",
			input: reversed,
			why:   "slice order is the caller's, and it must not reach the document",
		},
	}

	c := qt.New(t)
	want := encode(c, current())
	c.Assert(want, qt.Not(qt.Equals), "", qt.Commentf("the fixture has to produce a document at all"))

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			// Twenty, because one repeat of a randomized map order proves
			// nothing and the comparison is cheap.
			for range 20 {
				c.Assert(encode(c, test.input()), qt.Equals, want, qt.Commentf("%s", test.why))
			}
		})
	}
}
