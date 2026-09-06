package schemadiff_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
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
			Sequences:  []catalog.Sequence{{Name: "s_b"}, {Name: "s_a"}},
			Extensions: []catalog.Extension{{Name: "e_b"}, {Name: "e_a"}},
			Domains:    []catalog.Domain{{Name: "d_b"}, {Name: "d_a"}},
			Composites: []catalog.CompositeType{{Name: "c_b"}, {Name: "c_a"}},
			Ranges:     []catalog.Range{{Name: "r_b"}, {Name: "r_a"}},
			Synonyms:   []catalog.Synonym{{Name: "y_b"}, {Name: "y_a"}},
		}
	}
	reversed := func() *catalog.Database {
		db := current()
		for _, swap := range []func(){
			func() { db.Sequences[0], db.Sequences[1] = db.Sequences[1], db.Sequences[0] },
			func() { db.Extensions[0], db.Extensions[1] = db.Extensions[1], db.Extensions[0] },
			func() { db.Domains[0], db.Domains[1] = db.Domains[1], db.Domains[0] },
			func() { db.Composites[0], db.Composites[1] = db.Composites[1], db.Composites[0] },
			func() { db.Ranges[0], db.Ranges[1] = db.Ranges[1], db.Ranges[0] },
			func() { db.Synonyms[0], db.Synonyms[1] = db.Synonyms[1], db.Synonyms[0] },
		} {
			swap()
		}
		return db
	}

	encode := func(c *qt.C, db *catalog.Database) string {
		c.Helper()
		encoded, err := json.Marshal(schemadiff.Compare(&schemamodel.Database{}, db))
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
