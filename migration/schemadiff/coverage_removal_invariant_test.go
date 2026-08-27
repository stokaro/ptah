package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestCompare_NotInspectedNeverBecomesAbsent holds the first of the three
// invariants stokaro/ptah#2315 lists as unheld.
//
// docs/architecture_boundaries.md records it as "nothing on the shipping path
// -- core/coverage carries the record and the comparison threads it, but no
// test holds the property end to end". The threading is real: Coverage gates
// removals on what the DESIRED description says it does not describe. What was
// missing is a test that runs the property through schemadiff.Compare, which is
// the entry point every caller reaches.
//
// The failure it prevents is deletion by omission. `ptah-compat schema inspect`
// omits the block types the pinned Atlas community binary refuses to read;
// applying that document back to the database it came from once planned
// DROP EXTENSION, because a presentation decision had become deletion intent.
//
// EVERY ROW CARRIES ITS OWN INVERSE. The same fixture with no limit recorded
// must plan the removal -- without that half, a row would pass against a
// comparison that had stopped planning removals for that family at all, which
// is the same silence the invariant is about.
func TestCompare_NotInspectedNeverBecomesAbsent(t *testing.T) {
	tests := []struct {
		name    string
		kind    coverage.Kind
		current func(*catalog.Database)
		removed func(*difftypes.SchemaDiff) int
	}{
		{
			name:    "sequence",
			kind:    coverage.Sequence,
			current: func(db *catalog.Database) { db.Sequences = []catalog.Sequence{{Name: "s"}} },
			removed: func(d *difftypes.SchemaDiff) int { return len(d.SequencesRemoved) },
		},
		{
			name:    "extension",
			kind:    coverage.Extension,
			current: func(db *catalog.Database) { db.Extensions = []catalog.Extension{{Name: "e"}} },
			removed: func(d *difftypes.SchemaDiff) int { return len(d.ExtensionsRemoved) },
		},
		{
			name:    "domain",
			kind:    coverage.Domain,
			current: func(db *catalog.Database) { db.Domains = []catalog.Domain{{Name: "d"}} },
			removed: func(d *difftypes.SchemaDiff) int { return len(d.DomainsRemoved) },
		},
		{
			name:    "composite type",
			kind:    coverage.Composite,
			current: func(db *catalog.Database) { db.Composites = []catalog.CompositeType{{Name: "c"}} },
			removed: func(d *difftypes.SchemaDiff) int { return len(d.CompositeTypesRemoved) },
		},
		{
			name:    "range type",
			kind:    coverage.Range,
			current: func(db *catalog.Database) { db.Ranges = []catalog.Range{{Name: "r"}} },
			removed: func(d *difftypes.SchemaDiff) int { return len(d.RangesRemoved) },
		},
		// No role row, deliberately. compare/roles.go documents RolesRemoved as
		// "always empty (roles are not automatically removed for safety)", so a
		// row for it would assert that nothing is withheld from a family that
		// plans nothing -- a control that cannot fire, which is the shape this
		// test's inverse half exists to refuse. Roles reach coverage through
		// the ADDITIVE gate instead, which is a different property.
		{
			name:    "synonym",
			kind:    coverage.Synonym,
			current: func(db *catalog.Database) { db.Synonyms = []catalog.Synonym{{Name: "syn"}} },
			removed: func(d *difftypes.SchemaDiff) int { return len(d.SynonymsRemoved) },
		},

		{
			name: "extended property",
			kind: coverage.ExtendedProperty,
			current: func(db *catalog.Database) {
				db.ExtendedProperties = []catalog.ExtendedProperty{{Name: "MS_Description", Schema: "dbo"}}
			},
			removed: func(d *difftypes.SchemaDiff) int { return len(d.ExtendedPropertiesRemoved) },
		},
		{
			name:    "hypertable",
			kind:    coverage.Hypertable,
			current: func(db *catalog.Database) { db.Hypertables = []catalog.Hypertable{{Name: "metrics"}} },
			removed: func(d *difftypes.SchemaDiff) int { return len(d.HypertablesRemoved) },
		},
		{
			name: "continuous aggregate",
			kind: coverage.ContinuousAggregate,
			current: func(db *catalog.Database) {
				db.ContinuousAggregates = []catalog.ContinuousAggregate{{Name: "daily"}}
			},
			removed: func(d *difftypes.SchemaDiff) int { return len(d.ContinuousAggregatesRemoved) },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			current := &catalog.Database{}
			test.current(current)

			// The control first: with nothing recorded, the object the desired
			// description does not name IS a removal. A row whose control does
			// not fire proves nothing about the row below it.
			planned := schemadiff.Compare(&schemamodel.Database{}, current)
			c.Assert(test.removed(planned), qt.Not(qt.Equals), 0,
				qt.Commentf("the inverse: an unlimited description does plan this removal"))

			// The invariant: the same comparison, with the desired side saying
			// it does not describe this kind, plans nothing.
			desired := &schemamodel.Database{}
			desired.NotDescribed = desired.NotDescribed.With(coverage.Refused(test.kind))
			withheld := schemadiff.Compare(desired, current)
			c.Assert(test.removed(withheld), qt.Equals, 0,
				qt.Commentf("a kind the description does not describe is not a kind to drop"))
		})
	}
}
