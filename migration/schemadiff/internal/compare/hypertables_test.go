package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
)

// TestHypertables_ComparesByTheTableItPartitions pins what makes two
// hypertables the same one.
//
// A hypertable has no name of its own -- `timescaledb_information.hypertables`
// is keyed by the relation -- so the identity is the table, and an unqualified
// declaration names the same table a qualified catalog row does. Comparing the
// raw strings would report an addition and a removal on every run against a
// server whose read reports the schema (stokaro/ptah#1026).
func TestHypertables_ComparesByTheTableItPartitions(t *testing.T) {
	tests := []struct {
		name        string
		declared    []schemamodel.Hypertable
		live        []catalog.Hypertable
		wantAdded   []string
		wantRemoved []string
	}{
		{
			name:      "declared and absent",
			declared:  []schemamodel.Hypertable{{Table: "conditions", Column: "time"}},
			wantAdded: []string{"conditions"},
		},
		{
			// The declaration leaves the schema off and the catalog reports it,
			// which is what a Go-annotated schema against PostgreSQL looks
			// like. One table, two spellings.
			name:     "an unqualified declaration of a qualified row",
			declared: []schemamodel.Hypertable{{Table: "conditions", Column: "time"}},
			live: []catalog.Hypertable{{
				Schema: "public", Name: "conditions", PrimaryDimension: "time", Dimensions: 1,
			}},
		},
		{
			name: "live and undeclared",
			live: []catalog.Hypertable{{
				Schema: "public", Name: "conditions", PrimaryDimension: "time", Dimensions: 1,
			}},
			wantRemoved: []string{"public.conditions"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			declared := &schemamodel.Database{Hypertables: test.declared}
			live := &catalog.Database{Hypertables: test.live}

			compare.Hypertables(declared, live, diff, compare.CoverageOf(declared, live))

			c.Assert(diff.HypertablesAdded.Names(), qt.DeepEquals, test.wantAdded)
			c.Assert(diff.HypertablesRemoved.Names(), qt.DeepEquals, test.wantRemoved)
			c.Assert(diff.HypertablesModified, qt.HasLen, 0)
		})
	}
}

// TestHypertables_AnOmittedIntervalIsNotAChange is the row that would otherwise
// make every apply plan the same change forever.
//
// An empty declared interval takes TimescaleDB's own default -- 7 days for a
// timestamptz column, measured on 2.29.2 -- and the catalog then reports what
// the server chose. Comparing that against the empty declaration would report a
// difference on every run for a declaration that asked for whatever the server
// picked.
func TestHypertables_AnOmittedIntervalIsNotAChange(t *testing.T) {
	tests := []struct {
		name     string
		declared schemamodel.Hypertable
		live     catalog.Hypertable
		want     []difftypes.HypertableDiff
	}{
		{
			name:     "no interval declared",
			declared: schemamodel.Hypertable{Table: "conditions", Column: "time"},
			live: catalog.Hypertable{
				Schema: "public", Name: "conditions",
				PrimaryDimension: "time", ChunkInterval: "7 days", Dimensions: 1,
			},
		},
		{
			name: "the declared interval matches",
			declared: schemamodel.Hypertable{
				Table: "conditions", Column: "time", ChunkInterval: "1 day",
			},
			live: catalog.Hypertable{
				Schema: "public", Name: "conditions",
				PrimaryDimension: "time", ChunkInterval: "1 day", Dimensions: 1,
			},
		},
		{
			name: "the declared interval differs",
			declared: schemamodel.Hypertable{
				Table: "conditions", Column: "time", ChunkInterval: "1 hour",
			},
			live: catalog.Hypertable{
				Schema: "public", Name: "conditions",
				PrimaryDimension: "time", ChunkInterval: "7 days", Dimensions: 1,
			},
			want: []difftypes.HypertableDiff{{
				Table: "conditions", OldColumn: "time", NewColumn: "time",
				OldChunkInterval: "7 days", NewChunkInterval: "1 hour",
			}},
		},
		{
			name: "the dimension moved",
			declared: schemamodel.Hypertable{
				Table: "conditions", Column: "recorded_at",
			},
			live: catalog.Hypertable{
				Schema: "public", Name: "conditions",
				PrimaryDimension: "time", ChunkInterval: "7 days", Dimensions: 1,
			},
			want: []difftypes.HypertableDiff{{
				Table: "conditions", OldColumn: "time", NewColumn: "recorded_at",
				OldChunkInterval: "7 days",
			}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			declared := &schemamodel.Database{Hypertables: []schemamodel.Hypertable{test.declared}}
			live := &catalog.Database{Hypertables: []catalog.Hypertable{test.live}}

			compare.Hypertables(declared, live, diff, compare.CoverageOf(declared, live))

			c.Assert(diff.HypertablesModified, qt.DeepEquals, test.want)
			c.Assert(diff.HypertablesAdded, qt.HasLen, 0)
			c.Assert(diff.HypertablesRemoved, qt.HasLen, 0)
		})
	}
}

// TestHypertables_ADescriptionThatCouldNotSayItDoesNotUndoIt is the coverage
// half, and the one that costs the most when it is wrong.
//
// A format with no way to say a table is partitioned describes an ordinary
// table -- complete on its face, and wrong. Reading that silence as intent
// would report a removal the planner then refuses, so an operator applying a
// YAML document against a TimescaleDB server would be told their schema cannot
// be applied at all.
func TestHypertables_ADescriptionThatCouldNotSayItDoesNotUndoIt(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}
	live := &catalog.Database{Hypertables: []catalog.Hypertable{{
		Schema: "public", Name: "conditions", PrimaryDimension: "time", Dimensions: 1,
	}}}
	silent := &schemamodel.Database{NotDescribed: coverage.Set{}.With(coverage.Object{
		Kind:       coverage.Hypertable,
		Reason:     coverage.Unsupported,
		Provenance: coverage.DerivedFromFact,
	})}

	compare.Hypertables(silent, live, diff, compare.CoverageOf(silent, live))

	c.Assert(diff.HypertablesRemoved, qt.HasLen, 0)

	// The control: a description that COULD have named one still removes.
	speaking := &schemamodel.Database{}
	plain := &difftypes.SchemaDiff{}
	compare.Hypertables(speaking, live, plain, compare.CoverageOf(speaking, live))
	c.Assert(plain.HypertablesRemoved.Names(), qt.DeepEquals, []string{"public.conditions"})
}
