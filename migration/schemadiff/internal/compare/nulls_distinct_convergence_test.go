package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// An unset NullsDistinct is not a third state, and comparing it as one is a
// convergence bug rather than a cosmetic one. PostgreSQL prints the clause
// back only when it is NOT DISTINCT, so an index declared nulls_distinct="true"
// -- the engine's own default -- reads back as unset forever.
//
// Measured on PostgreSQL 18.6 before the fix: `ptah schema apply` succeeded,
// the server stored exactly what was asked for, and every later
// `ptah schema compare` reported the index as removed and added again, with a
// DROP INDEX / CREATE INDEX pair that no number of applies settled. See
// stokaro/ptah#2820.

func nullsDistinctIndexes(desired, current *bool) (*schemamodel.Database, *catalog.Database) {
	return &schemamodel.Database{
		Indexes: []schemamodel.Index{
			{Name: "idx_t_s", TableName: "t", Fields: []string{"s"}, Unique: true, NullsDistinct: desired},
		},
	}, &catalog.Database{
		Indexes: []catalog.Index{
			{Name: "idx_t_s", TableName: "t", Columns: []string{"s"}, IsUnique: true, NullsDistinct: current},
		},
	}
}

func TestIndexes_NullsDistinctConvergence_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		desired *bool
		current *bool
	}{
		{name: "declared default against a server that printed nothing", desired: new(true), current: nil},
		{name: "declared nothing against a server that printed nothing", desired: nil, current: nil},
		{name: "declared nothing against a server that printed the default", desired: nil, current: new(true)},
		{name: "both spell the default", desired: new(true), current: new(true)},
		{name: "both spell not distinct", desired: new(false), current: new(false)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, current := nullsDistinctIndexes(test.desired, test.current)
			diff := &difftypes.SchemaDiff{}

			compare.IndexesWithDialect(desired, current, diff, "postgres")

			c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
			c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
		})
	}
}

// The control that keeps the fold from being blindness: a real change in
// null-equality is still reported, from either side and in either direction.
func TestIndexes_NullsDistinctConvergence_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		desired *bool
		current *bool
	}{
		{name: "declared not distinct against a server that printed nothing", desired: new(false), current: nil},
		{name: "declared nothing against a server that printed not distinct", desired: nil, current: new(false)},
		{name: "declared the default against a server holding not distinct", desired: new(true), current: new(false)},
		{name: "declared not distinct against a server holding the default", desired: new(false), current: new(true)},
	}

	want := []difftypes.IndexRef{{Name: "idx_t_s", TableName: "t"}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, current := nullsDistinctIndexes(test.desired, test.current)
			diff := &difftypes.SchemaDiff{}

			compare.IndexesWithDialect(desired, current, diff, "postgres")

			c.Assert(diff.IndexAdditions(), qt.DeepEquals, want)
			c.Assert(diff.IndexRemovals(), qt.DeepEquals, want)
		})
	}
}
