package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/planner/dialects/mysql"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// A plan that would add two index names the target may treat as one is refused
// rather than emitted.
//
// Measured on mysql:8.4.11, `CREATE INDEX İ` followed by `CREATE INDEX i` on
// one table gets ERROR 1061 Duplicate key name 'i' -- the first index is
// created and the second is refused, which is a migration applied halfway. The
// two engines disagree about which non-ASCII names they fold, so the offline
// answer is that the pair may collide, not that it does or does not.
// See stokaro/ptah#2768.
func TestPlanner_NonASCIIIndexNamesThatMayCollide_AreRefused(t *testing.T) {
	tests := []struct {
		name  string
		first string
		other string
	}{
		{name: "dotted capital I beside ASCII i", first: "İ", other: "i"},
		{name: "A umlaut beside a umlaut", first: "Ä", other: "ä"},
		{name: "ASCII I beside a dotless i", first: "I", other: "ı"},
		// The Kelvin sign is written as an escape on purpose: pasted through a
		// shell it arrives normalized to ASCII K, and the row then compares a
		// name with itself and passes against any implementation at all.
		{name: "Kelvin sign beside ASCII K", first: "\u212A", other: "K"},
		{name: "capital sigma beside a final sigma", first: "Σ", other: "ς"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{
				IndexesAdded: difftypes.IndexChanges{
					{Index: schemamodel.Index{Name: test.first, Fields: []string{"a"}}, TableName: "t"},
					{Index: schemamodel.Index{Name: test.other, Fields: []string{"b"}}, TableName: "t"},
				},
			}

			nodes, err := mysql.New().GenerateMigrationAST(diff)

			c.Assert(err, qt.IsNotNil)
			c.Assert(nodes, qt.IsNil)
			c.Assert(err.Error(), qt.Contains, "conflict")
		})
	}
}

// Two ordinary ASCII names still plan, so the refusal above is about the
// unresolved equivalence rather than about adding two indexes to one table.
func TestPlanner_TwoASCIIIndexNames_StillPlan(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		IndexesAdded: difftypes.IndexChanges{
			{Index: schemamodel.Index{Name: "idx_alpha", Fields: []string{"a"}}, TableName: "t"},
			{Index: schemamodel.Index{Name: "idx_beta", Fields: []string{"b"}}, TableName: "t"},
		},
	}

	nodes, err := mysql.New().GenerateMigrationAST(diff)

	c.Assert(err, qt.IsNil)
	c.Assert(len(nodes) > 0, qt.IsTrue)
}
