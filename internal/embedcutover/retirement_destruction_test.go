package embedcutover_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/embedcutover"
)

// baseRetirementPlan is a retirement that destroys nothing, so every case
// below moves exactly one fact away from it.
func baseRetirementPlan() embedcutover.RetirementPlan {
	return embedcutover.RetirementPlan{
		Generation: "gen-1", Schema: "public", Table: "article_vectors",
		Column: "embedding", RowCount: 7,
	}
}

// TestRetirementPlan_TheDigestCoversEachDestructionSeparately is what makes an
// approval bind to the destruction rather than to the word.
//
// The row that matters is drops_table. Its statement removes the relation and
// every row in it, while drops_column removes columns from a relation the
// application keeps -- so an approval given for one must not authorize the
// other. Two booleans reaching one digest component, or a component whose
// label was written and whose value was not, would make them the same plan.
func TestRetirementPlan_TheDigestCoversEachDestructionSeparately(t *testing.T) {
	tests := []struct {
		name string
		plan embedcutover.RetirementPlan
	}{
		{name: "drops index", plan: embedcutover.RetirementPlan{
			Generation: "gen-1", Schema: "public", Table: "article_vectors",
			Column: "embedding", RowCount: 7, DropsIndex: true,
		}},
		{name: "drops column", plan: embedcutover.RetirementPlan{
			Generation: "gen-1", Schema: "public", Table: "article_vectors",
			Column: "embedding", RowCount: 7, DropsColumn: true,
		}},
		{name: "drops table", plan: embedcutover.RetirementPlan{
			Generation: "gen-1", Schema: "public", Table: "article_vectors",
			Column: "embedding", RowCount: 7, DropsTable: true,
		}},
	}

	base := baseRetirementPlan()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.plan.Digest(), qt.Not(qt.Equals), base.Digest(),
				qt.Commentf("a plan that %s has the digest of one that destroys nothing", test.name))
		})
	}
}

// TestRetirementPlan_DroppingAColumnIsNotDroppingATable is the discrimination
// the table above cannot make on its own.
//
// Each row there differs from a plan that destroys nothing, which a digest
// carrying a single "destroys something" bit would satisfy. This asks the
// question that bit gets wrong.
func TestRetirementPlan_DroppingAColumnIsNotDroppingATable(t *testing.T) {
	c := qt.New(t)

	column := baseRetirementPlan()
	column.DropsColumn = true
	table := baseRetirementPlan()
	table.DropsTable = true

	c.Assert(column.Digest(), qt.Not(qt.Equals), table.Digest())
}
