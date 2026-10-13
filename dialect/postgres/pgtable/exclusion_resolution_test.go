package pgtable_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dialect/postgres/pgtable"
)

// A server spelling is evidence about this declaration. Without that evidence,
// the declared expression still decides whether a constraint differs.
func TestExclusionComparison_UsesServerExpressions(t *testing.T) {
	const storedWhere = "(n >= (0)::numeric)"
	for _, test := range []struct {
		name, where string
		normalized  *pgtable.NormalizedExclusion
		wantChanges int
	}{
		{name: "server reports the catalog definition", where: "n >= 0",
			normalized: &pgtable.NormalizedExclusion{Elements: "r WITH =", WhereCondition: storedWhere}},
		{name: "server reports a different predicate", where: "n >= 1", wantChanges: 1,
			normalized: &pgtable.NormalizedExclusion{Elements: "r WITH =", WhereCondition: "(n >= (1)::numeric)"}},
		{name: "server reports different elements", where: "n >= 0", wantChanges: 1,
			normalized: &pgtable.NormalizedExclusion{Elements: "r WITH <>", WhereCondition: storedWhere}},
		{name: "server refuses a declaration with matching text", where: storedWhere},
		{name: "no server evidence for different text", where: "n >= 0", wantChanges: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := resolvedExclusion(test.where, test.normalized)
			observed := &pgtable.ObservedExclusion{UsingMethod: "gist", Elements: "r WITH =", WhereCondition: storedWhere}
			result, err := (pgtable.ExclusionService{}).CompareObjects(t.Context(), exclusionComparison(desired, observed))
			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Undecided, qt.HasLen, 0)
			c.Assert(result.Changes, qt.HasLen, test.wantChanges)
			c.Assert(desired.WhereCondition, qt.Equals, test.where)
		})
	}
}

func resolvedExclusion(where string, normalized *pgtable.NormalizedExclusion) *pgtable.DesiredExclusion {
	desired := &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "gist", Elements: "r WITH =", WhereCondition: where}}
	if normalized != nil {
		fact := *normalized
		fact.SourceKey = pgtable.ExclusionComparisonKey(desired.Exclusion)
		desired.Normalized = &fact
	}
	return desired
}

func TestExclusionComparison_AccessMethodIgnoresCase(t *testing.T) {
	c := qt.New(t)
	desired := resolvedExclusion("", nil)
	desired.UsingMethod = "GIST"
	observed := &pgtable.ObservedExclusion{UsingMethod: "gist", Elements: "r WITH ="}
	result, err := (pgtable.ExclusionService{}).CompareObjects(t.Context(), exclusionComparison(desired, observed))
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Undecided, qt.HasLen, 0)
	c.Assert(result.Changes, qt.HasLen, 0)
}
