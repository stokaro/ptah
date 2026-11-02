package pgtable_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
)

// TestExclusionComparison_CountsWhatItCompares pins the counts the inventory
// reports for EXCLUDE constraints: a constraint both sides hold on a table both
// sides hold is matched, also where the plan drops it and creates it again, an
// addition is added and a drop removed. A constraint the comparison adopts,
// cannot decide, is required to keep, or leaves to its table's creation or
// removal is counted on neither side.
func TestExclusionComparison_CountsWhatItCompares(t *testing.T) {
	declared := func() *pgtable.DesiredExclusion { return &pgtable.DesiredExclusion{Exclusion: exclusionDefinition()} }
	held := func() *pgtable.ObservedExclusion { return new(pgtable.ObservedExclusion(exclusionDefinition())) }
	tests := []struct {
		name    string
		request func() schemaext.ObjectComparisonRequest
		want    schemaext.ObjectCount
	}{
		{name: "a constraint that agrees", want: schemaext.ObjectCount{Kind: pgtable.ExclusionChangeKind, Matched: 1},
			request: func() schemaext.ObjectComparisonRequest { return exclusionComparison(declared(), held()) }},
		{name: "a constraint the plan replaces", want: schemaext.ObjectCount{Kind: pgtable.ExclusionChangeKind, Matched: 1},
			request: func() schemaext.ObjectComparisonRequest {
				return exclusionComparison(&pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "gist", Elements: "during WITH &&"}}, held())
			}},
		{name: "an addition", want: schemaext.ObjectCount{Kind: pgtable.ExclusionChangeKind, Added: 1},
			request: func() schemaext.ObjectComparisonRequest { return exclusionComparison(declared(), nil) }},
		{name: "a drop", want: schemaext.ObjectCount{Kind: pgtable.ExclusionChangeKind, Removed: 1},
			request: func() schemaext.ObjectComparisonRequest { return exclusionComparison(nil, held()) }},
		{name: "a declaration the read could not decide", want: schemaext.ObjectCount{Kind: pgtable.ExclusionChangeKind},
			request: func() schemaext.ObjectComparisonRequest {
				request := exclusionComparison(declared(), nil)
				request.Current.Coverage = must.Must(pgtable.ExclusionCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "reader limit"}, nil))
				return request
			}},
		{name: "a constraint the desired source did not describe", want: schemaext.ObjectCount{Kind: pgtable.ExclusionChangeKind},
			request: func() schemaext.ObjectComparisonRequest {
				request := exclusionComparison(nil, held())
				request.Desired.Coverage = schemaext.Coverage{}
				return request
			}},
		{name: "a constraint the declared schema requires", want: schemaext.ObjectCount{Kind: pgtable.ExclusionChangeKind},
			request: func() schemaext.ObjectComparisonRequest {
				request := exclusionComparison(nil, held())
				request.Required = []objectidentity.ID{exclusionSubject()}
				return request
			}},
		{name: "a table the plan creates", want: schemaext.ObjectCount{Kind: pgtable.ExclusionChangeKind},
			request: func() schemaext.ObjectComparisonRequest {
				request := exclusionComparison(declared(), nil)
				request.Parents[0].Current = false
				return request
			}},
		{name: "a table the plan drops", want: schemaext.ObjectCount{Kind: pgtable.ExclusionChangeKind},
			request: func() schemaext.ObjectComparisonRequest {
				request := exclusionComparison(nil, held())
				request.Parents[0].Desired = false
				return request
			}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := (pgtable.ExclusionService{}).CompareObjects(t.Context(), test.request())

			c.Assert(err, qt.IsNil)
			c.Assert(result.Counts, qt.DeepEquals, []schemaext.ObjectCount{test.want})
		})
	}
}
