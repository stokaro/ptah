package engine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
)

// countingComparison is an object comparison that changes nothing and counts
// the objects of its batch with counts.
func countingComparison(counts ...schemaext.ObjectCount) schemaext.ObjectComparisonService {
	return comparisonFunc(func(ctx context.Context, r schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
		result, err := unchangedComparison(ctx, r)
		result.Counts = counts
		return result, err
	})
}

// changingComparison is an object comparison that reports one change of
// comparedKind, for the first object, with counts.
func changingComparison(counts ...schemaext.ObjectCount) schemaext.ObjectComparisonService {
	return comparisonFunc(func(_ context.Context, r schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
		return schemaext.ObjectComparisonResult{
			Complete: true, Desired: r.Desired,
			Changes: []schemaext.ChangeRecord{{Subject: comparedRef(conversionFirst, "first"), Value: &comparedChange{Number: 1}}},
			Counts:  counts,
		}, nil
	})
}

// TestObjectComparison_ReturnsTheCountsAnOwnerStates passes a count through
// when the changes of its kind are what it says. The runtime cannot tell an
// addition from a removal, so one change may stand for either.
func TestObjectComparison_ReturnsTheCountsAnOwnerStates(t *testing.T) {
	tests := []struct {
		name  string
		count schemaext.ObjectCount
	}{
		{name: "an addition beside matched objects", count: schemaext.ObjectCount{Kind: comparedKind, Matched: 4, Added: 1}},
		{name: "a change to an object both sides hold", count: schemaext.ObjectCount{Kind: comparedKind, Matched: 1}},
		{name: "a removal", count: schemaext.ObjectCount{Kind: comparedKind, Matched: 2, Removed: 1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := mustRuntime(c, comparisonProvider(changingComparison(test.count)))

			result, err := runtime.CompareObjects(t.Context(), comparisonRequest(c))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Counts, qt.DeepEquals, []schemaext.ObjectCount{test.count})
		})
	}
}

// TestObjectComparison_SumsTheCountsOfEveryOwnerOfAChangeKind adds what two
// owners of one change kind count, since each counts only its own batch.
func TestObjectComparison_SumsTheCountsOfEveryOwnerOfAChangeKind(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, splitComparisonProvider(
		countingComparison(schemaext.ObjectCount{Kind: comparedKind, Matched: 1}),
		countingComparison(schemaext.ObjectCount{Kind: comparedKind, Matched: 2}),
	))

	result, err := runtime.CompareObjects(t.Context(), comparisonRequest(c))

	c.Assert(err, qt.IsNil)
	c.Assert(result.Counts, qt.DeepEquals, []schemaext.ObjectCount{{Kind: comparedKind, Matched: 3}})
}

// TestObjectComparison_LeavesOutAKindOneOfItsOwnersDidNotCount reports nothing
// for a change kind when one of the owners that ran did not count it: the
// other owner's count covers only part of the kind's objects and would read as
// all of them.
func TestObjectComparison_LeavesOutAKindOneOfItsOwnersDidNotCount(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, splitComparisonProvider(
		countingComparison(schemaext.ObjectCount{Kind: comparedKind, Matched: 1}),
		countingComparison(),
	))

	result, err := runtime.CompareObjects(t.Context(), comparisonRequest(c))

	c.Assert(err, qt.IsNil)
	c.Assert(result.Counts, qt.IsNil)
}

// TestObjectComparison_RejectsCountsItsChangesContradict refuses a reply whose
// counts cannot describe its changes: one change of comparedKind is an
// addition, a removal or a change to a matched object, and never two of them.
func TestObjectComparison_RejectsCountsItsChangesContradict(t *testing.T) {
	tests := []struct {
		name   string
		counts []schemaext.ObjectCount
	}{
		{name: "a kind the owner does not report changes of", counts: []schemaext.ObjectCount{{Kind: "example.org/other", Matched: 1}}},
		{name: "a kind counted twice", counts: []schemaext.ObjectCount{{Kind: comparedKind, Matched: 1}, {Kind: comparedKind, Matched: 1}}},
		{name: "a negative count the sums allow", counts: []schemaext.ObjectCount{{Kind: comparedKind, Matched: 2, Removed: -1}}},
		{name: "more additions than changes", counts: []schemaext.ObjectCount{{Kind: comparedKind, Added: 2}}},
		{name: "more removals than changes", counts: []schemaext.ObjectCount{{Kind: comparedKind, Removed: 2}}},
		{name: "one change counted as an addition and a removal", counts: []schemaext.ObjectCount{{Kind: comparedKind, Added: 1, Removed: 1}}},
		{name: "a change of no object counted", counts: []schemaext.ObjectCount{{Kind: comparedKind}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := mustRuntime(c, comparisonProvider(changingComparison(test.counts...)))

			result, err := runtime.CompareObjects(t.Context(), comparisonRequest(c))

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result.Counts, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
		})
	}
}

// TestFacetComparison_ReturnsTheCountsAnOwnerStates passes the count of a
// facet owner through, checked against its changes as an object owner's is.
func TestFacetComparison_ReturnsTheCountsAnOwnerStates(t *testing.T) {
	c := qt.New(t)
	count := schemaext.ObjectCount{Kind: comparedKind, Matched: 3, Added: 1}
	runtime := mustRuntime(c, facetProvider(facetComparisonFunc(func(_ context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		reply := facetReply(request)
		reply.Counts = []schemaext.ObjectCount{count}
		return reply, nil
	})))

	result, err := runtime.CompareFacets(t.Context(), facetRequest())

	c.Assert(err, qt.IsNil)
	c.Assert(result.Counts, qt.DeepEquals, []schemaext.ObjectCount{count})
}

// TestFacetComparison_RejectsCountsItsChangesContradict refuses a facet reply
// whose one change is counted as no object.
func TestFacetComparison_RejectsCountsItsChangesContradict(t *testing.T) {
	c := qt.New(t)
	runtime := mustRuntime(c, facetProvider(facetComparisonFunc(func(_ context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
		reply := facetReply(request)
		reply.Counts = []schemaext.ObjectCount{{Kind: comparedKind}}
		return reply, nil
	})))

	result, err := runtime.CompareFacets(t.Context(), facetRequest())

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result.Counts, qt.IsNil)
}
