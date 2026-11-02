package pgtypeprovider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
)

// typeCounts is the count of each kind's change, in the order the runtime
// returns them: domains, composite types and range types counted as given.
func typeCounts(domains, composites, ranges schemaext.ObjectCount) []schemaext.ObjectCount {
	composites.Kind, domains.Kind, ranges.Kind = pgtype.CompositeChangeKind, pgtype.DomainChangeKind, pgtype.RangeChangeKind
	return []schemaext.ObjectCount{composites, domains, ranges}
}

// TestCompareTypes_CountsWhatItCompares pins the counts the inventory reports
// for user types, under the change kind of each model: a type both sides hold
// is matched whether or not it differs, a creation is added and a drop
// removed. A type the comparison leaves alone or cannot decide is counted on
// neither side, and a kind no type of the batch has counts zero.
func TestCompareTypes_CountsWhatItCompares(t *testing.T) {
	composite := &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "n", Type: "integer"}}}
	tests := []struct {
		name    string
		request func() schemaext.ObjectComparisonRequest
		want    []schemaext.ObjectCount
	}{
		{name: "a domain that agrees", want: typeCounts(schemaext.ObjectCount{Matched: 1}, schemaext.ObjectCount{}, schemaext.ObjectCount{}),
			request: func() schemaext.ObjectComparisonRequest {
				return comparisonOf(values(&pgtype.DesiredDomain{BaseType: "text"}), values(&pgtype.ObservedDomain{BaseType: "text"}))
			}},
		{name: "a domain that differs", want: typeCounts(schemaext.ObjectCount{Matched: 1}, schemaext.ObjectCount{}, schemaext.ObjectCount{}),
			request: func() schemaext.ObjectComparisonRequest {
				return comparisonOf(values(&pgtype.DesiredDomain{BaseType: "integer"}), values(&pgtype.ObservedDomain{BaseType: "text"}))
			}},
		{name: "a created composite type and a dropped range type",
			want: typeCounts(schemaext.ObjectCount{}, schemaext.ObjectCount{Added: 1}, schemaext.ObjectCount{Removed: 1}),
			request: func() schemaext.ObjectComparisonRequest {
				return comparisonOf(values(&pgtype.DesiredComposite{Attributes: composite.Attributes}), values(&pgtype.ObservedRange{Subtype: "integer"}))
			}},
		{name: "a domain the read could not decide", want: typeCounts(schemaext.ObjectCount{}, schemaext.ObjectCount{}, schemaext.ObjectCount{}),
			request: func() schemaext.ObjectComparisonRequest {
				request := comparisonOf(values(&pgtype.DesiredDomain{BaseType: "text"}), nil)
				request.Current.Coverage = must.Must(pgtype.Coverage(pgtype.DomainKind, schemaext.Observed,
					schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "this read lists no domains"}, nil))
				return request
			}},
		{name: "a composite type the desired source did not describe", want: typeCounts(schemaext.ObjectCount{}, schemaext.ObjectCount{}, schemaext.ObjectCount{}),
			request: func() schemaext.ObjectComparisonRequest {
				request := comparisonOf(nil, values(composite))
				request.Desired.Coverage = must.Must(pgtype.Coverage(pgtype.CompositeKind, schemaext.Desired,
					schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the document leaves composite types out"}, nil))
				return request
			}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := newRuntime(c).CompareObjects(t.Context(), test.request())

			c.Assert(err, qt.IsNil)
			c.Assert(result.Counts, qt.DeepEquals, test.want)
		})
	}
}
