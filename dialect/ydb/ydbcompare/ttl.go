package ydbcompare

import (
	"context"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/rowdeletion"
)

// TTLService compares the TTL of YDB tables without database access. Its zero
// value is ready for concurrent use. Creation and removal of a table carry its
// TTL with the table; changes describe only tables both sides hold.
type TTLService struct{}

var ttlFacet = rowdeletion.Facet[*ydbschema.DesiredTTL, *ydbschema.ObservedTTL]{
	Target:           platform.YDB,
	Kind:             ydbschema.TTLKind,
	Capability:       capability.RowDeletionPolicy,
	Name:             "YDB TTL",
	ValidateDesired:  ydbschema.ValidateDesiredTTL,
	ValidateObserved: ydbschema.ValidateObservedTTL,
	Equivalent: func(semantics identifier.Semantics, desired *ydbschema.DesiredTTL, current *ydbschema.ObservedTTL) bool {
		return ydbschema.EquivalentTTL(desired.Policy, current.Policy, semantics.ColumnIdentityKey)
	},
	Change: func(before *ydbschema.ObservedTTL, _ bool, after *ydbschema.DesiredTTL, _ bool) schemaext.ChangeValue {
		return &ydbdiff.TTL{Before: before, After: after}
	},
	Adopt: (*ydbschema.ObservedTTL).Desired,
}

// CompareFacets returns a complete comparison and preserves desired
// declarations. A declaration with complete source knowledge manages the TTL:
// a table it leaves without a value requests none. A source that could not
// declare the TTL leaves it unmanaged, and the observed TTL is adopted into the
// effective declaration so a later rebuild keeps it. Unknown current state
// with managed intent returns an undecided diagnostic, never an empty
// successful diff. The interval is compared in the whole seconds YDB keeps,
// the unit exactly and the column under the request's identifier rules.
// Invalid input and cancellation return a zero result.
func (TTLService) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	return ttlFacet.Compare(ctx, request)
}
