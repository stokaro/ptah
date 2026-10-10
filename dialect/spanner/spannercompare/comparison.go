// Package spannercompare compares Spanner row deletion policies using captured
// source knowledge. It reads the interval the server rewrites through the
// number of hours it denotes.
package spannercompare

import (
	"context"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/spanner/spannerdiff"
	"ptah.run/dialect/spanner/spannerschema"
	"ptah.run/internal/rowdeletion"
)

// Service compares row deletion policies without database access. Its zero
// value is ready for concurrent use. Creation and removal of a table carry its
// policy with the table; changes describe only tables both sides hold.
type Service struct{}

var facet = rowdeletion.Facet[*spannerschema.DesiredRowDeletion, *spannerschema.ObservedRowDeletion]{
	Target:           platform.Spanner,
	Kind:             spannerschema.RowDeletionKind,
	Capability:       capability.RowDeletionPolicy,
	Name:             "Spanner row deletion policy",
	ValidateDesired:  spannerschema.ValidateDesired,
	ValidateObserved: spannerschema.ValidateObserved,
	Equivalent: func(semantics identifier.Semantics, desired *spannerschema.DesiredRowDeletion, current *spannerschema.ObservedRowDeletion) bool {
		return spannerschema.Equivalent(desired.Policy, current.Policy, semantics.ColumnIdentityKey)
	},
	Change: func(before *spannerschema.ObservedRowDeletion, _ bool, after *spannerschema.DesiredRowDeletion, _ bool) schemaext.ChangeValue {
		return &spannerdiff.RowDeletion{Before: before, After: after}
	},
	Adopt: (*spannerschema.ObservedRowDeletion).Desired,
}

// CompareFacets returns a complete comparison and preserves desired
// declarations. A declaration with complete source knowledge manages the
// policy: a table it leaves without a value requests none. A source that could
// not declare the policy leaves it unmanaged, and the observed policy is
// adopted into the effective declaration so a later rebuild keeps it. Unknown
// current state with managed intent returns an undecided diagnostic, never an
// empty successful diff. The interval is compared in hours and the column
// under the request's identifier rules. Invalid input and cancellation return
// a zero result.
func (Service) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	return facet.Compare(ctx, request)
}
