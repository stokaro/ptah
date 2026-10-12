package mssqlcompare

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqldiff"
	"ptah.run/dialect/mssql/mssqlschema"
)

// RoutineOptionsService compares the options SQL Server routines' headers
// write, the principal they run as among them. Its zero value is ready for
// concurrent use.
type RoutineOptionsService struct{}

// CompareFacets reports a change for a routine both sides hold whose
// declared options differ from the ones it holds, the principals compared as
// the request's identifier rules compare them. A routine with no named
// principal on a side runs as its owner or its caller there, which the common
// security mode compares.
//
// A declaration without the facet states no option when its source claims to
// describe the options, and a current routine without one holds none when its
// read claims so. A source that makes no claim, such as a Go annotation,
// which has no way to write the options, leaves them unmanaged: the routine
// keeps the ones it holds, and no change removes them. Replacing such a
// routine would create it without them, which the host refuses. Declared
// options against a read that makes no claim are undecided, and so is an
// explicit knowledge limit on either side, such as a routine created WITH
// ENCRYPTION, whose reason the read names. The declarations are returned as
// they were. Invalid input and cancellation return a zero result.
func (RoutineOptionsService) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	if ctx == nil {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	if platform.NormalizeDialect(request.Target) != platform.SQLServer {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: SQL Server routine options comparison on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{mssqlschema.RoutineOptionsKind}) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: unsupported SQL Server routine options comparison kinds", schemaext.ErrInvalidValue)
	}
	result := schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired}
	result.Desired.Records = slices.Clone(result.Desired.Records)
	for _, owner := range request.Owners {
		if err := ctx.Err(); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		if !mssqlschema.HoldsRoutineOptions(owner.Subject) {
			return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: routine options attach to a routine, not %s", schemaext.ErrInvalidValue, owner.Subject)
		}
		change, undecided, err := compareRoutineOptions(request, owner)
		if err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		if change != nil {
			result.Changes = append(result.Changes, schemaext.FacetChange{Kind: mssqlschema.RoutineOptionsKind,
				Change: schemaext.ChangeRecord{Subject: owner.Subject, Value: change}})
		}
		if undecided != "" {
			result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: mssqlschema.RoutineOptionsKind, Subject: owner.Subject, Reason: undecided})
		}
	}
	return result, ctx.Err()
}

// compareRoutineOptions returns the change one routine needs, or the reason
// it cannot be decided. Each value is validated, a routine on one side only
// included.
func compareRoutineOptions(request schemaext.FacetComparisonRequest, owner schemaext.ParentState) (*mssqldiff.RoutineOptions, string, error) {
	desired, current, err := routineOptionsValues(request, owner.Subject)
	if err != nil {
		return nil, "", err
	}
	if !owner.Desired || !owner.Current || !request.Includes(mssqlschema.RoutineOptionsKind, owner.Subject) {
		return nil, "", nil
	}
	if reason, limited := limitedRoutineOptions(request.Desired.Coverage, owner.Subject, "the desired source could not describe the routine's options"); limited {
		return nil, reason, nil
	}
	if reason, limited := limitedRoutineOptions(request.Current.Coverage, owner.Subject, "the routine's options were not fully inspected"); limited {
		return nil, reason, nil
	}
	if desired == nil && !describesRoutineOptions(request.Desired.Coverage, owner.Subject) {
		return nil, "", nil
	}
	if current == nil && !describesRoutineOptions(request.Current.Coverage, owner.Subject) {
		if desired != nil {
			return nil, "the current source does not say which options the routine holds", nil
		}
		return nil, "", nil
	}
	switch {
	case desired == nil && current == nil:
		return nil, "", nil
	case desired != nil && current != nil && sameRoutineOptions(request.Identifiers, mssqlschema.RoutineOptions(*desired), mssqlschema.RoutineOptions(*current)):
		return nil, "", nil
	}
	change := &mssqldiff.RoutineOptions{}
	if current != nil {
		change.Before = new(*current)
	}
	if desired != nil {
		change.After = new(*desired)
	}
	return change, "", nil
}

// routineOptionsValues returns the options each side gives subject, nil for
// none, each one validated.
func routineOptionsValues(request schemaext.FacetComparisonRequest, subject objectidentity.ID) (*mssqlschema.DesiredRoutineOptions, *mssqlschema.ObservedRoutineOptions, error) {
	desired, _, err := schemaext.FacetAs[*mssqlschema.DesiredRoutineOptions](routineFacets(request.Desired, subject), mssqlschema.RoutineOptionsKind)
	if err != nil {
		return nil, nil, err
	}
	if desired != nil {
		if err := mssqlschema.ValidateDesiredRoutineOptions(desired); err != nil {
			return nil, nil, err
		}
	}
	current, _, err := schemaext.FacetAs[*mssqlschema.ObservedRoutineOptions](routineFacets(request.Current, subject), mssqlschema.RoutineOptionsKind)
	if err != nil {
		return nil, nil, err
	}
	if current != nil {
		if err := mssqlschema.ValidateObservedRoutineOptions(current); err != nil {
			return nil, nil, err
		}
	}
	return desired, current, nil
}

// describesRoutineOptions reports a claim that the source describes the
// options of subject, so a routine it gives no facet holds none.
func describesRoutineOptions(coverage schemaext.Coverage, subject objectidentity.ID) bool {
	switch coverage.Lookup(mssqlschema.RoutineOptionsKind, subject).State {
	case schemaext.Complete, schemaext.Absent, schemaext.Defaulted:
		return true
	default:
		return false
	}
}

// limitedRoutineOptions reports an explicit claim that the source could not
// describe the options of subject, with the reason the source gives, or
// fallback where it gives none.
func limitedRoutineOptions(coverage schemaext.Coverage, subject objectidentity.ID, fallback string) (string, bool) {
	knowledge, found := coverage.SubjectKnowledge(mssqlschema.RoutineOptionsKind, subject)
	if !found || (knowledge.State != schemaext.Uninspected && knowledge.State != schemaext.Unrepresentable) {
		return "", false
	}
	if knowledge.Reason != "" {
		return knowledge.Reason, true
	}
	return fallback, true
}

func routineFacets(state schemaext.FacetState, subject objectidentity.ID) schemaext.Facets {
	for _, record := range state.Records {
		if record.Subject.Key() == subject.Key() {
			return record.Values
		}
	}
	return schemaext.Facets{}
}

// sameRoutineOptions applies catalog name equivalence before comparing values.
// Copies keep the declaration's principal spelling intact.
func sameRoutineOptions(semantics identifier.Semantics, desired, current mssqlschema.RoutineOptions) bool {
	if semantics.TableIdentityKey(desired.Principal) == semantics.TableIdentityKey(current.Principal) {
		desired.Principal = current.Principal
	}
	return desired.Same(current)
}
