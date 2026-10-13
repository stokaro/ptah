package mssqlcompare

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqldiff"
	"ptah.run/dialect/mssql/mssqlschema"
)

// ExecuteAsService compares the principal SQL Server routines run as. Its
// zero value is ready for concurrent use.
type ExecuteAsService struct{}

// CompareFacets reports a change for a routine both sides hold whose
// declared principal differs from the one it runs as, the names compared as
// the request's identifier rules compare them. A routine with no named principal
// on a side runs as its owner or its caller there, which the common security
// mode compares.
//
// A declaration without a principal names none when its source claims to
// describe the clause, and a current routine without one runs as none when
// its read claims so. A source that makes no claim, such as a Go annotation,
// which has no way to write the clause, leaves the principal unmanaged: the
// routine keeps the one it runs as, and no change removes it. Replacing such a
// routine would create it without the principal, which the host refuses. A
// declared principal against a read that makes no claim is undecided, and so
// is an explicit knowledge limit on either side. The declarations
// are returned as they were. Invalid input and cancellation return a zero
// result.
func (ExecuteAsService) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	if ctx == nil {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	if platform.NormalizeDialect(request.Target) != platform.SQLServer {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: SQL Server EXECUTE AS comparison on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{mssqlschema.ExecuteAsKind}) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: unsupported SQL Server EXECUTE AS comparison kinds", schemaext.ErrInvalidValue)
	}
	result := schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired}
	result.Desired.Records = slices.Clone(result.Desired.Records)
	for _, owner := range request.Owners {
		if err := ctx.Err(); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		if !mssqlschema.HoldsExecuteAs(owner.Subject) {
			return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: an EXECUTE AS principal attaches to a routine, not %s", schemaext.ErrInvalidValue, owner.Subject)
		}
		change, undecided, err := compareExecuteAs(request, owner)
		if err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		if change != nil {
			result.Changes = append(result.Changes, schemaext.FacetChange{Kind: mssqlschema.ExecuteAsKind,
				Change: schemaext.ChangeRecord{Subject: owner.Subject, Value: change}})
		}
		if undecided != "" {
			result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: mssqlschema.ExecuteAsKind, Subject: owner.Subject, Reason: undecided})
		}
	}
	return result, ctx.Err()
}

// compareExecuteAs returns the change one routine needs, or the reason it
// cannot be decided. Each value is validated, a routine on one side only
// included.
func compareExecuteAs(request schemaext.FacetComparisonRequest, owner schemaext.ParentState) (*mssqldiff.ExecuteAs, string, error) {
	desired, current, err := executeAsValues(request, owner.Subject)
	if err != nil {
		return nil, "", err
	}
	if !owner.Desired || !owner.Current || !request.Includes(mssqlschema.ExecuteAsKind, owner.Subject) {
		return nil, "", nil
	}
	if limitedExecuteAs(request.Desired.Coverage, owner.Subject) {
		return nil, "the desired source could not describe the principal the routine runs as", nil
	}
	if limitedExecuteAs(request.Current.Coverage, owner.Subject) {
		return nil, "the principal the routine runs as was not fully inspected", nil
	}
	if desired == nil && !describesExecuteAs(request.Desired.Coverage, owner.Subject) {
		return nil, "", nil
	}
	if current == nil && !describesExecuteAs(request.Current.Coverage, owner.Subject) {
		if desired != nil {
			return nil, "the current source does not say which principal the routine runs as", nil
		}
		return nil, "", nil
	}
	switch {
	case desired == nil && current == nil:
		return nil, "", nil
	case desired != nil && current != nil && (mssqlschema.SamePrincipal(desired.Principal, current.Principal) || request.Identifiers.TableIdentityKey(desired.Principal) == request.Identifiers.TableIdentityKey(current.Principal)):
		return nil, "", nil
	}
	change := &mssqldiff.ExecuteAs{}
	if current != nil {
		change.Before = new(*current)
	}
	if desired != nil {
		change.After = new(*desired)
	}
	return change, "", nil
}

// executeAsValues returns the principal each side gives subject, nil for
// none, each one validated.
func executeAsValues(request schemaext.FacetComparisonRequest, subject objectidentity.ID) (*mssqlschema.DesiredExecuteAs, *mssqlschema.ObservedExecuteAs, error) {
	desired, _, err := schemaext.FacetAs[*mssqlschema.DesiredExecuteAs](routineFacets(request.Desired, subject), mssqlschema.ExecuteAsKind)
	if err != nil {
		return nil, nil, err
	}
	if desired != nil {
		if err := mssqlschema.ValidateDesiredExecuteAs(desired); err != nil {
			return nil, nil, err
		}
	}
	current, _, err := schemaext.FacetAs[*mssqlschema.ObservedExecuteAs](routineFacets(request.Current, subject), mssqlschema.ExecuteAsKind)
	if err != nil {
		return nil, nil, err
	}
	if current != nil {
		if err := mssqlschema.ValidateObservedExecuteAs(current); err != nil {
			return nil, nil, err
		}
	}
	return desired, current, nil
}

// describesExecuteAs reports a claim that the source describes the clause of
// subject, so a routine it gives no principal runs as none.
func describesExecuteAs(coverage schemaext.Coverage, subject objectidentity.ID) bool {
	switch coverage.Lookup(mssqlschema.ExecuteAsKind, subject).State {
	case schemaext.Complete, schemaext.Absent, schemaext.Defaulted:
		return true
	default:
		return false
	}
}

// limitedExecuteAs reports an explicit claim that the source could not
// describe the clause of subject.
func limitedExecuteAs(coverage schemaext.Coverage, subject objectidentity.ID) bool {
	knowledge, found := coverage.SubjectKnowledge(mssqlschema.ExecuteAsKind, subject)
	return found && (knowledge.State == schemaext.Uninspected || knowledge.State == schemaext.Unrepresentable)
}

func routineFacets(state schemaext.FacetState, subject objectidentity.ID) schemaext.Facets {
	for _, record := range state.Records {
		if record.Subject.Key() == subject.Key() {
			return record.Values
		}
	}
	return schemaext.Facets{}
}
