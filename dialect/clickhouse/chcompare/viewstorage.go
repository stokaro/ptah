package chcompare

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
)

// ViewStorageService reports a materialized view undecided when either side
// could not describe its storage, with the reason that side recorded. Ptah
// creates every view with `ENGINE = MergeTree ORDER BY tuple()` and models no
// other storage, so a view a read saw stored another way is neither
// synchronized with a declaration nor something a plan could change. A view
// both sides describe, and a view only one side holds, compare as nothing
// here: creation and removal belong to the common view.
// [chschema.ViewStorageKind] carries no value, and one on either side is
// invalid. The zero value is ready for concurrent use without database access.
type ViewStorageService struct{}

// CompareFacets returns a complete comparison with the desired declarations
// unchanged and no changes. Errors and cancellation return no partial result.
// Inputs remain unchanged.
func (ViewStorageService) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	if ctx == nil {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: view storage comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	if request.Target != platform.ClickHouse {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: ClickHouse view storage comparison on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{chschema.ViewStorageKind}) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: unsupported ClickHouse view storage comparison kinds", schemaext.ErrInvalidValue)
	}
	if carriesViewStorage(request.Desired) || carriesViewStorage(request.Current) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: ClickHouse view storage carries no value", schemaext.ErrInvalidValue)
	}
	result := schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired}
	result.Desired.Records = slices.Clone(result.Desired.Records)
	seen := make(map[objectidentity.Key]bool)
	for _, owner := range request.Owners {
		if err := ctx.Err(); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		if owner.Subject.Kind != objectidentity.KindMatView || owner.Subject.Name.Empty() ||
			seen[owner.Subject.Key()] || (!owner.Desired && !owner.Current) {
			return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: invalid or duplicate ClickHouse materialized view owner", schemaext.ErrInvalidValue)
		}
		seen[owner.Subject.Key()] = true
		if !owner.Desired || !owner.Current || !request.Includes(chschema.ViewStorageKind, owner.Subject) {
			continue
		}
		if undecided, found := undescribedStorage(request, owner.Subject); found {
			result.Undecided = append(result.Undecided, undecided)
		}
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	return result, nil
}

// carriesViewStorage reports whether any record of state holds a value of
// [chschema.ViewStorageKind].
func carriesViewStorage(state schemaext.FacetState) bool {
	return slices.ContainsFunc(state.Records, func(record schemaext.FacetRecord) bool {
		return slices.Contains(record.Values.DeclaredKinds(), chschema.ViewStorageKind)
	})
}

// undescribedStorage is the undecided change for a view whose storage the
// read, or else the desired source, could not describe.
func undescribedStorage(request schemaext.FacetComparisonRequest, subject objectidentity.ID) (schemaext.UndecidedChange, bool) {
	for _, side := range []struct {
		coverage schemaext.Coverage
		name     string
	}{{request.Current.Coverage, "the read"}, {request.Desired.Coverage, "the desired source"}} {
		knowledge, found := side.coverage.SubjectKnowledge(chschema.ViewStorageKind, subject)
		if found && (knowledge.State == schemaext.Unrepresentable || knowledge.State == schemaext.Uninspected) {
			return schemaext.UndecidedChange{
				Kind: chschema.ViewStorageKind, Subject: subject,
				Reason: side.name + " could not describe the ClickHouse materialized view's storage: " + knowledge.Reason,
			}, true
		}
	}
	return schemaext.UndecidedChange{}, false
}
