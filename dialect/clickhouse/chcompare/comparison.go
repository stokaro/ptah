// Package chcompare compares ClickHouse table and index facets using captured source
// knowledge. Defaults are resolved by chprepare before comparison.
package chcompare

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/clickhouse/internal/chsql"
)

// Service compares resolved table settings without database access. Its zero
// value is ready for concurrent use. Creation and removal remain parent-owned;
// changes capture both operands only for surviving tables with usable evidence.
type Service struct{}

// CompareFacets returns a complete comparison, preserving desired declarations.
// Unavailable current state returns an undecided diagnostic, never an empty
// successful diff. Invalid input, unresolved defaults, errors, and cancellation
// return a zero result. Inputs remain unchanged.
func (Service) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	if ctx == nil {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	if request.Target != platform.ClickHouse {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: ClickHouse comparison on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{chschema.TableKind}) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: unsupported ClickHouse facet comparison kinds", schemaext.ErrInvalidValue)
	}
	result := schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired}
	result.Desired.Records = slices.Clone(result.Desired.Records)
	seen := make(map[objectidentity.Key]bool)
	for _, owner := range request.Owners {
		if owner.Subject.Kind != objectidentity.KindTable || owner.Subject.Name.Empty() || seen[owner.Subject.Key()] || (!owner.Desired && !owner.Current) {
			return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: invalid or duplicate ClickHouse facet owner", schemaext.ErrInvalidValue)
		}
		seen[owner.Subject.Key()] = true
		if !request.Includes(chschema.TableKind, owner.Subject) {
			continue
		}
		if err := compareTable(request, owner, &result); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	return result, nil
}

func compareTable(request schemaext.FacetComparisonRequest, owner schemaext.ParentState, result *schemaext.FacetComparisonResult) error {
	desired, current, err := tableValues(request, owner.Subject)
	if err != nil {
		return err
	}
	if !owner.Desired {
		return nil
	}
	if limited(request.Desired.Coverage, owner.Subject) {
		undecided(result, owner.Subject, "the desired source could not describe ClickHouse table settings")
		return nil
	}
	if !owner.Current {
		return nil
	}
	// A model selected by another table does not create intent on this one.
	// Keep unmentioned, uninspected settings unmanaged; explicit knowledge
	// limits still need an undecided result rather than a fabricated value.
	if desired == nil && current == nil &&
		request.Desired.Coverage.Lookup(chschema.TableKind, owner.Subject).State == schemaext.Uninspected &&
		request.Current.Coverage.Lookup(chschema.TableKind, owner.Subject).State == schemaext.Uninspected &&
		!limited(request.Current.Coverage, owner.Subject) {
		return nil
	}
	if current == nil || limited(request.Current.Coverage, owner.Subject) {
		undecided(result, owner.Subject, "ClickHouse table settings were not fully inspected")
		return nil
	}
	if desired == nil {
		knowledge := request.Desired.Coverage.Lookup(chschema.TableKind, owner.Subject)
		if knowledge.State == schemaext.Defaulted || knowledge.State == schemaext.Absent || knowledge.State == schemaext.Complete {
			return fmt.Errorf("%w: ClickHouse table settings require an explicit resolved declaration", schemaext.ErrInvalidValue)
		}
		facets, err := schemaext.NewFacets(current.Desired())
		if err != nil {
			return err
		}
		result.Desired.Records = append(result.Desired.Records, schemaext.FacetRecord{Subject: owner.Subject, Values: facets})
		return nil
	}
	projected, err := desired.Observed()
	if err != nil {
		return err
	}
	if chsql.SameTable(projected, current) {
		return nil
	}
	result.Changes = append(result.Changes, schemaext.FacetChange{
		Kind: chschema.TableKind, Change: schemaext.ChangeRecord{Subject: owner.Subject, Value: &chdiff.Table{Before: current, After: desired}},
	})
	return nil
}

func tableValues(request schemaext.FacetComparisonRequest, subject objectidentity.ID) (*chschema.DesiredTable, *chschema.ObservedTable, error) {
	desired, _, err := schemaext.FacetAs[*chschema.DesiredTable](values(request.Desired, subject), chschema.TableKind)
	if err != nil {
		return nil, nil, err
	}
	current, _, err := schemaext.FacetAs[*chschema.ObservedTable](values(request.Current, subject), chschema.TableKind)
	if err != nil {
		return nil, nil, err
	}
	if current != nil {
		if err := chschema.ValidateObserved(current); err != nil {
			return nil, nil, err
		}
	}
	if desired != nil {
		if _, err := desired.Observed(); err != nil {
			return nil, nil, err
		}
	}
	return desired, current, nil
}

func values(state schemaext.FacetState, subject objectidentity.ID) schemaext.Facets {
	for _, record := range state.Records {
		if record.Subject.Key() == subject.Key() {
			return record.Values
		}
	}
	return schemaext.Facets{}
}

func limited(coverage schemaext.Coverage, subject objectidentity.ID) bool {
	knowledge, found := coverage.SubjectKnowledge(chschema.TableKind, subject)
	return found && (knowledge.State == schemaext.Uninspected || knowledge.State == schemaext.Unrepresentable)
}

func undecided(result *schemaext.FacetComparisonResult, subject objectidentity.ID, reason string) {
	result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: chschema.TableKind, Subject: subject, Reason: reason})
}
