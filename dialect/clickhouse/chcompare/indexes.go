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

// IndexService compares resolved skipping-index settings. Common index
// creation, removal, and expression changes remain parent-owned. The zero
// value is ready for concurrent use without database access.
type IndexService struct{}

// CompareFacets captures setting changes only for surviving indexes with usable
// observations. An omitted declaration adopts observed settings; it never asks
// for defaults. Explicit knowledge limits produce undecided diagnostics. Errors
// and cancellation return no partial result. Inputs remain unchanged.
func (IndexService) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	if ctx == nil {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: index comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	if request.Target != platform.ClickHouse {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: ClickHouse index comparison on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{chschema.IndexKind}) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: unsupported ClickHouse index comparison kinds", schemaext.ErrInvalidValue)
	}
	result := schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired}
	result.Desired.Records = slices.Clone(result.Desired.Records)
	seen := make(map[objectidentity.Key]bool)
	for _, owner := range request.Owners {
		if err := ctx.Err(); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		if owner.Subject.Kind != objectidentity.KindIndex || owner.Subject.Name.Empty() || owner.Subject.Parent.Empty() ||
			seen[owner.Subject.Key()] || (!owner.Desired && !owner.Current) {
			return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: invalid or duplicate ClickHouse index owner", schemaext.ErrInvalidValue)
		}
		seen[owner.Subject.Key()] = true
		if !request.Includes(chschema.IndexKind, owner.Subject) {
			continue
		}
		if err := compareIndex(request, owner, &result); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	return result, nil
}

func compareIndex(request schemaext.FacetComparisonRequest, owner schemaext.ParentState, result *schemaext.FacetComparisonResult) error {
	desired, current, err := indexValues(request, owner.Subject)
	if err != nil || !owner.Desired {
		return err
	}
	if indexLimited(request.Desired.Coverage, owner.Subject) {
		indexUndecided(result, owner.Subject, "the desired source could not describe ClickHouse index settings")
		return nil
	}
	if !owner.Current {
		return nil
	}
	if desired == nil && current == nil &&
		request.Desired.Coverage.Lookup(chschema.IndexKind, owner.Subject).State == schemaext.Uninspected &&
		request.Current.Coverage.Lookup(chschema.IndexKind, owner.Subject).State == schemaext.Uninspected &&
		!indexLimited(request.Current.Coverage, owner.Subject) {
		return nil
	}
	if current == nil || indexLimited(request.Current.Coverage, owner.Subject) {
		indexUndecided(result, owner.Subject, "ClickHouse index settings were not fully inspected")
		return nil
	}
	if desired == nil {
		return adoptIndexSettings(result, owner.Subject, current)
	}
	projected, err := desired.Observed()
	if err != nil {
		return err
	}
	if chsql.SameIndex(projected, current) {
		return nil
	}
	result.Changes = append(result.Changes, schemaext.FacetChange{
		Kind: chschema.IndexKind, Change: schemaext.ChangeRecord{Subject: owner.Subject, Value: &chdiff.Index{Before: current, After: desired}},
	})
	return nil
}

func adoptIndexSettings(result *schemaext.FacetComparisonResult, subject objectidentity.ID, current *chschema.ObservedIndex) error {
	knowledge := result.Desired.Coverage.Lookup(chschema.IndexKind, subject)
	if knowledge.State == schemaext.Defaulted || knowledge.State == schemaext.Absent || knowledge.State == schemaext.Complete {
		return fmt.Errorf("%w: ClickHouse index settings require an explicit resolved declaration", schemaext.ErrInvalidValue)
	}
	adopted, err := values(result.Desired, subject).With(current.Desired())
	if err != nil {
		return err
	}
	for i := range result.Desired.Records {
		if result.Desired.Records[i].Subject.Key() == subject.Key() {
			result.Desired.Records[i].Values = adopted
			return nil
		}
	}
	result.Desired.Records = append(result.Desired.Records, schemaext.FacetRecord{Subject: subject, Values: adopted})
	return nil
}

func indexValues(request schemaext.FacetComparisonRequest, subject objectidentity.ID) (*chschema.DesiredIndex, *chschema.ObservedIndex, error) {
	desired, _, err := schemaext.FacetAs[*chschema.DesiredIndex](values(request.Desired, subject), chschema.IndexKind)
	if err != nil {
		return nil, nil, err
	}
	current, _, err := schemaext.FacetAs[*chschema.ObservedIndex](values(request.Current, subject), chschema.IndexKind)
	if err != nil {
		return nil, nil, err
	}
	if current != nil {
		if err := chschema.ValidateObservedIndex(current); err != nil {
			return nil, nil, err
		}
		if request.Current.Coverage.Lookup(chschema.IndexKind, subject).State == schemaext.Absent {
			return nil, nil, fmt.Errorf("%w: observed ClickHouse index settings are marked absent", schemaext.ErrInvalidValue)
		}
	}
	if desired != nil {
		if _, err := desired.Observed(); err != nil {
			return nil, nil, err
		}
	}
	return desired, current, nil
}

func indexLimited(coverage schemaext.Coverage, subject objectidentity.ID) bool {
	knowledge, found := coverage.SubjectKnowledge(chschema.IndexKind, subject)
	return found && (knowledge.State == schemaext.Uninspected || knowledge.State == schemaext.Unrepresentable)
}

func indexUndecided(result *schemaext.FacetComparisonResult, subject objectidentity.ID, reason string) {
	result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: chschema.IndexKind, Subject: subject, Reason: reason})
}
