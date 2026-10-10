package ydbcompare

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbindex"
)

// VectorIndexService compares the vector settings of YDB indexes without
// database access. Its zero value is ready for concurrent use. Creation and
// removal of an index, and a change of its kind or its columns, belong to the
// common index; this service describes only indexes both sides hold.
type VectorIndexService struct{}

// CompareFacets returns a complete comparison and preserves desired
// declarations.
//
// A declaration is read as the settings it builds an index with, through
// [ydbindex.ResolveVector], and compared setting by setting with what the
// index was built with. A difference is a change, and so is a declaration
// that does not resolve or one stated on an index the read found without
// vector settings: the planner builds the index again or refuses it with the
// reason, as the renderer does for a new index.
//
// An index whose declaration states no vector settings while the read found
// some is a common change of the index's kind, or, under a source that cannot
// describe the settings, an index whose settings are adopted into the
// effective declaration. A declaration or an observation the coverage marks
// as not described is undecided.
//
// Invalid input and cancellation return a zero result. Inputs are unchanged.
func (VectorIndexService) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	if ctx == nil {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	if request.Target != platform.YDB {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: YDB vector index comparison on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{ydbschema.VectorIndexKind}) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: unsupported YDB vector index comparison kinds", schemaext.ErrInvalidValue)
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
			return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: invalid or duplicate YDB vector index owner", schemaext.ErrInvalidValue)
		}
		seen[owner.Subject.Key()] = true
		if !request.Includes(ydbschema.VectorIndexKind, owner.Subject) {
			continue
		}
		if err := compareVectorIndex(request, owner, &result); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	return result, nil
}

// compareVectorIndex compares the settings of an index both sides hold. A new
// index is built from its declaration and a removed one goes with its
// definition, so neither needs a comparison here.
func compareVectorIndex(request schemaext.FacetComparisonRequest, owner schemaext.ParentState, result *schemaext.FacetComparisonResult) error {
	if !owner.Desired {
		return nil
	}
	if vectorLimited(request.Desired.Coverage, owner.Subject) {
		vectorUndecided(result, owner.Subject, "the desired source could not describe YDB vector index settings")
		return nil
	}
	if !owner.Current {
		return nil
	}
	desired, current, err := vectorValues(request, owner.Subject)
	if err != nil {
		return err
	}
	if desired == nil && current == nil {
		return nil
	}
	if vectorLimited(request.Current.Coverage, owner.Subject) ||
		(current == nil && request.Current.Coverage.Lookup(ydbschema.VectorIndexKind, owner.Subject).State != schemaext.Complete) {
		vectorUndecided(result, owner.Subject, "YDB vector index settings were not fully inspected")
		return nil
	}
	if desired == nil {
		if request.Desired.Coverage.Lookup(ydbschema.VectorIndexKind, owner.Subject).State == schemaext.Complete {
			return nil
		}
		return adoptVectorSettings(result, owner.Subject, current)
	}
	if current != nil {
		resolved, err := ydbindex.ResolveVector(new(desired.Settings()), "")
		if err == nil && resolved == current.Settings() {
			return nil
		}
	}
	result.Changes = append(result.Changes, schemaext.FacetChange{
		Kind: ydbschema.VectorIndexKind, Change: schemaext.ChangeRecord{Subject: owner.Subject, Value: &ydbdiff.VectorIndex{Before: current, After: desired}},
	})
	return nil
}

// adoptVectorSettings keeps the settings an index holds where the source
// could not say what they should be.
func adoptVectorSettings(result *schemaext.FacetComparisonResult, subject objectidentity.ID, current *ydbschema.ObservedVectorIndex) error {
	adopted, err := facetValues(result.Desired, subject).With(current.Desired())
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

func vectorValues(request schemaext.FacetComparisonRequest, subject objectidentity.ID) (*ydbschema.DesiredVectorIndex, *ydbschema.ObservedVectorIndex, error) {
	desired, _, err := schemaext.FacetAs[*ydbschema.DesiredVectorIndex](facetValues(request.Desired, subject), ydbschema.VectorIndexKind)
	if err != nil {
		return nil, nil, err
	}
	current, _, err := schemaext.FacetAs[*ydbschema.ObservedVectorIndex](facetValues(request.Current, subject), ydbschema.VectorIndexKind)
	if err != nil {
		return nil, nil, err
	}
	if desired != nil {
		if err := ydbschema.ValidateDesiredVectorIndex(desired); err != nil {
			return nil, nil, err
		}
	}
	if current != nil {
		if err := ydbschema.ValidateObservedVectorIndex(current); err != nil {
			return nil, nil, err
		}
	}
	return desired, current, nil
}

// facetValues returns the facets state holds for subject, or none.
func facetValues(state schemaext.FacetState, subject objectidentity.ID) schemaext.Facets {
	for _, record := range state.Records {
		if record.Subject.Key() == subject.Key() {
			return record.Values
		}
	}
	return schemaext.Facets{}
}

func vectorLimited(coverage schemaext.Coverage, subject objectidentity.ID) bool {
	knowledge, found := coverage.SubjectKnowledge(ydbschema.VectorIndexKind, subject)
	return found && (knowledge.State == schemaext.Uninspected || knowledge.State == schemaext.Unrepresentable)
}

func vectorUndecided(result *schemaext.FacetComparisonResult, subject objectidentity.ID, reason string) {
	result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: ydbschema.VectorIndexKind, Subject: subject, Reason: reason})
}
