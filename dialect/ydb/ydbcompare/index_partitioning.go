package ydbcompare

import (
	"context"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbindex"
)

// IndexPartitioningService compares the partitioning of YDB global indexes
// without database access, reading the declaration over what the index
// holds as [TablePartitioningService] reads a table's. Its zero value is
// ready for concurrent use. Creation and removal of an index carry its
// settings with the index; changes describe only indexes both sides hold,
// a renamed index under its new name.
type IndexPartitioningService struct{}

// CompareFacets returns a complete comparison and preserves desired
// declarations; see [TablePartitioningService.CompareFacets]. Invalid input
// and cancellation return a zero result.
func (IndexPartitioningService) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	return compareFacet(ctx, request, objectidentity.KindIndex, ydbschema.IndexPartitioningKind, "YDB index partitioning", compareIndexPartitioning)
}

func compareIndexPartitioning(c *tableFacet, owner schemaext.ParentState) error {
	desiredFacets, currentFacets := c.facets(owner.Subject)
	desired, err := facetValue[*ydbschema.DesiredIndexPartitioning](desiredFacets, ydbschema.ValidateDesiredIndexPartitioning)
	if err != nil {
		return err
	}
	current, err := facetValue[*ydbschema.ObservedIndexPartitioning](currentFacets, ydbschema.ValidateObservedIndexPartitioning)
	if err != nil {
		return err
	}
	if c.sourceUndescribed(owner) {
		c.undecided(owner.Subject, "the desired source could not describe the index's settings")
		return nil
	}
	if !owner.Desired || !owner.Current || desired == nil || desired.IsZero() {
		return nil
	}
	if reason := unestablished(c, owner.Subject, current, "settings"); reason != "" {
		c.undecided(owner.Subject, reason)
		return nil
	}
	var before *ydbschema.IndexPartitioning
	if current != nil {
		before = &current.IndexPartitioning
	}
	held, heldErr := ydbindex.Held(before)
	resolved, desiredErr := ydbindex.Resolve(&desired.IndexPartitioning, held)
	if heldErr == nil && desiredErr == nil && resolved.Equal(held) {
		return nil
	}
	c.change(owner.Subject, &ydbdiff.IndexPartitioning{Before: current, After: desired})
	return nil
}

// facetValue is the value of type V the facets hold, validated, or nil where
// they hold none.
func facetValue[V interface {
	schemaext.Value
	comparable
}](facets schemaext.Facets, validate func(V) error) (V, error) {
	var zero V
	if len(facets.Kinds()) == 0 {
		return zero, nil
	}
	value, found, err := schemaext.FacetAs[V](facets, zero.Kind())
	if err != nil || !found {
		return zero, err
	}
	if err := validate(value); err != nil {
		return zero, err
	}
	return value, nil
}
