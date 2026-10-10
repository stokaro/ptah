package ydbcompare

import (
	"context"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbpartition"
)

// TablePartitioningService compares the settings of YDB row tables -- how
// each splits into partitions, its read replicas and its key bloom filter --
// without database access. Its zero value is ready for concurrent use.
// Creation and removal of a table carry its settings with the table; changes
// describe only tables both sides hold.
type TablePartitioningService struct{}

// CompareFacets returns a complete comparison and preserves desired
// declarations.
//
// A setting a declaration leaves out keeps what the table holds, because a
// new table takes it from the cluster's table profile (see package
// ydbpartition). So the declaration is read over what the table holds
// ([ydbpartition.ResolveTable]), a table whose declaration states nothing
// keeps every setting, and a change carries the declaration and what the
// table holds. A starting layout counts only through the minimum partition
// count it gives a new table, which is the one record of it YDB keeps. A
// side that does not resolve differs, so the plan reaches the planner, which
// refuses it with the reason.
//
// A declaration the source could not describe is undecided, on a table the
// plan creates too. A table whose settings the read could not describe, or
// did not inspect, is undecided when the declaration states settings.
// Invalid input and cancellation return a zero result.
func (TablePartitioningService) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	return compareTableFacet(ctx, request, ydbschema.TablePartitioningKind, "YDB table partitioning", comparePartitioning)
}

func comparePartitioning(c *tableFacet, owner schemaext.ParentState) error {
	desired, current, err := partitioningValues(c, owner.Subject)
	if err != nil {
		return err
	}
	if c.sourceUndescribed(owner) {
		c.undecided(owner.Subject, "the desired source could not describe the table's settings")
		return nil
	}
	if !owner.Desired || !owner.Current || desired == nil || desired.IsZero() {
		return nil
	}
	if reason := unestablished(c, owner.Subject, current, "settings"); reason != "" {
		c.undecided(owner.Subject, reason)
		return nil
	}
	var before *ydbschema.TablePartitioning
	if current != nil {
		before = &current.TablePartitioning
	}
	held, heldErr := ydbpartition.HeldTable(before)
	resolved, desiredErr := ydbpartition.ResolveTable(&desired.TablePartitioning, held)
	if heldErr == nil && desiredErr == nil && resolved.Equal(held) {
		return nil
	}
	c.change(owner.Subject, &ydbdiff.TablePartitioning{Before: current, After: desired})
	return nil
}

func partitioningValues(c *tableFacet, subject objectidentity.ID) (*ydbschema.DesiredTablePartitioning, *ydbschema.ObservedTablePartitioning, error) {
	desiredFacets, currentFacets := c.facets(subject)
	desired, hasDesired, err := schemaext.FacetAs[*ydbschema.DesiredTablePartitioning](desiredFacets, ydbschema.TablePartitioningKind)
	if err != nil {
		return nil, nil, err
	}
	current, hasCurrent, err := schemaext.FacetAs[*ydbschema.ObservedTablePartitioning](currentFacets, ydbschema.TablePartitioningKind)
	if err != nil {
		return nil, nil, err
	}
	if !hasDesired {
		desired = nil
	} else if err := ydbschema.ValidateDesiredTablePartitioning(desired); err != nil {
		return nil, nil, err
	}
	if !hasCurrent {
		current = nil
	} else if err := ydbschema.ValidateObservedTablePartitioning(current); err != nil {
		return nil, nil, err
	}
	return desired, current, nil
}
