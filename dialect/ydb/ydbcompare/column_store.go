package ydbcompare

import (
	"context"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
)

// ColumnStoreService compares the column storage of YDB tables without
// database access. Its zero value is ready for concurrent use. Creation and
// removal of a table carry its storage with the table; changes describe only
// tables both sides hold.
//
// A declaration that leaves the hash key or the shard count out takes what the
// table holds, and a tiered TTL is compared by the seconds its intervals
// denote. A table both sides hold whose storage differs is a change, which
// the planner makes in place only for the TTL: the storage kind, the hash key
// and the shard count are fixed when YDB creates the table.
//
// A source that cannot describe column storage, an HCL or a DBML document,
// keeps the storage the table holds: it is adopted into the effective
// declaration, and nothing is planned. A declaration the source could not
// describe is undecided, on a table the plan creates too. A column table
// declared where the read did not establish the storage is undecided as well.
// Invalid input and cancellation return a zero result.
type ColumnStoreService struct{}

// CompareFacets returns a complete comparison and preserves desired
// declarations.
func (ColumnStoreService) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	return compareTableFacet(ctx, request, ydbschema.ColumnStoreKind, "YDB column storage", compareStore)
}

func compareStore(c *tableFacet, owner schemaext.ParentState) error {
	desired, current, err := storeValues(c, owner.Subject)
	if err != nil {
		return err
	}
	if c.sourceUndescribed(owner) {
		c.undecided(owner.Subject, "the desired source could not describe the table's storage")
		return nil
	}
	if !owner.Desired || !owner.Current {
		return nil
	}
	if desired == nil && !c.known(c.request.Desired.Coverage, owner.Subject) {
		// The source is silent about the table's storage, so the table keeps
		// the storage it holds.
		if current != nil {
			return c.adopt(owner.Subject, current.Desired())
		}
		return nil
	}
	if reason := unestablished(c, owner.Subject, current, "storage settings"); reason != "" {
		if desired != nil {
			c.undecided(owner.Subject, reason)
		}
		return nil
	}
	if !satisfied(desired, current) {
		c.change(owner.Subject, &ydbdiff.ColumnStore{Before: current, After: desired})
	}
	return nil
}

// satisfied reports whether a table holding current has the storage desired
// asks for: row storage on both sides, or a column table whose layout is
// what desired states and whose tiered TTL is the one desired asks for.
func satisfied(desired *ydbschema.DesiredColumnStore, current *ydbschema.ObservedColumnStore) bool {
	if desired == nil || current == nil {
		return desired == nil && current == nil
	}
	return ydbschema.LayoutSatisfied(desired.ColumnStore, current.ColumnStore) && ydbschema.TieredTTLEqual(desired.TTL, current.TTL)
}

func storeValues(c *tableFacet, subject objectidentity.ID) (*ydbschema.DesiredColumnStore, *ydbschema.ObservedColumnStore, error) {
	desiredFacets, currentFacets := c.facets(subject)
	desired, hasDesired, err := schemaext.FacetAs[*ydbschema.DesiredColumnStore](desiredFacets, ydbschema.ColumnStoreKind)
	if err != nil {
		return nil, nil, err
	}
	current, hasCurrent, err := schemaext.FacetAs[*ydbschema.ObservedColumnStore](currentFacets, ydbschema.ColumnStoreKind)
	if err != nil {
		return nil, nil, err
	}
	if !hasDesired {
		desired = nil
	} else if err := ydbschema.ValidateDesiredColumnStore(desired); err != nil {
		return nil, nil, err
	}
	if !hasCurrent {
		current = nil
	} else if err := ydbschema.ValidateObservedColumnStore(current); err != nil {
		return nil, nil, err
	}
	return desired, current, nil
}
