package ydbcompare

import (
	"context"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbfamily"
)

// ColumnFamiliesService compares the column families of YDB row tables
// without database access. Its zero value is ready for concurrent use.
// Creation and removal of a table carry its families with the table; changes
// describe only tables both sides hold.
type ColumnFamiliesService struct{}

// CompareFacets returns a complete comparison and preserves desired
// declarations.
//
// A setting a declaration leaves out means "keep what the table holds",
// because a table's defaults come from the cluster's table profile (see
// package ydbfamily). So for every table both sides hold, a change carries as
// its After what the table holds once the declaration is applied
// ([ydbfamily.Applied]): every family the table holds stays, each setting the
// declaration does not state keeps the held value, and the columns sit where
// the declaration places them. A rebuild writes the same; see
// [ptah.run/dialect/ydb/ydbplan.RebuiltFamilies]. Under complete source
// coverage a table whose declaration states no family asks for every column
// in the default family.
//
// A source that cannot describe families, an HCL or a DBML document, is
// silent about where each column sits too, so the table keeps the families it
// holds, columns included: they are adopted into the effective declaration,
// and nothing is planned. A declaration the source
// could not describe is undecided, on a table the plan creates too. A table
// whose families the read could not describe, or did not inspect, is
// undecided when the declaration states families, since a statement built
// against families the read did not see could undo what it did not read.
// Invalid input and cancellation return a zero result.
func (ColumnFamiliesService) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	return compareTableFacet(ctx, request, ydbschema.ColumnFamiliesKind, "YDB column family", compareFamilies)
}

func compareFamilies(c *tableFacet, owner schemaext.ParentState) error {
	desired, current, err := familyValues(c, owner.Subject)
	if err != nil {
		return err
	}
	// A table the plan creates carries its declaration with it, so a source
	// that could not describe its families is undecided there too.
	if c.sourceUndescribed(owner) {
		c.undecided(owner.Subject, "the desired source could not describe the table's column families")
		return nil
	}
	if !owner.Desired || !owner.Current {
		return nil
	}
	if desired == nil && !c.known(c.request.Desired.Coverage, owner.Subject) {
		// The source is silent about where each column sits, so the table
		// keeps the families it holds, columns included.
		if current != nil {
			return c.adopt(owner.Subject, current.Desired())
		}
		return nil
	}
	if reason := unestablished(c, owner.Subject, current, "column families"); reason != "" {
		if desired != nil && len(ydbfamily.Normalize(desired.Families)) > 0 {
			c.undecided(owner.Subject, reason)
		}
		return nil
	}
	compareFamilyValues(c, owner.Subject, desired, current)
	return nil
}

// compareFamilyValues reports a change for a table both sides hold whose
// families differ from what the declaration asks for.
func compareFamilyValues(c *tableFacet, subject objectidentity.ID, desired *ydbschema.DesiredColumnFamilies, current *ydbschema.ObservedColumnFamilies) {
	var declared, held []ydbschema.ColumnFamily
	if desired != nil {
		declared = desired.Families
	}
	if current != nil {
		held = current.Families
	}
	effective := &ydbschema.DesiredColumnFamilies{Families: ydbfamily.Applied(declared, held)}
	if ydbfamily.Satisfied(effective.Families, held) {
		return
	}
	c.change(subject, &ydbdiff.ColumnFamilies{Before: current, After: effective})
}

func familyValues(c *tableFacet, subject objectidentity.ID) (*ydbschema.DesiredColumnFamilies, *ydbschema.ObservedColumnFamilies, error) {
	desiredFacets, currentFacets := c.facets(subject)
	desired, hasDesired, err := schemaext.FacetAs[*ydbschema.DesiredColumnFamilies](desiredFacets, ydbschema.ColumnFamiliesKind)
	if err != nil {
		return nil, nil, err
	}
	current, hasCurrent, err := schemaext.FacetAs[*ydbschema.ObservedColumnFamilies](currentFacets, ydbschema.ColumnFamiliesKind)
	if err != nil {
		return nil, nil, err
	}
	if !hasDesired {
		desired = nil
	} else if err := ydbschema.ValidateDesiredColumnFamilies(desired); err != nil {
		return nil, nil, err
	}
	if !hasCurrent {
		current = nil
	} else if err := ydbschema.ValidateObservedColumnFamilies(current); err != nil {
		return nil, nil, err
	}
	return desired, current, nil
}
