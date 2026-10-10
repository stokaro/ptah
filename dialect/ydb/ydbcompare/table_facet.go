package ydbcompare

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// tableFacet is one comparison of a YDB table or index facet: the request it answers,
// the result it fills, and each side's records indexed by subject. declared
// indexes the result's desired records, which an adoption extends.
type tableFacet struct {
	kind                       schemaext.Kind
	request                    schemaext.FacetComparisonRequest
	result                     schemaext.FacetComparisonResult
	desired, current, declared map[objectidentity.Key]int
}

// compareTableFacet checks a comparison request for one YDB table facet kind,
// named name in an error, and runs table for every table owner the request
// includes. The result is complete and preserves desired declarations; an
// error or cancellation returns a zero result.
func compareTableFacet(ctx context.Context, request schemaext.FacetComparisonRequest, kind schemaext.Kind, name string,
	table func(*tableFacet, schemaext.ParentState) error,
) (schemaext.FacetComparisonResult, error) {
	return compareFacet(ctx, request, objectidentity.KindTable, kind, name, table)
}

// compareFacet is [compareTableFacet] for owners of ownerKind; an index owner
// names its table as its parent.
func compareFacet(ctx context.Context, request schemaext.FacetComparisonRequest, ownerKind objectidentity.Kind, kind schemaext.Kind, name string,
	compare func(*tableFacet, schemaext.ParentState) error,
) (schemaext.FacetComparisonResult, error) {
	if ctx == nil {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	if request.Target != platform.YDB {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: %s comparison on %q", ptaherr.ErrUnsupportedDialect, name, request.Target)
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{kind}) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: unsupported %s comparison kinds", schemaext.ErrInvalidValue, name)
	}
	c := &tableFacet{
		kind:     kind,
		request:  request,
		result:   schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired},
		desired:  facetRecords(request.Desired),
		current:  facetRecords(request.Current),
		declared: facetRecords(request.Desired),
	}
	c.result.Desired.Records = slices.Clone(c.result.Desired.Records)
	seen := make(map[objectidentity.Key]bool)
	for _, owner := range request.Owners {
		if owner.Subject.Kind != ownerKind || owner.Subject.Name.Empty() || seen[owner.Subject.Key()] || (!owner.Desired && !owner.Current) ||
			(ownerKind == objectidentity.KindIndex && owner.Subject.Parent.Empty()) {
			return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: invalid or duplicate %s owner", schemaext.ErrInvalidValue, name)
		}
		seen[owner.Subject.Key()] = true
		if !request.Includes(kind, owner.Subject) {
			continue
		}
		if err := compare(c, owner); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	return c.result, nil
}

func facetRecords(state schemaext.FacetState) map[objectidentity.Key]int {
	index := make(map[objectidentity.Key]int, len(state.Records))
	for i, record := range state.Records {
		index[record.Subject.Key()] = i
	}
	return index
}

// facets returns each side's facets of subject; a side without a record has
// none.
func (c *tableFacet) facets(subject objectidentity.ID) (desired, current schemaext.Facets) {
	if i, found := c.desired[subject.Key()]; found {
		desired = c.request.Desired.Records[i].Values
	}
	if i, found := c.current[subject.Key()]; found {
		current = c.request.Current.Records[i].Values
	}
	return desired, current
}

// sourceUndescribed reports whether the desired source could not describe the
// facet of a table it declares, which leaves even a table the plan creates
// undecided.
func (c *tableFacet) sourceUndescribed(owner schemaext.ParentState) bool {
	knowledge, found := c.request.Desired.Coverage.SubjectKnowledge(c.kind, owner.Subject)
	return owner.Desired && found && knowledge.State == schemaext.Unrepresentable
}

// known reports knowledge that makes a missing value an absence.
func (c *tableFacet) known(coverage schemaext.Coverage, subject objectidentity.ID) bool {
	switch coverage.Lookup(c.kind, subject).State {
	case schemaext.Complete, schemaext.Absent:
		return true
	default:
		return false
	}
}

// unestablished says why the read did not establish the facet of the table
// subject, whose read value is current, nil for none, or returns "": the read
// found a value it could not describe, or did not inspect the table. what
// names the facet in the reason.
func unestablished[O any](c *tableFacet, subject objectidentity.ID, current *O, what string) string {
	notInspected := fmt.Sprintf("the table's %s were not inspected", what)
	if knowledge, found := c.request.Current.Coverage.SubjectKnowledge(c.kind, subject); found {
		switch knowledge.State {
		case schemaext.Unrepresentable:
			return fmt.Sprintf("the read found %s it could not describe", what)
		case schemaext.Uninspected:
			return notInspected
		default:
		}
	}
	if current == nil && !c.known(c.request.Current.Coverage, subject) {
		return notInspected
	}
	return ""
}

// change reports a change of the table subject.
func (c *tableFacet) change(subject objectidentity.ID, value schemaext.ChangeValue) {
	c.result.Changes = append(c.result.Changes, schemaext.FacetChange{
		Kind: c.kind, Change: schemaext.ChangeRecord{Subject: subject, Value: value.CloneChange()},
	})
}

// adopt adds value to the effective declaration of subject, which declares
// none, so a planner that rebuilds the table keeps it.
func (c *tableFacet) adopt(subject objectidentity.ID, value schemaext.Value) error {
	if i, found := c.declared[subject.Key()]; found {
		facets, err := c.result.Desired.Records[i].Values.With(value)
		if err != nil {
			return err
		}
		c.result.Desired.Records[i].Values = facets
		return nil
	}
	facets, err := schemaext.NewFacets(value)
	if err != nil {
		return err
	}
	c.declared[subject.Key()] = len(c.result.Desired.Records)
	c.result.Desired.Records = append(c.result.Desired.Records, schemaext.FacetRecord{Subject: subject, Values: facets})
	return nil
}

func (c *tableFacet) undecided(subject objectidentity.ID, reason string) {
	c.result.Undecided = append(c.result.Undecided, schemaext.UndecidedChange{Kind: c.kind, Subject: subject, Reason: reason})
}
