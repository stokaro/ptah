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
	if ctx == nil {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	if request.Target != platform.YDB {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: YDB column family comparison on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{ydbschema.ColumnFamiliesKind}) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: unsupported YDB column family comparison kinds", schemaext.ErrInvalidValue)
	}
	c := familyComparison{
		request:  request,
		result:   schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired},
		desired:  familyRecords(request.Desired),
		current:  familyRecords(request.Current),
		declared: familyRecords(request.Desired),
	}
	c.result.Desired.Records = slices.Clone(c.result.Desired.Records)
	seen := make(map[objectidentity.Key]bool)
	for _, owner := range request.Owners {
		if owner.Subject.Kind != objectidentity.KindTable || owner.Subject.Name.Empty() || seen[owner.Subject.Key()] || (!owner.Desired && !owner.Current) {
			return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: invalid or duplicate YDB column family owner", schemaext.ErrInvalidValue)
		}
		seen[owner.Subject.Key()] = true
		if !request.Includes(ydbschema.ColumnFamiliesKind, owner.Subject) {
			continue
		}
		if err := c.table(owner); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	return c.result, nil
}

// familyComparison holds one request's records indexed by subject. declared
// indexes the result's desired records, which effective extends.
type familyComparison struct {
	request                    schemaext.FacetComparisonRequest
	result                     schemaext.FacetComparisonResult
	desired, current, declared map[objectidentity.Key]int
}

func familyRecords(state schemaext.FacetState) map[objectidentity.Key]int {
	index := make(map[objectidentity.Key]int, len(state.Records))
	for i, record := range state.Records {
		index[record.Subject.Key()] = i
	}
	return index
}

func (c *familyComparison) table(owner schemaext.ParentState) error {
	desired, current, err := c.values(owner.Subject)
	if err != nil {
		return err
	}
	// A table the plan creates carries its declaration with it, so a source
	// that could not describe its families is undecided there too.
	if knowledge, found := c.request.Desired.Coverage.SubjectKnowledge(ydbschema.ColumnFamiliesKind, owner.Subject); owner.Desired && found && knowledge.State == schemaext.Unrepresentable {
		c.undecided(owner.Subject, "the desired source could not describe the table's column families")
		return nil
	}
	if !owner.Desired || !owner.Current {
		return nil
	}
	if desired == nil && !familiesKnown(c.request.Desired.Coverage, owner.Subject) {
		// The source is silent about where each column sits, so the table
		// keeps the families it holds, columns included.
		if current != nil {
			return c.effective(owner.Subject, current.Desired())
		}
		return nil
	}
	if reason := c.unestablished(owner.Subject, current); reason != "" {
		if desired != nil && len(ydbfamily.Normalize(desired.Families)) > 0 {
			c.undecided(owner.Subject, reason)
		}
		return nil
	}
	c.compare(owner.Subject, desired, current)
	return nil
}

// unestablished says why the read did not establish the families of the table
// subject, or returns "": the read found families it could not describe, or
// did not inspect them.
func (c *familyComparison) unestablished(subject objectidentity.ID, current *ydbschema.ObservedColumnFamilies) string {
	const notInspected = "the table's column families were not inspected"
	if knowledge, found := c.request.Current.Coverage.SubjectKnowledge(ydbschema.ColumnFamiliesKind, subject); found {
		switch knowledge.State {
		case schemaext.Unrepresentable:
			return "the read found column families it could not describe"
		case schemaext.Uninspected:
			return notInspected
		default:
		}
	}
	if current == nil && !familiesKnown(c.request.Current.Coverage, subject) {
		return notInspected
	}
	return ""
}

// compare reports a change for a table both sides hold whose families differ
// from what the declaration asks for.
func (c *familyComparison) compare(subject objectidentity.ID, desired *ydbschema.DesiredColumnFamilies, current *ydbschema.ObservedColumnFamilies) {
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
	change := &ydbdiff.ColumnFamilies{Before: current, After: effective}
	c.result.Changes = append(c.result.Changes, schemaext.FacetChange{
		Kind: ydbschema.ColumnFamiliesKind, Change: schemaext.ChangeRecord{Subject: subject, Value: change.CloneChange()},
	})
}

func (c *familyComparison) values(subject objectidentity.ID) (*ydbschema.DesiredColumnFamilies, *ydbschema.ObservedColumnFamilies, error) {
	var desiredFacets, currentFacets schemaext.Facets
	if i, found := c.desired[subject.Key()]; found {
		desiredFacets = c.request.Desired.Records[i].Values
	}
	if i, found := c.current[subject.Key()]; found {
		currentFacets = c.request.Current.Records[i].Values
	}
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

// effective adopts value into the effective declaration of subject, which
// declares no families, so a planner that rebuilds the table keeps them.
func (c *familyComparison) effective(subject objectidentity.ID, value *ydbschema.DesiredColumnFamilies) error {
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

func (c *familyComparison) undecided(subject objectidentity.ID, reason string) {
	c.result.Undecided = append(c.result.Undecided, schemaext.UndecidedChange{Kind: ydbschema.ColumnFamiliesKind, Subject: subject, Reason: reason})
}

// familiesKnown reports knowledge that makes a missing value an absence.
func familiesKnown(coverage schemaext.Coverage, subject objectidentity.ID) bool {
	switch coverage.Lookup(ydbschema.ColumnFamiliesKind, subject).State {
	case schemaext.Complete, schemaext.Absent:
		return true
	default:
		return false
	}
}
