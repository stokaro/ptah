// Package tscompare compares TimescaleDB state using captured source knowledge:
// hypertable settings attached to tables, and continuous aggregates as named
// objects. It performs no database access; a server's spelling of a declared
// aggregate body arrives already attached to the declaration.
package tscompare

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsdiff"
	"ptah.run/dialect/timescaledb/tsschema"
)

// HypertableService compares the hypertable settings of surviving tables. Its
// zero value is ready for concurrent use.
//
// The identity is the table: a hypertable has no name of its own, so a table
// is partitioned or it is not, and there is nothing to rename. Creation and
// removal of the table carry its settings with the common table lifecycle.
// A source that cannot describe hypertables keeps the partitioning the server
// holds, because silence about a table's partitioning is not a request to
// remove it: the table itself is in the document, and its silence looks like a
// complete description of an ordinary table (stokaro/ptah#1026).
type HypertableService struct{}

// CompareFacets returns a complete comparison that retains every declaration.
// Unavailable current state for a declared hypertable is undecided, never an
// empty successful diff. Inputs remain unchanged.
func (HypertableService) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	if ctx == nil {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	if !platform.IsPostgresFamily(request.Target) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: TimescaleDB comparison on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{tsschema.HypertableKind}) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: unsupported TimescaleDB facet comparison kinds", schemaext.ErrInvalidValue)
	}
	result := schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired}
	result.Desired.Records = slices.Clone(result.Desired.Records)
	seen := make(map[objectidentity.Key]bool)
	for _, owner := range request.Owners {
		if err := ctx.Err(); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		if owner.Subject.Kind != objectidentity.KindTable || owner.Subject.Name.Empty() || seen[owner.Subject.Key()] || (!owner.Desired && !owner.Current) {
			return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: invalid or duplicate hypertable owner", schemaext.ErrInvalidValue)
		}
		seen[owner.Subject.Key()] = true
		if !request.Includes(tsschema.HypertableKind, owner.Subject) {
			continue
		}
		if err := compareHypertable(request, owner, &result); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	return result, nil
}

func compareHypertable(request schemaext.FacetComparisonRequest, owner schemaext.ParentState, result *schemaext.FacetComparisonResult) error {
	desired, current, err := hypertableValues(request, owner.Subject)
	if err != nil {
		return err
	}
	if !owner.Desired || !owner.Current {
		return nil
	}
	if limited(request.Desired.Coverage, owner.Subject) {
		undecided(result, owner.Subject, "the desired source could not describe this table's hypertable settings")
		return nil
	}
	desiredKnowledge := request.Desired.Coverage.Lookup(tsschema.HypertableKind, owner.Subject)
	if limited(request.Current.Coverage, owner.Subject) {
		// A limit is recorded for a table the catalog reports as a hypertable
		// it could not describe, so a declaration either way is a decision
		// about settings nobody read. A source that cannot describe them has
		// made no such decision.
		if desired != nil || current != nil || !unknown(desiredKnowledge) {
			undecided(result, owner.Subject, "this table's hypertable settings were not fully inspected")
		}
		return nil
	}
	if desired == nil && unknown(desiredKnowledge) {
		return adopt(result, owner.Subject, current)
	}
	currentKnowledge := request.Current.Coverage.Lookup(tsschema.HypertableKind, owner.Subject)
	if current == nil && unknown(currentKnowledge) {
		if desired != nil {
			undecided(result, owner.Subject, "whether this table is a hypertable was not inspected: "+currentKnowledge.Reason)
		}
		return nil
	}
	switch {
	case desired == nil && current == nil:
		return nil
	case desired != nil && current != nil && tsschema.SamePartitioning(desired, current):
		return nil
	}
	result.Changes = append(result.Changes, schemaext.FacetChange{Kind: tsschema.HypertableKind,
		Change: schemaext.ChangeRecord{Subject: owner.Subject, Value: &tsdiff.Hypertable{Before: current, After: desired}}})
	return nil
}

// adopt keeps the partitioning the server holds in the effective declaration of
// a source that cannot describe it. An observation that cannot become a
// declaration stays unmanaged; no change is planned for it either way.
func adopt(result *schemaext.FacetComparisonResult, subject objectidentity.ID, current *tsschema.ObservedHypertable) error {
	if current == nil {
		return nil
	}
	declared, err := current.Desired()
	if err != nil {
		return err
	}
	for i, record := range result.Desired.Records {
		if record.Subject.Key() != subject.Key() {
			continue
		}
		values, err := record.Values.With(declared)
		if err != nil {
			return err
		}
		result.Desired.Records[i].Values = values
		return nil
	}
	facets, err := schemaext.NewFacets(declared)
	if err != nil {
		return err
	}
	result.Desired.Records = append(result.Desired.Records, schemaext.FacetRecord{Subject: subject, Values: facets})
	return nil
}

func hypertableValues(request schemaext.FacetComparisonRequest, subject objectidentity.ID) (*tsschema.DesiredHypertable, *tsschema.ObservedHypertable, error) {
	desired, _, err := schemaext.FacetAs[*tsschema.DesiredHypertable](facetValues(request.Desired, subject), tsschema.HypertableKind)
	if err != nil {
		return nil, nil, err
	}
	current, _, err := schemaext.FacetAs[*tsschema.ObservedHypertable](facetValues(request.Current, subject), tsschema.HypertableKind)
	if err != nil {
		return nil, nil, err
	}
	if desired != nil {
		if err := tsschema.ValidateDesiredHypertable(desired); err != nil {
			return nil, nil, err
		}
	}
	if current != nil {
		if err := tsschema.ValidateObservedHypertable(current); err != nil {
			return nil, nil, err
		}
	}
	return desired, current, nil
}

func facetValues(state schemaext.FacetState, subject objectidentity.ID) schemaext.Facets {
	for _, record := range state.Records {
		if record.Subject.Key() == subject.Key() {
			return record.Values
		}
	}
	return schemaext.Facets{}
}

func limited(coverage schemaext.Coverage, subject objectidentity.ID) bool {
	knowledge, found := coverage.SubjectKnowledge(tsschema.HypertableKind, subject)
	return found && unknown(knowledge)
}

func unknown(knowledge schemaext.Knowledge) bool {
	return knowledge.State == schemaext.Uninspected || knowledge.State == schemaext.Unrepresentable
}

func undecided(result *schemaext.FacetComparisonResult, subject objectidentity.ID, reason string) {
	result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: tsschema.HypertableKind, Subject: subject, Reason: reason})
}
