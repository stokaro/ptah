package engine

import (
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// comparisonCoverageInput shares knowledge-preservation rules without giving
// object and facet subjects the same identity or ownership semantics.
type comparisonCoverageInput struct {
	kinds         []schemaext.Kind
	desired       schemaext.Coverage
	current       schemaext.Coverage
	related       func(schemaext.SubjectCoverage) bool
	observedValue func(schemaext.Kind, objectidentity.ID) bool
}

// comparisonCoverageRequirements is shared by both comparison surfaces because
// source knowledge must not acquire a different meaning for an attached value.
// An uninspected namespace alone makes no claim about target applicability;
// encountered state and explicit subject limitations cannot be treated as empty.
func comparisonCoverageRequirements(coverage schemaext.Coverage, kinds, required map[schemaext.Kind]bool) {
	for _, record := range coverage.KindRecords() {
		kinds[record.Model.Kind] = true
		if record.Knowledge.State == schemaext.Unrepresentable {
			required[record.Model.Kind] = true
		}
	}
	for _, record := range coverage.SubjectRecords() {
		if record.Knowledge.State == schemaext.Defaulted || record.Knowledge.State == schemaext.Uninspected || record.Knowledge.State == schemaext.Unrepresentable {
			required[record.Kind] = true
		}
	}
}

func validateComparisonCoverage(request comparisonCoverageInput, result schemaext.Coverage) error {
	records := make(map[schemaext.Kind]schemaext.KindCoverage)
	for _, record := range result.KindRecords() {
		if !slices.Contains(request.kinds, record.Model.Kind) {
			return fmt.Errorf("%w: comparison returned unrelated coverage %q", schemaext.ErrInvalidValue, record.Model.Kind)
		}
		records[record.Model.Kind] = record
	}
	for _, original := range request.desired.KindRecords() {
		returned, found := records[original.Model.Kind]
		if !found || original != returned {
			return fmt.Errorf("%w: comparison changed source-wide coverage for %q", schemaext.ErrInvalidValue, original.Model.Kind)
		}
		delete(records, original.Model.Kind)
	}
	for kind, record := range records {
		if record.Knowledge.State != schemaext.Uninspected {
			return fmt.Errorf("%w: comparison invented source-wide authority for %q", schemaext.ErrInvalidValue, kind)
		}
	}
	for _, record := range result.SubjectRecords() {
		if !request.related(record) {
			return fmt.Errorf("%w: comparison returned unrelated coverage for %s", schemaext.ErrInvalidValue, record.Subject)
		}
		if err := comparisonKnowledge(request, record); err != nil {
			return err
		}
	}
	for _, original := range request.desired.SubjectRecords() {
		returned := result.Lookup(original.Kind, original.Subject)
		if returned == original.Knowledge {
			continue
		}
		switch original.Knowledge.State {
		case schemaext.Complete, schemaext.Absent, schemaext.Defaulted:
			return fmt.Errorf("%w: comparison discarded explicit source knowledge for %s", schemaext.ErrInvalidValue, original.Subject)
		default:
			if err := comparisonKnowledge(request, schemaext.SubjectCoverage{Kind: original.Kind, Subject: original.Subject, Knowledge: returned}); err != nil {
				return err
			}
		}
	}
	return nil
}

func comparisonKnowledge(request comparisonCoverageInput, record schemaext.SubjectCoverage) error {
	original := request.desired.Lookup(record.Kind, record.Subject)
	if record.Knowledge == original {
		return nil
	}
	if original.State == schemaext.Complete || original.State == schemaext.Absent || original.State == schemaext.Defaulted {
		return fmt.Errorf("%w: comparison changed explicit desired knowledge for %s", schemaext.ErrInvalidValue, record.Subject)
	}
	if record.Knowledge.State == schemaext.Uninspected || record.Knowledge.State == schemaext.Unrepresentable {
		return nil
	}
	current := request.current.Lookup(record.Kind, record.Subject)
	if (record.Knowledge.State == schemaext.Complete || record.Knowledge.State == schemaext.Absent) && current.State == record.Knowledge.State {
		return nil
	}
	// A returned object is positive evidence even if its siblings could not all
	// be enumerated. An explicit limitation on that object still takes priority.
	known, limited := request.current.SubjectKnowledge(record.Kind, record.Subject)
	if record.Knowledge.State == schemaext.Complete && (!limited || known.State == schemaext.Complete) &&
		request.observedValue(record.Kind, record.Subject) {
		return nil
	}
	return fmt.Errorf("%w: comparison invented knowledge for %s", schemaext.ErrInvalidValue, record.Subject)
}
