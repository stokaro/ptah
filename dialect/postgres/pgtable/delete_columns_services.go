package pgtable

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// DeleteColumnsService converts, compares and reverses ON DELETE column
// selections in batches. Its zero value is concurrent-safe and reads no database.
type DeleteColumnsService struct{}

// ConvertFeatures preserves the selection while changing its representation.
// Invalid directions, values or cancellation return no partial result.
func (DeleteColumnsService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	return convertTableFeatures(ctx, request, "ON DELETE column selection", convertDeleteColumns)
}
func convertDeleteColumns(from schemaext.Representation, value schemaext.Value) (schemaext.Value, error) {
	switch d := value.(type) {
	case *DesiredDeleteColumns:
		if from == schemaext.Desired {
			if err := ValidateDesiredDeleteColumns(d); err != nil {
				return nil, err
			}
			return &ObservedDeleteColumns{Columns: slices.Clone(d.Columns)}, nil
		}
	case *ObservedDeleteColumns:
		if from == schemaext.Observed {
			if err := ValidateObservedDeleteColumns(d); err != nil {
				return nil, err
			}
			return &DesiredDeleteColumns{Columns: slices.Clone(d.Columns)}, nil
		}
	}
	return nil, fmt.Errorf("%w: unexpected %s ON DELETE column selection %T", schemaext.ErrInvalidValue, from, value)
}

func deleteColumnsContext(ctx context.Context, target string) error {
	if err := persistenceContext(ctx, target); err != nil {
		return err
	}
	if target != platform.Postgres && target != platform.YugabyteDB {
		return fmt.Errorf("%w: ON DELETE column lists require PostgreSQL support, not %s", ptaherr.ErrUnsupportedFeature, target)
	}
	return nil
}

// CompareFacets compares selections as sets of logical column identities. An
// absent facet under complete coverage selects every local column of that
// side's captured foreign key. Order, repetition and an explicit complete list
// do not cause a migration. Missing knowledge remains undecided. A common
// constraint's creation, removal or kind change accounts for its selection.
// Errors and cancellation return no partial result or input mutations.
func (DeleteColumnsService) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	if err := deleteColumnsContext(ctx, request.Target); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{DeleteColumnsKind}) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: unsupported ON DELETE comparison kinds", schemaext.ErrInvalidValue)
	}
	result := schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired}
	for _, owner := range request.Owners {
		if err := ctx.Err(); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		change, reason, err := compareDeleteColumns(request, owner)
		if err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		if change != nil {
			result.Changes = append(result.Changes, schemaext.FacetChange{Kind: DeleteColumnsKind, Change: schemaext.ChangeRecord{Subject: owner.Subject, Value: change}})
		}
		if reason != "" {
			result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: DeleteColumnsKind, Subject: owner.Subject, Reason: reason})
		}
	}
	return completedPersistence(ctx, result)
}

func compareDeleteColumns(request schemaext.FacetComparisonRequest, owner schemaext.ParentState) (*DeleteColumnsChange, string, error) {
	if !HoldsDeleteColumns(owner.Subject) {
		return nil, "", fmt.Errorf("%w: ON DELETE column selection requires a named constraint, not %s", schemaext.ErrInvalidValue, owner.Subject)
	}
	desired, _, err := schemaext.FacetAs[*DesiredDeleteColumns](tableFacets(request.Desired, owner.Subject), DeleteColumnsKind)
	if err != nil {
		return nil, "", err
	}
	current, _, err := schemaext.FacetAs[*ObservedDeleteColumns](tableFacets(request.Current, owner.Subject), DeleteColumnsKind)
	if err != nil {
		return nil, "", err
	}
	declared, observed := deleteConstraintContext(request.DesiredConstraints, owner.Subject), deleteConstraintContext(request.CurrentConstraints, owner.Subject)
	if err := checkDeleteSelection(request.Identifiers, desired, declared); err != nil {
		return nil, "", err
	}
	if err := checkDeleteSelection(request.Identifiers, current, observed); err != nil {
		return nil, "", err
	}
	if !owner.Desired || !owner.Current || !request.Includes(DeleteColumnsKind, owner.Subject) {
		return nil, "", nil
	}
	if declared != nil && declared.Type != "FOREIGN KEY" || observed != nil && observed.Type != "FOREIGN KEY" {
		return nil, "", nil
	}
	return compareKnownDeleteColumns(request, owner.Subject, desired, current, declared, observed)
}

func compareKnownDeleteColumns(request schemaext.FacetComparisonRequest, subject objectidentity.ID,
	desired *DesiredDeleteColumns, current *ObservedDeleteColumns, declared, observed *schemaext.ConstraintContext,
) (*DeleteColumnsChange, string, error) {
	if reason := deleteColumnsKnowledgeLimit(request, subject); reason != "" {
		return nil, reason, nil
	}
	if desired == nil && !describesDeleteColumns(request.Desired.Coverage, subject) {
		return nil, "", nil
	}
	if current == nil && !describesDeleteColumns(request.Current.Coverage, subject) {
		return nil, "the current source does not describe ON DELETE column selection", nil
	}
	var before, after []string
	if desired != nil {
		after = desired.Columns
	}
	if current != nil {
		before = current.Columns
	}
	if maps.Equal(deleteColumnSet(request.Identifiers, nil, before), deleteColumnSet(request.Identifiers, nil, after)) {
		return nil, "", nil
	}
	if declared == nil || observed == nil {
		return nil, "ON DELETE comparison requires both captured foreign-key definitions", nil
	}
	if maps.Equal(deleteColumnSet(request.Identifiers, observed.Columns, before), deleteColumnSet(request.Identifiers, declared.Columns, after)) {
		return nil, "", nil
	}
	return (&DeleteColumnsChange{Before: current, After: desired}).Copy(), "", nil
}

func deleteConstraintContext(contexts []schemaext.ConstraintContext, subject objectidentity.ID) *schemaext.ConstraintContext {
	for i := range contexts {
		if contexts[i].Subject.Key() == subject.Key() {
			return &contexts[i]
		}
	}
	return nil
}

func checkDeleteSelection(semantics identifier.Semantics, value schemaext.Value, definition *schemaext.ConstraintContext) error {
	var columns []string
	switch d := value.(type) {
	case *DesiredDeleteColumns:
		if d == nil {
			return nil
		}
		if err := ValidateDesiredDeleteColumns(d); err != nil {
			return err
		}
		columns = d.Columns
	case *ObservedDeleteColumns:
		if d == nil {
			return nil
		}
		if err := ValidateObservedDeleteColumns(d); err != nil {
			return err
		}
		columns = d.Columns
	}
	if definition == nil {
		return nil
	}
	if definition.Type != "FOREIGN KEY" {
		return fmt.Errorf("%w: ON DELETE column selection attaches to FOREIGN KEY, not %s", schemaext.ErrInvalidValue, definition.Type)
	}
	for _, column := range columns {
		if _, found := deleteColumnSet(semantics, nil, definition.Columns)[semantics.ColumnIdentityKey(column)]; !found {
			return fmt.Errorf("%w: ON DELETE names column %q outside key %s", schemaext.ErrInvalidValue, column, definition.Subject)
		}
	}
	return nil
}

func deleteColumnSet(semantics identifier.Semantics, keyColumns, listed []string) map[string]struct{} {
	if len(listed) == 0 {
		listed = keyColumns
	}
	result := make(map[string]struct{}, len(listed))
	for _, column := range listed {
		result[semantics.ColumnIdentityKey(column)] = struct{}{}
	}
	return result
}
func describesDeleteColumns(coverage schemaext.Coverage, subject objectidentity.ID) bool {
	switch coverage.Lookup(DeleteColumnsKind, subject).State {
	case schemaext.Complete, schemaext.Absent, schemaext.Defaulted:
		return true
	default:
		return false
	}
}
func deleteColumnsKnowledgeLimit(request schemaext.FacetComparisonRequest, subject objectidentity.ID) string {
	for _, coverage := range []schemaext.Coverage{request.Desired.Coverage, request.Current.Coverage} {
		knowledge := coverage.Lookup(DeleteColumnsKind, subject)
		if !coverage.SelectKinds([]schemaext.Kind{DeleteColumnsKind}).IsZero() && (knowledge.State == schemaext.Uninspected || knowledge.State == schemaext.Unrepresentable) {
			if knowledge.Reason != "" {
				return knowledge.Reason
			}
			return "ON DELETE column selection is not fully described"
		}
	}
	return ""
}

// ReverseChanges reverses each selection and projects the state left by the
// forward change. Invalid input or cancellation returns no partial batch.
func (DeleteColumnsService) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	if err := deleteColumnsContext(ctx, request.Target); err != nil {
		return nil, err
	}
	result := make([]schemaext.Reversal, 0, len(request.Changes))
	for _, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		change, ok := record.Value.(*DeleteColumnsChange)
		if !ok || !HoldsDeleteColumns(record.Subject) {
			return nil, fmt.Errorf("%w: reversal requires an ON DELETE column-selection change", schemaext.ErrInvalidValue)
		}
		inverse, err := change.Reverse()
		if err != nil {
			return nil, err
		}
		reversal := schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: record.Subject, Value: inverse}, Strategy: "restore the observed ON DELETE column selection",
			ForwardState: []schemaext.ProjectedValue{{Placement: schemaext.FacetPlacement, Kind: DeleteColumnsKind}}}
		if inverse.Before != nil {
			reversal.ForwardState[0].Value = inverse.Before.Clone()
		}
		result = append(result, reversal)
	}
	return completedPersistence(ctx, result)
}
