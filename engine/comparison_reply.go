package engine

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

func (r *Runtime) validateComparisonReply(ctx context.Context, service int, request schemaext.ObjectComparisonRequest, result schemaext.ObjectComparisonResult) (schemaext.ObjectComparisonResult, error) {
	if !result.Complete {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: object comparison did not complete", schemaext.ErrInvalidValue)
	}
	var err error
	result.Desired, err = r.codecs.SnapshotObjectState(ctx, schemaext.Desired, result.Desired)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	if err := comparisonDesired(request, result.Desired); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	result.Changes, err = r.codecs.SnapshotChanges(ctx, result.Changes)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	seen := make(map[objectidentity.Key]bool)
	for _, change := range result.Changes {
		if !slices.Contains(r.comparisonServices[service].ChangeKinds, change.Value.Kind()) || !comparisonSubject(request, change.Subject) || seen[change.Subject.Key()] {
			return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: comparison returned an unrelated or duplicate change for %s", schemaext.ErrInvalidValue, change.Subject)
		}
		if parent, found := comparisonParent(request.Parents, change.Subject); found && (!parent.Desired || !parent.Current) {
			return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: parent transition already owns change for %s", schemaext.ErrInvalidValue, change.Subject)
		}
		seen[change.Subject.Key()] = true
	}
	result.Undecided = slices.Clone(result.Undecided)
	type diagnosticKey struct {
		kind    schemaext.Kind
		subject objectidentity.Key
	}
	diagnostics := make(map[diagnosticKey]bool)
	for _, diagnostic := range result.Undecided {
		subjectKind := diagnostic.Subject.Kind
		key := diagnosticKey{diagnostic.Kind, diagnostic.Subject.Key()}
		if !slices.Contains(request.Kinds, diagnostic.Kind) ||
			(subjectKind != objectidentity.KindTable && subjectKind != objectidentity.Kind(diagnostic.Kind)) ||
			strings.TrimSpace(diagnostic.Reason) == "" || !comparisonDiagnosticSubject(request, diagnostic.Subject) ||
			diagnostics[key] || seen[diagnostic.Subject.Key()] {
			return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: invalid comparison diagnostic for %s", schemaext.ErrInvalidValue, diagnostic.Subject)
		}
		diagnostics[key] = true
	}
	return result, nil
}

func comparisonDesired(request schemaext.ObjectComparisonRequest, result schemaext.ObjectState) error {
	for _, ref := range request.Desired.Objects.Refs() {
		original, _, err := request.Desired.Objects.Get(ref)
		if err != nil {
			return err
		}
		returned, found, err := result.Objects.Get(ref)
		if err != nil {
			return err
		}
		if !found || !original.Value.Equal(returned.Value) {
			return fmt.Errorf("%w: comparison discarded an explicit declaration for %s", schemaext.ErrInvalidValue, ref)
		}
	}
	for _, ref := range result.Objects.Refs() {
		if !slices.Contains(request.Kinds, schemaext.Kind(ref.Kind)) || !comparisonSubject(request, ref) {
			return fmt.Errorf("%w: comparison invented an unrelated desired object %s", schemaext.ErrInvalidValue, ref)
		}
		if parent, found := comparisonParent(request.Parents, ref); found && !parent.Desired {
			return fmt.Errorf("%w: comparison preserved an object below a removed parent %s", schemaext.ErrInvalidValue, ref)
		}
	}
	return comparisonCoverage(request, result.Coverage)
}

func comparisonCoverage(request schemaext.ObjectComparisonRequest, result schemaext.Coverage) error {
	return validateComparisonCoverage(comparisonCoverageInput{
		kinds: request.Kinds, desired: request.Desired.Coverage, current: request.Current.Coverage,
		related: func(record schemaext.SubjectCoverage) bool { return comparisonCoverageSubject(request, record) },
		observedValue: func(_ schemaext.Kind, subject objectidentity.ID) bool {
			return slices.ContainsFunc(request.Current.Objects.Refs(), func(ref objectidentity.ID) bool { return ref.Key() == subject.Key() })
		},
	}, result)
}

func comparisonCoverageSubject(request schemaext.ObjectComparisonRequest, record schemaext.SubjectCoverage) bool {
	if !slices.Contains(request.Kinds, record.Kind) {
		return false
	}
	if record.Subject.Kind == objectidentity.Kind(record.Kind) {
		return comparisonSubject(request, record.Subject)
	}
	return slices.ContainsFunc(request.Parents, func(parent schemaext.ParentState) bool { return parent.Subject.Key() == record.Subject.Key() })
}

func comparisonDiagnosticSubject(request schemaext.ObjectComparisonRequest, subject objectidentity.ID) bool {
	return slices.ContainsFunc(request.Parents, func(parent schemaext.ParentState) bool { return parent.Subject.Key() == subject.Key() }) || comparisonSubject(request, subject)
}

func comparisonSubject(request schemaext.ObjectComparisonRequest, subject objectidentity.ID) bool {
	if !slices.Contains(request.Kinds, schemaext.Kind(subject.Kind)) {
		return false
	}
	if !subject.Parent.Empty() {
		if _, found := comparisonParent(request.Parents, subject); !found {
			return false
		}
	}
	for _, state := range []schemaext.ObjectState{request.Desired, request.Current} {
		if slices.ContainsFunc(state.Objects.Refs(), func(ref objectidentity.ID) bool { return ref.Key() == subject.Key() }) {
			return true
		}
		if _, found := state.Coverage.SubjectKnowledge(schemaext.Kind(subject.Kind), subject); found {
			return true
		}
	}
	return false
}

func comparisonParent(parents []schemaext.ParentState, subject objectidentity.ID) (schemaext.ParentState, bool) {
	if subject.Parent.Empty() {
		return schemaext.ParentState{}, false
	}
	ref := objectidentity.ID{Kind: objectidentity.KindTable, Catalog: subject.Catalog, Schema: subject.Schema, Name: subject.Parent}
	for _, parent := range parents {
		if parent.Subject.Key() == ref.Key() {
			return parent, true
		}
	}
	return schemaext.ParentState{}, false
}
