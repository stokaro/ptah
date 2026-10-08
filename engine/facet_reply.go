package engine

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

type facetSubject struct {
	kind schemaext.Kind
	ref  objectidentity.Key
}

func distinctFacetChanges(changes []schemaext.FacetChange) error {
	seen := make(map[facetSubject]bool)
	for _, change := range changes {
		key := facetSubject{change.Change.Value.Kind(), change.Change.Subject.Key()}
		if seen[key] {
			return fmt.Errorf("%w: facet comparers returned duplicate change %q for %s", schemaext.ErrInvalidValue, key.kind, change.Change.Subject)
		}
		seen[key] = true
	}
	return nil
}

func (r *Runtime) validateFacetReply(ctx context.Context, service int, request schemaext.FacetComparisonRequest, result schemaext.FacetComparisonResult) (schemaext.FacetComparisonResult, error) {
	if !result.Complete {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: facet comparison did not complete", schemaext.ErrInvalidValue)
	}
	var err error
	result.Desired, err = r.codecs.SnapshotFacetState(ctx, schemaext.Desired, result.Desired)
	if err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	if err := facetDesired(request, result.Desired); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	result.Changes = slices.Clone(result.Changes)
	seen, payloads := make(map[facetSubject]bool), make(map[facetSubject]bool)
	for i, change := range result.Changes {
		owner, found := facetOwner(request.Owners, change.Change.Subject)
		key := facetSubject{change.Kind, change.Change.Subject.Key()}
		if !slices.Contains(request.Kinds, change.Kind) || !found || !owner.Desired || !owner.Current || seen[key] {
			return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: unrelated, duplicate, or parent-owned facet change for %s", schemaext.ErrInvalidValue, change.Change.Subject)
		}
		if !facetCanChange(request, change.Kind, change.Change.Subject) {
			return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: facet change lacks declared intent or observed state for %s/%q", schemaext.ErrInvalidValue, change.Change.Subject, change.Kind)
		}
		captured, err := r.codecs.SnapshotChanges(ctx, []schemaext.ChangeRecord{change.Change})
		if err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
		payloadKey := facetSubject{captured[0].Value.Kind(), key.ref}
		if !slices.Contains(r.facetServices[service].ChangeKinds, payloadKey.kind) || payloads[payloadKey] {
			return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: unrelated or duplicate facet change payload %q", schemaext.ErrInvalidValue, payloadKey.kind)
		}
		result.Changes[i].Change = captured[0]
		seen[key], payloads[payloadKey] = true, true
	}
	result.Undecided = slices.Clone(result.Undecided)
	if err := validateFacetDiagnostics(request, result.Undecided, seen); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	return result, nil
}

func validateFacetDiagnostics(request schemaext.FacetComparisonRequest, undecided []schemaext.UndecidedChange, changes map[facetSubject]bool) error {
	diagnostics := make(map[facetSubject]bool)
	for _, diagnostic := range undecided {
		key := facetSubject{diagnostic.Kind, diagnostic.Subject.Key()}
		_, found := facetOwner(request.Owners, diagnostic.Subject)
		if !request.Includes(diagnostic.Kind, diagnostic.Subject) || !slices.Contains(request.Kinds, diagnostic.Kind) || !found || strings.TrimSpace(diagnostic.Reason) == "" || diagnostics[key] || changes[key] {
			return fmt.Errorf("%w: invalid facet diagnostic for %s", schemaext.ErrInvalidValue, diagnostic.Subject)
		}
		diagnostics[key] = true
	}
	return nil
}

func retainFacetDeclarations(request schemaext.FacetComparisonRequest, result schemaext.FacetState) error {
	for _, original := range request.Desired.Records {
		returned := facetValues(result, original.Subject)
		for _, kind := range original.Values.DeclaredKinds() {
			if !sameFacetScope(original.Values, returned, kind) {
				return fmt.Errorf("%w: facet comparison changed source target scope for %s/%q", schemaext.ErrInvalidValue, original.Subject, kind)
			}
			if !slices.Contains(original.Values.Kinds(), kind) {
				continue
			}
			before, _, err := original.Values.Get(kind)
			if err != nil {
				return err
			}
			after, found, err := returned.Get(kind)
			if err != nil {
				return err
			}
			if !found || !before.Equal(after) {
				return fmt.Errorf("%w: facet comparison discarded an explicit declaration for %s/%q", schemaext.ErrInvalidValue, original.Subject, kind)
			}
		}
	}
	return nil
}

func facetDesired(request schemaext.FacetComparisonRequest, result schemaext.FacetState) error {
	if err := retainFacetDeclarations(request, result); err != nil {
		return err
	}
	for _, record := range result.Records {
		owner, found := facetOwner(request.Owners, record.Subject)
		if !found || !owner.Desired {
			return fmt.Errorf("%w: facet comparison returned a missing or removed owner %s", schemaext.ErrInvalidValue, record.Subject)
		}
		for _, kind := range record.Values.DeclaredKinds() {
			original := facetValues(request.Desired, record.Subject)
			if len(record.Values.TargetScope(kind)) != 0 && !sameFacetScope(original, record.Values, kind) {
				return fmt.Errorf("%w: facet comparison invented target scope for %s/%q", schemaext.ErrInvalidValue, record.Subject, kind)
			}
			if !request.Includes(kind, record.Subject) && sameFacetScope(original, record.Values, kind) {
				continue
			}
			if !slices.Contains(request.Kinds, kind) || !facetHasSource(request, kind, record.Subject) {
				return fmt.Errorf("%w: facet comparison invented an unrelated value for %s/%q", schemaext.ErrInvalidValue, record.Subject, kind)
			}
		}
	}
	return validateComparisonCoverage(comparisonCoverageInput{
		kinds: request.Kinds, desired: request.Desired.Coverage, current: request.Current.Coverage,
		related: func(record schemaext.SubjectCoverage) bool {
			_, found := facetOwner(request.Owners, record.Subject)
			return found && slices.Contains(request.Kinds, record.Kind) && request.Includes(record.Kind, record.Subject)
		},
		observedValue: func(kind schemaext.Kind, subject objectidentity.ID) bool {
			return slices.Contains(facetValues(request.Current, subject).Kinds(), kind)
		},
	}, result.Coverage)
}

func facetValues(state schemaext.FacetState, subject objectidentity.ID) schemaext.Facets {
	for _, record := range state.Records {
		if record.Subject.Key() == subject.Key() {
			return record.Values
		}
	}
	return schemaext.Facets{}
}

func facetHasSource(request schemaext.FacetComparisonRequest, kind schemaext.Kind, subject objectidentity.ID) bool {
	if !request.Includes(kind, subject) {
		return false
	}
	return slices.Contains(facetValues(request.Desired, subject).Kinds(), kind) ||
		slices.Contains(facetValues(request.Current, subject).Kinds(), kind) ||
		request.Desired.Coverage.Lookup(kind, subject).State == schemaext.Defaulted
}

func facetCanChange(request schemaext.FacetComparisonRequest, kind schemaext.Kind, subject objectidentity.ID) bool {
	if !request.Includes(kind, subject) {
		return false
	}
	if knowledge, explicit := request.Desired.Coverage.SubjectKnowledge(kind, subject); explicit &&
		(knowledge.State == schemaext.Uninspected || knowledge.State == schemaext.Unrepresentable) {
		return false
	}
	desired := request.Desired.Coverage.Lookup(kind, subject).State
	intent := slices.Contains(facetValues(request.Desired, subject).Kinds(), kind) ||
		desired == schemaext.Complete || desired == schemaext.Absent || desired == schemaext.Defaulted
	if !intent {
		return false
	}
	current := request.Current.Coverage.Lookup(kind, subject).State
	if current == schemaext.Complete || current == schemaext.Absent {
		return true
	}
	// A concrete value is positive evidence even when sibling subjects were
	// not enumerated, but an explicit limitation on this value takes priority.
	knowledge, limited := request.Current.Coverage.SubjectKnowledge(kind, subject)
	return slices.Contains(facetValues(request.Current, subject).Kinds(), kind) &&
		(!limited || knowledge.State == schemaext.Complete)
}

func sortFacetReply(result *schemaext.FacetComparisonResult) {
	slices.SortFunc(result.Changes, func(a, b schemaext.FacetChange) int {
		return cmp.Or(schemaext.CompareRefs(a.Change.Subject, b.Change.Subject), cmp.Compare(a.Kind, b.Kind))
	})
	slices.SortFunc(result.Undecided, func(a, b schemaext.UndecidedChange) int {
		return cmp.Or(schemaext.CompareRefs(a.Subject, b.Subject), cmp.Compare(a.Kind, b.Kind))
	})
}
