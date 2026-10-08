package schemaext

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
)

// FacetRecord attaches typed feature settings to one common object. Its subject
// is the object's identity, not a synthetic feature object with another name.
// Several models may attach to the same subject through Values.
type FacetRecord struct {
	Subject objectidentity.ID
	Values  Facets
}

// FacetState captures attached values and independent source knowledge. Missing
// values alone establish neither absence nor a request for target defaults.
type FacetState struct {
	Records  []FacetRecord
	Coverage Coverage
}

// FacetComparisonRequest compares attached settings against captured target
// facts. Owners describes common object lifecycles even when neither source has
// a facet value. Includes determines whether each model applies to an owner;
// an exclusion for one model does not exclude another on the same owner. The
// service performs no inspection or mutation of its inputs.
type FacetComparisonRequest struct {
	Target       string
	Identifiers  identifier.Semantics
	Capabilities capability.Capabilities
	Kinds        []Kind
	Desired      FacetState
	Current      FacetState
	Owners       []ParentState
}

// Includes reports whether a prepared request compares kind on subject. The
// runtime projects source bindings before dispatch. A deliberate exclusion
// permits neither changes, adopted values, nor undecided diagnostics. Providers
// must check this for each model/owner pair, even under complete source coverage.
func (r FacetComparisonRequest) Includes(kind Kind, subject objectidentity.ID) bool {
	for _, record := range r.Desired.Records {
		if record.Subject.Key() == subject.Key() {
			return !slices.Contains(record.Values.DeclaredKinds(), kind) || slices.Contains(record.Values.Kinds(), kind)
		}
	}
	return true
}

// FacetChange names the model whose setting changes. A common subject can have
// changes to several models, so Subject alone does not identify the change.
type FacetChange struct {
	Kind   Kind
	Change ChangeRecord
}

// FacetComparisonResult is a completed comparison of one owner's models. It
// retains explicit declarations and knowledge limits. Parent creation/removal
// owns its attached settings; Changes describes only surviving common objects.
type FacetComparisonResult struct {
	Complete  bool
	Desired   FacetState
	Changes   []FacetChange
	Undecided []UndecidedChange
}

// FacetComparisonService compares an entire batch of attached settings. Errors
// and cancellation discard the whole reply. A successful reply sets Complete,
// including when the captured states require no changes.
type FacetComparisonService interface {
	CompareFacets(context.Context, FacetComparisonRequest) (FacetComparisonResult, error)
}

// SnapshotFacetState validates recorded model definitions and captures every
// concrete value through its selected local codec. Duplicate common subjects
// are errors, including when their value sets are disjoint.
func (r Registry) SnapshotFacetState(ctx context.Context, representation Representation, state FacetState) (FacetState, error) {
	if _, err := r.EncodeCoverage(ctx, representation, state.Coverage); err != nil {
		return FacetState{}, err
	}
	result := FacetState{Coverage: state.Coverage, Records: make([]FacetRecord, 0, len(state.Records))}
	seen := make(map[objectidentity.Key]bool)
	for _, record := range state.Records {
		ref := record.Subject
		if ref.Kind == "" || ref.Name.Source == "" || ref.Name.Normalized == "" || seen[ref.Key()] {
			return FacetState{}, fmt.Errorf("%w: invalid or duplicate facet subject %s", ErrInvalidValue, ref)
		}
		seen[ref.Key()] = true
		facets, err := r.SnapshotFacets(ctx, representation, record.Values)
		if err != nil {
			return FacetState{}, err
		}
		result.Records = append(result.Records, FacetRecord{Subject: ref, Values: facets})
	}
	slices.SortFunc(result.Records, func(a, b FacetRecord) int { return CompareRefs(a.Subject, b.Subject) })
	return result, nil
}
