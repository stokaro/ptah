package schemaext

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
)

// RelationSnapshot retains captured definitions and their validated references.
// It is immutable. Source coverage remains separate from the owner's account of
// a concrete value, so a newer runtime cannot manufacture inspected absence.
// Its zero value is uninitialized, not a complete empty dependency graph.
type RelationSnapshot struct {
	registry       Registry
	representation Representation
	kinds          []Kind
	coverage       Coverage
	values         []RelationValue
	relations      []ValueRelations
	index          map[objectidentity.Key][]int
	ready          bool
}

// MarshalJSON refuses an implicit encoding that would discard captured models.
func (RelationSnapshot) MarshalJSON() ([]byte, error) { return nil, ErrExplicitCodec }

// UnmarshalJSON requires reconstruction through validated owner discovery.
func (*RelationSnapshot) UnmarshalJSON([]byte) error { return ErrExplicitCodec }

type relationKey struct {
	kind      Kind
	placement Placement
	subject   objectidentity.Key
}

func relationIdentity(subject RelationSubject) relationKey {
	return relationKey{subject.Kind, subject.Placement, subject.Subject.Key()}
}

func validateRelationSubject(subject RelationSubject) error {
	if !subject.Kind.Valid() || (subject.Placement != ObjectPlacement && subject.Placement != FacetPlacement) ||
		(subject.Placement == ObjectPlacement && subject.Subject.Kind != objectidentity.Kind(subject.Kind)) {
		return fmt.Errorf("%w: invalid relation subject", ErrInvalidValue)
	}
	return validateRelationReference(subject.Subject)
}

func validateRelationReference(ref objectidentity.ID) error {
	if ref.Kind == "" || ref.Name.Source == "" || ref.Name.Normalized == "" {
		return fmt.Errorf("%w: relation requires a structured reference", ErrInvalidValue)
	}
	if _, err := objectidentity.Resolve(objectidentity.Reference{Kind: ref.Kind, ID: ref}, []objectidentity.ID{ref}); err != nil {
		return fmt.Errorf("%w: invalid relation reference: %w", ErrInvalidValue, err)
	}
	return nil
}

func (r Registry) snapshotRelationValues(ctx context.Context, representation Representation, values []RelationValue) ([]RelationValue, error) {
	if err := codecContext(ctx); err != nil {
		return nil, err
	}
	if representation != Desired && representation != Observed {
		return nil, fmt.Errorf("%w: relations require a schema representation", ErrInvalidValue)
	}
	seen := make(map[relationKey]bool, len(values))
	payloads := make([]Value, len(values))
	for i, value := range values {
		if err := validateRelationSubject(value.Subject); err != nil {
			return nil, err
		}
		key := relationIdentity(value.Subject)
		if seen[key] {
			return nil, fmt.Errorf("%w: duplicate relation subject", ErrInvalidValue)
		}
		seen[key] = true
		payloads[i] = value.Value
	}
	payloads, err := r.SnapshotValues(ctx, representation, payloads)
	if err != nil {
		return nil, err
	}
	result := slices.Clone(values)
	for i, payload := range payloads {
		if payload.Kind() != result[i].Subject.Kind {
			return nil, fmt.Errorf("%w: relation value changed its model kind", ErrInvalidValue)
		}
		result[i].Value = payload
	}
	return result, nil
}

// SnapshotRelationRequest validates a complete discovery input before an owner
// is called. It clones definitions through selected codecs, checks source/model
// consistency, and freezes the ordered model list. It adds no coverage claims.
func (r Registry) SnapshotRelationRequest(ctx context.Context, request RelationRequest) (RelationRequest, error) {
	values, err := r.snapshotRelationValues(ctx, request.Representation, request.Values)
	if err != nil {
		return RelationRequest{}, err
	}
	if _, err := r.EncodeCoverage(ctx, request.Representation, request.Coverage); err != nil {
		return RelationRequest{}, err
	}
	kinds := slices.Clone(request.Kinds)
	slices.Sort(kinds)
	for i, kind := range kinds {
		if !kind.Valid() || (i > 0 && kind == kinds[i-1]) {
			return RelationRequest{}, fmt.Errorf("%w: invalid or duplicate relation model", ErrInvalidValue)
		}
		if _, found := r.codecs[codecKey{kind: kind, representation: request.Representation}]; !found {
			return RelationRequest{}, &UnknownCodecError{Kind: kind, Representation: request.Representation}
		}
	}
	for _, record := range request.Coverage.KindRecords() {
		if !slices.Contains(kinds, record.Model.Kind) {
			return RelationRequest{}, fmt.Errorf("%w: relation discovery omitted a captured model", ErrInvalidValue)
		}
	}
	for _, value := range values {
		if request.Coverage.Lookup(value.Subject.Kind, value.Subject.Subject).State == Absent {
			return RelationRequest{}, fmt.Errorf("%w: relation definition contradicts captured absence", ErrInvalidValue)
		}
		if !slices.Contains(kinds, value.Subject.Kind) {
			return RelationRequest{}, fmt.Errorf("%w: relation discovery omitted a value's model", ErrInvalidValue)
		}
	}
	request.Values, request.Kinds = values, kinds
	request.Capabilities = request.Capabilities.Clone()
	request.Identifiers = request.Identifiers.Clone()
	if err := ctx.Err(); err != nil {
		return RelationRequest{}, err
	}
	return request, nil
}

// AcceptRelations validates an owner's ordered receipts and captures an
// independent snapshot. Kinds lists every selected model whose namespace must
// be known before an incoming dependency query can establish completeness.
// This function preserves Coverage verbatim; results cannot upgrade it.
func (r Registry) AcceptRelations(ctx context.Context, request RelationRequest, result RelationResult) (RelationSnapshot, error) {
	request, err := r.SnapshotRelationRequest(ctx, request)
	if err != nil {
		return RelationSnapshot{}, err
	}
	if !result.Complete || len(result.Values) != len(request.Values) {
		return RelationSnapshot{}, fmt.Errorf("%w: relation discovery omitted completion or receipts", ErrInvalidValue)
	}
	relations := make([]ValueRelations, len(request.Values))
	for i, value := range request.Values {
		if value.Subject != result.Values[i].Subject {
			return RelationSnapshot{}, fmt.Errorf("%w: relation discovery changed an ordered subject", ErrInvalidValue)
		}
		relations[i], err = acceptValueRelations(result.Values[i])
		if err != nil {
			return RelationSnapshot{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return RelationSnapshot{}, err
	}
	return RelationSnapshot{registry: r, representation: request.Representation, kinds: request.Kinds,
		coverage: request.Coverage, values: request.Values, relations: relations,
		index: indexRelations(relations), ready: true}, nil
}

func acceptValueRelations(value ValueRelations) (ValueRelations, error) {
	if (!value.Complete && strings.TrimSpace(value.Reason) == "") || (value.Complete && value.Reason != "") {
		return ValueRelations{}, fmt.Errorf("%w: unresolved relations require a reason", ErrInvalidValue)
	}
	dependencies := slices.Clone(value.Dependencies)
	seen := make(map[objectidentity.Key]bool, len(dependencies))
	owner, owned := relationOwner(value.Subject)
	for _, dependency := range dependencies {
		if err := validateRelationReference(dependency); err != nil {
			return ValueRelations{}, err
		}
		if seen[dependency.Key()] || (owned && owner.Key() == dependency.Key()) {
			return ValueRelations{}, fmt.Errorf("%w: duplicate relation or repeated ownership", ErrInvalidValue)
		}
		seen[dependency.Key()] = true
	}
	slices.SortFunc(dependencies, CompareRefs)
	value.Dependencies = dependencies
	return value, nil
}

func relationOwner(subject RelationSubject) (objectidentity.ID, bool) {
	ref := subject.Subject
	if subject.Placement == FacetPlacement {
		return ref, true
	}
	if !ref.Parent.Empty() {
		return objectidentity.ID{Kind: objectidentity.KindTable, Catalog: ref.Catalog, Schema: ref.Schema, Name: ref.Parent}, true
	}
	return objectidentity.ID{}, false
}
