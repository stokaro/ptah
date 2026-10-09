package schemaext

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
)

// ErrIncompleteRelations means captured state cannot establish a complete
// related-object context. A partial listing must not authorize changes.
var ErrIncompleteRelations = errors.New("feature relationships are incomplete")

// RelatedFeatures is captured context, not a request to modify its members.
// Values retains complete definitions, including every binding of a multi-table
// object. Required names their common owners and explicit dependencies. Those
// references do not prove the referenced objects exist or were inspected.
type RelatedFeatures struct {
	Values   []RelationValue
	Required []objectidentity.ID
}

// Records returns independent owner receipts in captured value order. Callers
// must retain Complete and the source Coverage; records alone are not evidence
// that all incoming references were discovered.
func (s RelationSnapshot) Records() []ValueRelations {
	result := slices.Clone(s.relations)
	for i := range result {
		result[i].Dependencies = slices.Clone(result[i].Dependencies)
	}
	return result
}

// Coverage returns the immutable source claims without adding model knowledge.
func (s RelationSnapshot) Coverage() Coverage { return s.coverage }

// CaptureRelated returns the connected feature context around the named
// subjects. It follows dependencies in both directions so a policy binding
// several tables and other policies sharing those tables stay together. Table
// ownership is derived from structured identities. Definition order follows the
// capture; references are sorted. Every selected model namespace and relation
// receipt must be complete, since an unknown subject could refer to any supplied
// target. Failures return no partial context. This does not authorize expanding
// user scope or modifying the returned prerequisites.
func (s RelationSnapshot) CaptureRelated(ctx context.Context, subjects []objectidentity.ID) (RelatedFeatures, error) {
	if err := codecContext(ctx); err != nil {
		return RelatedFeatures{}, err
	}
	if !s.ready {
		return RelatedFeatures{}, fmt.Errorf("%w: no captured relationship state", ErrIncompleteRelations)
	}
	if err := s.requireCompleteRelations(); err != nil {
		return RelatedFeatures{}, err
	}
	active := make(map[objectidentity.Key]bool)
	var queue []objectidentity.Key
	for _, subject := range subjects {
		if err := validateRelationReference(subject); err != nil {
			return RelatedFeatures{}, err
		}
		queue = enqueueRelationReference(queue, active, subject)
	}
	return s.captureConnectedRelations(ctx, queue, active)
}

func (s RelationSnapshot) captureConnectedRelations(ctx context.Context, queue []objectidentity.Key, active map[objectidentity.Key]bool) (RelatedFeatures, error) {
	selected := make([]bool, len(s.values))
	required := make(map[objectidentity.Key]objectidentity.ID)
	for head := 0; head < len(queue); head++ {
		for _, i := range s.index[queue[head]] {
			if err := ctx.Err(); err != nil {
				return RelatedFeatures{}, err
			}
			if selected[i] {
				continue
			}
			selected[i] = true
			relations := s.relations[i]
			queue = enqueueRelationReference(queue, active, relations.Subject.Subject)
			for _, ref := range relationTargets(relations) {
				queue = enqueueRelationReference(queue, active, ref)
				required[ref.Key()] = ref
			}
		}
	}
	values := make([]RelationValue, 0)
	for i, value := range s.values {
		if selected[i] {
			values = append(values, value)
		}
	}
	values, err := s.registry.snapshotRelationValues(ctx, s.representation, values)
	if err != nil {
		return RelatedFeatures{}, err
	}
	refs := make([]objectidentity.ID, 0, len(required))
	for _, ref := range required {
		refs = append(refs, ref)
	}
	slices.SortFunc(refs, CompareRefs)
	return RelatedFeatures{Values: values, Required: refs}, nil
}

func (s RelationSnapshot) requireCompleteRelations() error {
	claims := make(map[Kind]Knowledge)
	for _, record := range s.coverage.KindRecords() {
		claims[record.Model.Kind] = record.Knowledge
	}
	for _, kind := range s.kinds {
		if claims[kind].State != Complete {
			return fmt.Errorf("%w: model %q was not completely enumerated", ErrIncompleteRelations, kind)
		}
	}
	for _, record := range s.coverage.SubjectRecords() {
		if record.Knowledge.State != Complete && record.Knowledge.State != Absent {
			return fmt.Errorf("%w: %q on %s: %s", ErrIncompleteRelations, record.Kind, record.Subject, record.Knowledge.Reason)
		}
	}
	for _, record := range s.relations {
		if !record.Complete {
			return fmt.Errorf("%w: %q on %s: %s", ErrIncompleteRelations, record.Subject.Kind, record.Subject.Subject, record.Reason)
		}
	}
	return nil
}

func relationTargets(value ValueRelations) []objectidentity.ID {
	result := slices.Clone(value.Dependencies)
	if owner, owned := relationOwner(value.Subject); owned {
		result = append(result, owner)
	}
	return result
}

func relationReferenceKeys(ref objectidentity.ID) []objectidentity.Key {
	keys := []objectidentity.Key{ref.Key()}
	if !ref.Parent.Empty() {
		parent := objectidentity.ID{Kind: objectidentity.KindTable, Catalog: ref.Catalog, Schema: ref.Schema, Name: ref.Parent}
		keys = append(keys, parent.Key())
	}
	return keys
}

func relationScopeKeys(ref objectidentity.ID) []objectidentity.Key {
	keys := relationReferenceKeys(ref)
	if !ref.Schema.Empty() {
		schema := objectidentity.ID{Kind: objectidentity.KindSchema, Catalog: ref.Catalog, Name: ref.Schema}
		keys = append(keys, schema.Key())
	}
	return keys
}

func enqueueRelationReference(queue []objectidentity.Key, active map[objectidentity.Key]bool, ref objectidentity.ID) []objectidentity.Key {
	for _, key := range relationReferenceKeys(ref) {
		if !active[key] {
			active[key] = true
			queue = append(queue, key)
		}
	}
	return queue
}

func indexRelations(relations []ValueRelations) map[objectidentity.Key][]int {
	index := make(map[objectidentity.Key][]int)
	for i, value := range relations {
		seen := make(map[objectidentity.Key]bool)
		for _, ref := range append(relationTargets(value), value.Subject.Subject) {
			for _, key := range relationScopeKeys(ref) {
				if !seen[key] {
					index[key] = append(index[key], i)
					seen[key] = true
				}
			}
		}
	}
	return index
}
