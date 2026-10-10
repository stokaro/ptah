package schemaext

import (
	"fmt"
	"maps"
	"slices"
)

// WithTargetScope binds an existing value to source-declared target names.
// Names are syntax-checked and normalized, but only a selected runtime resolves
// their meaning. An empty list removes the binding. The original is unchanged.
func (f Facets) WithTargetScope(kind Kind, targets ...string) (Facets, error) {
	if _, found := f.values[kind]; !found {
		return Facets{}, fmt.Errorf("%w: target scope requires a concrete facet %q", ErrInvalidValue, kind)
	}
	scope, err := normalizeTargetScope(targets)
	if err != nil {
		return Facets{}, err
	}
	result := Facets{values: f.values, scopes: maps.Clone(f.scopes)}
	if len(scope) == 0 {
		delete(result.scopes, kind)
		return result, nil
	}
	if result.scopes == nil {
		result.scopes = make(map[Kind][]string)
	}
	result.scopes[kind] = scope
	return result, nil
}

// TargetScope returns an independent snapshot of the declaration's target names.
// Empty means unrestricted. A binding remains after ForTarget excludes a value.
func (f Facets) TargetScope(kind Kind) []string { return slices.Clone(f.scopes[kind]) }

// DeclaredKinds returns concrete and deliberately excluded kinds in sorted order.
// An excluded kind retains a nonempty TargetScope and has no value in Get.
func (f Facets) DeclaredKinds() []Kind {
	kinds := make(map[Kind]bool, len(f.values)+len(f.scopes))
	for kind := range f.values {
		kinds[kind] = true
	}
	for kind := range f.scopes {
		kinds[kind] = true
	}
	return slices.Sorted(maps.Keys(kinds))
}

// HasTargetScopes reports source bindings, including projected exclusions.
func (f Facets) HasTargetScopes() bool { return len(f.scopes) != 0 }

// ForTarget projects concrete values without consulting their model codecs.
// Exclusions keep their source binding so repeated projection and comparison
// cannot turn an intentional omission into deletion intent. A previously
// excluded value cannot be restored for an included target: use its source
// declaration instead. The collection shares only immutable private snapshots.
func (f Facets) ForTarget(target TargetSelection) (Facets, error) {
	if err := target.Validate(); err != nil {
		return Facets{}, err
	}
	result := Facets{values: maps.Clone(f.values), scopes: f.scopes}
	for kind, scope := range f.scopes {
		if !target.Includes(scope) {
			delete(result.values, kind)
			continue
		}
		if _, found := result.values[kind]; !found {
			return Facets{}, fmt.Errorf("%w: facet %q was excluded from this captured declaration; project the source for %q", ErrInvalidValue, kind, target.Name())
		}
	}
	return result, nil
}

func (f Facets) declares(kind Kind) bool {
	_, present := f.values[kind]
	_, scoped := f.scopes[kind]
	return present || scoped
}

// normalizeTargetScope normalizes and sorts a facet's or an object's target
// names and refuses a name given twice.
func normalizeTargetScope(targets []string) ([]string, error) {
	result := make([]string, len(targets))
	for i, name := range targets {
		normalized, err := NormalizeTargetSpelling(name)
		if err != nil {
			return nil, err
		}
		result[i] = normalized
	}
	slices.Sort(result)
	if len(slices.Compact(slices.Clone(result))) != len(result) {
		return nil, fmt.Errorf("%w: duplicate target name", ErrDuplicate)
	}
	return result, nil
}
