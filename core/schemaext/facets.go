package schemaext

import (
	"fmt"
	"maps"
	"slices"
)

// Facets is an immutable collection with at most one value of each kind.
// Its zero value is empty. Copying a Facets value shares only private snapshots;
// insertion and lookup clone values, and no method exposes the stored map.
type Facets struct {
	values map[Kind]Value
	scopes map[Kind][]string
}

// NewFacets validates and snapshots the supplied values. Duplicate kinds are
// errors even when their values compare equal.
func NewFacets(values ...Value) (Facets, error) {
	result := Facets{values: make(map[Kind]Value, len(values))}
	for _, value := range values {
		cloned, err := CloneValue(value)
		if err != nil {
			return Facets{}, err
		}
		if _, found := result.values[cloned.Kind()]; found {
			return Facets{}, fmt.Errorf("%w: facet %q", ErrDuplicate, cloned.Kind())
		}
		result.values[cloned.Kind()] = cloned
	}
	return result, nil
}

// With returns a new collection containing value. It refuses an existing kind;
// intentional replacement uses Replace so merging cannot silently overwrite it.
func (f Facets) With(value Value) (Facets, error) {
	cloned, err := CloneValue(value)
	if err != nil {
		return Facets{}, err
	}
	if f.declares(cloned.Kind()) {
		return Facets{}, fmt.Errorf("%w: facet %q", ErrDuplicate, cloned.Kind())
	}
	return f.replaced(cloned), nil
}

// Replace returns a new collection with an existing kind replaced. It refuses
// a missing kind so a caller's update cannot silently become an insertion.
func (f Facets) Replace(value Value) (Facets, error) {
	cloned, err := CloneValue(value)
	if err != nil {
		return Facets{}, err
	}
	if _, found := f.values[cloned.Kind()]; !found {
		return Facets{}, fmt.Errorf("%w: cannot replace missing facet %q", ErrInvalidValue, cloned.Kind())
	}
	return f.replaced(cloned), nil
}

func (f Facets) replaced(value Value) Facets {
	result := Facets{values: make(map[Kind]Value, len(f.values)+1), scopes: f.scopes}
	maps.Copy(result.values, f.values)
	result.values[value.Kind()] = value
	return result
}

// Without returns a collection without kind. The original remains unchanged.
func (f Facets) Without(kind Kind) Facets {
	result := Facets{values: maps.Clone(f.values), scopes: maps.Clone(f.scopes)}
	delete(result.values, kind)
	delete(result.scopes, kind)
	return result
}

// Merge combines independent declarations and refuses duplicate kinds, even
// when their values compare equal. The result shares only immutable snapshots.
func (f Facets) Merge(other Facets) (Facets, error) {
	result := Facets{values: make(map[Kind]Value, len(f.values)+len(other.values)), scopes: make(map[Kind][]string, len(f.scopes)+len(other.scopes))}
	maps.Copy(result.values, f.values)
	maps.Copy(result.scopes, f.scopes)
	for _, kind := range other.DeclaredKinds() {
		if f.declares(kind) {
			return Facets{}, fmt.Errorf("%w: facet %q", ErrDuplicate, kind)
		}
	}
	maps.Copy(result.values, other.values)
	maps.Copy(result.scopes, other.scopes)
	return result, nil
}

// Get returns an independent value and whether kind is present. A clone error
// returns no value; absence is never reported in place of a malformed payload.
func (f Facets) Get(kind Kind) (Value, bool, error) {
	value, found := f.values[kind]
	if !found {
		return nil, false, nil
	}
	cloned, err := CloneValue(value)
	return cloned, true, err
}

// FacetAs returns a typed snapshot. A present kind with a different concrete
// type is an error, rather than an absent or empty value.
func FacetAs[T Value](facets Facets, kind Kind) (T, bool, error) {
	var zero T
	value, found, err := facets.Get(kind)
	if err != nil || !found {
		return zero, found, err
	}
	typed, ok := value.(T)
	if !ok {
		return zero, true, fmt.Errorf("%w: facet %q has unexpected type %T", ErrInvalidValue, kind, value)
	}
	return typed, true, nil
}

// Kinds returns the concrete values' sorted kind identities in a fresh slice.
// Use DeclaredKinds when exclusions must survive a collection transformation.
func (f Facets) Kinds() []Kind { return slices.Sorted(maps.Keys(f.values)) }

// Values returns independent snapshots ordered by kind.
func (f Facets) Values() ([]Value, error) {
	result := make([]Value, 0, len(f.values))
	for _, kind := range f.Kinds() {
		value, _, err := f.Get(kind)
		if err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, nil
}

// Len returns the number of concrete facet values, excluding scoped omissions.
func (f Facets) Len() int { return len(f.values) }

// IsZero reports an empty collection for explicit omitzero model fields.
func (f Facets) IsZero() bool { return f.Len() == 0 && len(f.scopes) == 0 }

// MarshalJSON refuses implicit interface serialization. Use Registry codecs.
func (Facets) MarshalJSON() ([]byte, error) { return nil, ErrExplicitCodec }

// UnmarshalJSON refuses decoding without the selected codec registry.
func (*Facets) UnmarshalJSON([]byte) error { return ErrExplicitCodec }
