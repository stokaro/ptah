package schemaext

import (
	"cmp"
	"fmt"
	"maps"
	"slices"

	"ptah.run/core/objectidentity"
)

// Object is one independently addressable feature-owned schema object. Ref
// carries the existing structured identity, including any owning table. Its
// kind equals Value.Kind; state is never hidden in a namespace-sized facet.
type Object struct {
	Ref   objectidentity.ID
	Value Value
}

// MarshalJSON refuses implicit serialization of the concrete value. Encode the
// object through Registry.EncodeObjects to retain its owner and definition.
func (Object) MarshalJSON() ([]byte, error) { return nil, ErrExplicitCodec }

// UnmarshalJSON refuses reconstruction without the selected value codec.
func (*Object) UnmarshalJSON([]byte) error { return ErrExplicitCodec }

// Objects is an immutable collection keyed by structured comparison identity.
// Its zero value is empty. Stored payloads cannot be reached without cloning.
type Objects struct {
	values map[objectidentity.Key]Object
}

// NewObjects validates and snapshots independently named feature objects.
// Duplicate identities, including normalized collisions, are refused.
func NewObjects(objects ...Object) (Objects, error) {
	result := Objects{values: make(map[objectidentity.Key]Object, len(objects))}
	for _, object := range objects {
		cloned, err := cloneObject(object)
		if err != nil {
			return Objects{}, err
		}
		if _, found := result.values[cloned.Ref.Key()]; found {
			return Objects{}, fmt.Errorf("%w: object %s", ErrDuplicate, cloned.Ref)
		}
		result.values[cloned.Ref.Key()] = cloned
	}
	return result, nil
}

func cloneObject(object Object) (Object, error) {
	value, err := CloneValue(object.Value)
	if err != nil {
		return Object{}, err
	}
	if object.Ref.Kind != objectidentity.Kind(value.Kind()) || object.Ref.Name.Source == "" || object.Ref.Name.Normalized == "" {
		return Object{}, fmt.Errorf("%w: object identity does not name value kind %q", ErrInvalidValue, value.Kind())
	}
	return Object{Ref: object.Ref, Value: value}, nil
}

// With returns a new collection containing object, refusing an existing identity.
func (o Objects) With(object Object) (Objects, error) {
	cloned, err := cloneObject(object)
	if err != nil {
		return Objects{}, err
	}
	if _, found := o.values[cloned.Ref.Key()]; found {
		return Objects{}, fmt.Errorf("%w: object %s", ErrDuplicate, cloned.Ref)
	}
	result := Objects{values: make(map[objectidentity.Key]Object, len(o.values)+1)}
	maps.Copy(result.values, o.values)
	result.values[cloned.Ref.Key()] = cloned
	return result, nil
}

// Without returns a new collection without ref. It does not infer cascade rules.
func (o Objects) Without(ref objectidentity.ID) Objects {
	result := Objects{values: maps.Clone(o.values)}
	delete(result.values, ref.Key())
	return result
}

// Get returns a cloned object, preserving its recorded source spelling.
func (o Objects) Get(ref objectidentity.ID) (Object, bool, error) {
	object, found := o.values[ref.Key()]
	if !found {
		return Object{}, false, nil
	}
	cloned, err := cloneObject(object)
	return cloned, true, err
}

// All returns cloned objects in structured identity order. Diagnostic strings
// are not ordering keys: dots inside name components must not hide boundaries.
func (o Objects) All() ([]Object, error) {
	result := make([]Object, 0, len(o.values))
	for _, object := range o.values {
		cloned, err := cloneObject(object)
		if err != nil {
			return nil, err
		}
		result = append(result, cloned)
	}
	slices.SortFunc(result, func(a, b Object) int { return CompareRefs(a.Ref, b.Ref) })
	return result, nil
}

// CompareRefs orders structured comparison identities component by component.
// Source spelling remains in each ID for rendering and diagnostics.
func CompareRefs(a, b objectidentity.ID) int {
	return cmp.Or(
		cmp.Compare(a.Kind, b.Kind),
		cmp.Compare(a.Catalog.Normalized, b.Catalog.Normalized),
		cmp.Compare(a.Schema.Normalized, b.Schema.Normalized),
		cmp.Compare(a.Parent.Normalized, b.Parent.Normalized),
		cmp.Compare(a.Name.Normalized, b.Name.Normalized),
		cmp.Compare(a.Signature, b.Signature),
	)
}

// Len returns the number of individually addressable objects.
func (o Objects) Len() int { return len(o.values) }

// IsZero reports an empty collection for explicit omitzero model fields.
func (o Objects) IsZero() bool { return o.Len() == 0 }

// MarshalJSON refuses implicit interface serialization. Use Registry codecs.
func (Objects) MarshalJSON() ([]byte, error) { return nil, ErrExplicitCodec }

// UnmarshalJSON refuses decoding without the selected codec registry.
func (*Objects) UnmarshalJSON([]byte) error { return ErrExplicitCodec }
