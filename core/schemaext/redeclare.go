package schemaext

import (
	"fmt"
	"maps"
	"slices"

	"ptah.run/core/objectidentity"
)

// Redeclarable is an owner's statement that its object is idempotent to
// declare: declaring it a second time, with an equal value and the same
// target scope, declares nothing new. A PostgreSQL extension is the example:
// two packages that each use pg_trgm may each declare it.
//
// It is a property of the model, which its owner states, and not a way for a
// host to tolerate duplicates: a host that combines declarations keeps one of
// two equal ones, and still refuses two that differ in anything, naming the
// identity. A value that does not implement it, or answers false, keeps every
// second declaration of an identity a refusal, as [Objects.With] and
// [Objects.Merge] refuse it.
type Redeclarable interface {
	Value
	Redeclarable() bool
}

// ComposableDeclaration is an owner's rule for combining independently
// authored parts of one object, such as the privileges granted to one role.
// The operation combines local representations, as Clone and Equal do: it
// performs no I/O, calls no remote provider, and leaves both operands unchanged. Conflicting intent returns an error. Statement-order
// execution belongs to the source decoder, not to declaration composition.
//
// Objects invokes it only for the same identity and target scope. The returned
// value must retain the model kind and is captured before joining the result.
// A model that implements neither this contract nor Redeclarable keeps duplicate
// declarations as errors.
type ComposableDeclaration interface {
	Value
	ComposeDeclaration(Value) (Value, error)
}

// Guarded is an owner's statement that its object was declared under the
// statement's "may already exist" clause, as CREATE EXTENSION IF NOT EXISTS
// declares one. A SQL schema directory runs its files in order, so it admits
// a later file's guarded declaration of an object an earlier file declared,
// as the server running them would, and refuses an unguarded one. Whether the
// two declarations then combine is [Redeclarable]'s question, not this one's.
type Guarded interface {
	Value
	Guarded() bool
}

// Declare returns a new collection containing object. A second declaration of
// an identity follows its owner's representation contract: a
// [ComposableDeclaration] combines both values, and a [Redeclarable] accepts
// equal values. Composition takes precedence when a value implements both.
// Different target scopes and duplicates without either permission wrap
// [ErrDuplicate]. Composition errors propagate. Invalid results wrap
// [ErrInvalidValue]. On any error the result is empty and o is unchanged.
func (o Objects) Declare(object Object) (Objects, error) {
	cloned, err := cloneObject(object)
	if err != nil {
		return Objects{}, err
	}
	held, found := o.values[cloned.Ref.Key()]
	if !found {
		return o.With(cloned)
	}
	combined, err := combineDeclaration(held, cloned)
	if err != nil {
		return Objects{}, err
	}
	result := Objects{values: make(map[objectidentity.Key]Object, len(o.values))}
	maps.Copy(result.values, o.values)
	result.values[cloned.Ref.Key()] = combined
	return result, nil
}

// MergeDeclarations combines independently captured declarations, as
// [Objects.Merge] does, under the rule [Objects.Declare] applies to an
// identity both hold. Where several identities both hold are refused, the
// refusal names the first in [CompareRefs] order.
func (o Objects) MergeDeclarations(other Objects) (Objects, error) {
	result := Objects{values: make(map[objectidentity.Key]Object, len(o.values)+len(other.values))}
	maps.Copy(result.values, o.values)
	for _, value := range other.ordered() {
		key := value.Ref.Key()
		if held, exists := result.values[key]; exists {
			combined, err := combineDeclaration(held, value)
			if err != nil {
				return Objects{}, err
			}
			result.values[key] = combined
			continue
		}
		result.values[key] = value
	}
	return result, nil
}

// redeclare refuses declared as a second declaration of held's identity,
// unless the owner states the object is idempotent to declare and the two
// are one declaration.
func redeclare(held, declared Object) error {
	redeclarable, ok := declared.Value.(Redeclarable)
	if !ok || !redeclarable.Redeclarable() {
		return fmt.Errorf("%w: object %s", ErrDuplicate, declared.Ref)
	}
	if !held.Value.Equal(declared.Value) || !slices.Equal(held.Targets, declared.Targets) {
		return fmt.Errorf("%w: conflicting definitions of %s", ErrDuplicate, declared.Ref)
	}
	return nil
}

func combineDeclaration(held, declared Object) (Object, error) {
	_, ok := held.Value.(ComposableDeclaration)
	if !ok {
		if err := redeclare(held, declared); err != nil {
			return Object{}, err
		}
		return held, nil
	}
	if !slices.Equal(held.Targets, declared.Targets) {
		return Object{}, fmt.Errorf("%w: conflicting target scopes of %s", ErrDuplicate, declared.Ref)
	}
	// Capture operands even for implementations whose accidental mutation would
	// otherwise leak into a source collection after a refusal.
	left, err := cloneObject(held)
	if err != nil {
		return Object{}, err
	}
	right, err := cloneObject(declared)
	if err != nil {
		return Object{}, err
	}
	composer, ok := left.Value.(ComposableDeclaration)
	if !ok {
		return Object{}, fmt.Errorf("%w: declaration clone loses composition for %s", ErrInvalidValue, held.Ref)
	}
	combined, err := composer.ComposeDeclaration(right.Value)
	if err != nil {
		return Object{}, err
	}
	if err := ValidatePayload(combined); err != nil {
		return Object{}, err
	}
	if combined.Kind() != held.Value.Kind() {
		return Object{}, fmt.Errorf("%w: declaration composition changes model kind for %s", ErrInvalidValue, declared.Ref)
	}
	left.Value = combined
	return cloneObject(left)
}
