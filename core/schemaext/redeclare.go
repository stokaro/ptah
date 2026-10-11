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

// Declare returns a new collection containing object, as [Objects.With]
// does, except for an identity declared again. A [Redeclarable] object equal
// to the one the collection holds, value and target scope, leaves the
// collection as it is; one that differs is refused, naming the identity.
// Every other second declaration is refused as [Objects.With] refuses it.
// Both refusals wrap [ErrDuplicate].
func (o Objects) Declare(object Object) (Objects, error) {
	cloned, err := cloneObject(object)
	if err != nil {
		return Objects{}, err
	}
	held, found := o.values[cloned.Ref.Key()]
	if !found {
		return o.With(cloned)
	}
	if err := redeclare(held, cloned); err != nil {
		return Objects{}, err
	}
	return o, nil
}

// MergeDeclarations combines independently captured declarations, as
// [Objects.Merge] does, under the rule [Objects.Declare] applies to an
// identity both hold.
func (o Objects) MergeDeclarations(other Objects) (Objects, error) {
	result := Objects{values: make(map[objectidentity.Key]Object, len(o.values)+len(other.values))}
	maps.Copy(result.values, o.values)
	for key, value := range other.values {
		if held, exists := result.values[key]; exists {
			if err := redeclare(held, value); err != nil {
				return Objects{}, err
			}
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
