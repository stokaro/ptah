package schemaext

import (
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
)

// DefinedType is a named data type a feature object defines, as a host that
// orders, qualifies or compares by type name reads it: what the type is
// called and which types its own definition names.
type DefinedType struct {
	// ID is the type's identity under a common kind:
	// [objectidentity.KindDomain], [objectidentity.KindComposite] or
	// [objectidentity.KindRange]. Its parts keep the spelling the
	// declaration or the catalog gave the type. A schema the declaration left
	// to the connection's default is a defaulted part, and
	// [objectidentity.Part.Authored] is the schema the declaration wrote.
	ID objectidentity.ID
	// References are the types the definition names, each spelled as the
	// definition writes it: a domain's base type, a composite type's
	// attribute types, a range type's subtype. A host creates a type after
	// the types it names.
	References []string
}

// TypeDefiner is a feature value whose object defines a named data type that
// a column, a routine's signature or another type may name, such as a
// PostgreSQL domain. A host reads the type through it without importing the
// owner: to create a routine whose signature names the type after it, to
// qualify a column's type with the schema the type is declared in, and to
// tell a column typed by a domain from one typed by its base type.
//
// DefinedType is given the object's identity, which holds what a value alone
// may not, such as the type's schema. It returns the type the object defines.
type TypeDefiner interface {
	DefinedType(ref objectidentity.ID) DefinedType
}

// TypeCreator is a change value whose plan creates a data type, drops one and
// creates it again, or changes one in place, as [TypeDefiner] describes the
// type. A host that places a routine after the types a plan creates reads the
// type through it: such a routine is created after every type the plan
// touches, the way the host places one after an enum it creates.
//
// CreatedType is given the change's subject. It reports false for a change
// that leaves no type behind, such as a drop.
type TypeCreator interface {
	CreatedType(subject objectidentity.ID) (DefinedType, bool)
}

// DefinedTypes returns the types the objects define, in identity order. An
// object whose value is not a [TypeDefiner] defines none. Every answer is a
// valid one: a collection refuses an object whose value answers with a kind
// other than the three type kinds, or with no name, when the object enters
// it, so a host reads the types without a refusal of its own to handle.
func DefinedTypes(objects Objects) []DefinedType {
	var result []DefinedType
	for _, ref := range objects.Refs() {
		if definer, ok := objects.values[ref.Key()].Value.(TypeDefiner); ok {
			result = append(result, cloneDefinedType(definer.DefinedType(ref)))
		}
	}
	return result
}

// validDefinedObject refuses an object whose value is a [TypeDefiner] that
// answers with an invalid type, as every collection does when the object
// enters it.
func validDefinedObject(object Object) error {
	definer, ok := object.Value.(TypeDefiner)
	if !ok {
		return nil
	}
	if err := validDefinedType(definer.DefinedType(object.Ref)); err != nil {
		return fmt.Errorf("object %s: %w", object.Ref, err)
	}
	return nil
}

// CreatedTypes returns the types the changes create, recreate or change in
// place, in change order. A change whose value is not a [TypeCreator], or
// that reports no type, creates none. A change reaches a host without passing
// a collection, so an answer under a kind other than the three type kinds, or
// without a name, is refused here with [ErrInvalidValue] rather than left
// out: a host that cannot see a type orders as if it did not exist.
func CreatedTypes(changes []ChangeRecord) ([]DefinedType, error) {
	var result []DefinedType
	for _, change := range changes {
		creator, ok := change.Value.(TypeCreator)
		if !ok {
			continue
		}
		created, found := creator.CreatedType(change.Subject)
		if !found {
			continue
		}
		if err := validDefinedType(created); err != nil {
			return nil, fmt.Errorf("change of %s: %w", change.Subject, err)
		}
		result = append(result, cloneDefinedType(created))
	}
	return result, nil
}

var definedTypeKinds = []objectidentity.Kind{objectidentity.KindDomain, objectidentity.KindComposite, objectidentity.KindRange}

func validDefinedType(defined DefinedType) error {
	if !slices.Contains(definedTypeKinds, defined.ID.Kind) {
		return fmt.Errorf("%w: a defined type is a domain, a composite type or a range type, not %q", ErrInvalidValue, defined.ID.Kind)
	}
	if defined.ID.Name.Source == "" || defined.ID.Name.Normalized == "" {
		return fmt.Errorf("%w: a defined type has a name", ErrInvalidValue)
	}
	return nil
}

func cloneDefinedType(defined DefinedType) DefinedType {
	defined.References = slices.Clone(defined.References)
	return defined
}
