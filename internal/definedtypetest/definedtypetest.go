// Package definedtypetest gives tests a feature object that defines a data
// type, as a feature owner's domain, composite type or range type does,
// without the owner. The hosts that qualify, order and compare by type name
// read such an object through [schemaext.TypeDefiner] alone, so a test of a
// host does not need the owner to reach that path.
//
// It is library code only tests import.
package definedtypetest

import (
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

// Kind is the model kind of the objects [Object] builds. No runtime registers
// it.
const Kind schemaext.Kind = "example.org/definedtypetest/type"

// Value defines a data type under the common kind TypeKind, naming
// References. It is the value of the objects [Object] builds.
type Value struct {
	TypeKind   objectidentity.Kind
	References []string
}

// Kind returns [Kind].
func (*Value) Kind() schemaext.Kind { return Kind }

// Clone returns an independent value.
func (v *Value) Clone() schemaext.Value {
	return &Value{TypeKind: v.TypeKind, References: slices.Clone(v.References)}
}

// Equal reports whether other is a [Value] defining the same kind of type
// with the same references.
func (v *Value) Equal(other schemaext.Value) bool {
	w, ok := other.(*Value)
	return ok && w.TypeKind == v.TypeKind && slices.Equal(w.References, v.References)
}

// DefinedType returns the type the object ref names: ref under TypeKind.
func (v *Value) DefinedType(ref objectidentity.ID) schemaext.DefinedType {
	ref.Kind = v.TypeKind
	return schemaext.DefinedType{ID: ref, References: slices.Clone(v.References)}
}

// Object returns an object that defines the data type schema.name of
// typeKind, which names references, with the identifier rules of PostgreSQL.
// An empty schema leaves the type to the connection's default schema.
func Object(typeKind objectidentity.Kind, schema, name string, references ...string) schemaext.Object {
	ref := objectidentity.NewBuilder(identifier.ForDialect("postgres")).SchemaScopedParts(objectidentity.Kind(Kind), schema, name)
	return schemaext.Object{Ref: ref, Value: &Value{TypeKind: typeKind, References: references}}
}
