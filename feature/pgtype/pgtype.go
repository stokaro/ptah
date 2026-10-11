// Package pgtype owns the PostgreSQL user-defined data types other than
// enums: domains, composite types and range types. It holds each as a named
// object identified by its schema and name, in a desired and an observed
// representation, the change between the two, and the operations that carry
// a change to a renderer. It contains no provider selection, database access
// or SQL.
//
// The model keeps PostgreSQL's semantics. The three kinds share the namespace
// of pg_type with enums and tables, and name each other in both directions:
// a domain may be declared over a composite type, and a composite type may
// have a domain attribute. A column, a routine signature or another type may
// name one, so a plan creates them after the enums and before the tables. Each
// value defines its type through [schemaext.TypeDefiner] and each change
// through [schemaext.TypeCreator], under the common kinds
// [objectidentity.KindDomain], [objectidentity.KindComposite] and
// [objectidentity.KindRange], so a host qualifies, orders and compares by the
// type without importing this package.
package pgtype

import (
	"fmt"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/internal/tableref"
)

// Owner is the provider identity that owns every user-type codec and service.
// It is separate from the targets the services are registered for.
const Owner = "ptah.run/pgtype"

// The model kinds.
const (
	// DomainKind identifies one domain.
	DomainKind schemaext.Kind = "ptah.run/pgtype/domain"
	// CompositeKind identifies one composite type.
	CompositeKind schemaext.Kind = "ptah.run/pgtype/composite"
	// RangeKind identifies one range type.
	RangeKind schemaext.Kind = "ptah.run/pgtype/range"
)

// Kinds are the three model kinds, in the order the package documents them.
func Kinds() []schemaext.Kind { return []schemaext.Kind{DomainKind, CompositeKind, RangeKind} }

// commonKinds maps each model kind to the common kind a host reads it under.
var commonKinds = map[schemaext.Kind]objectidentity.Kind{
	DomainKind:    objectidentity.KindDomain,
	CompositeKind: objectidentity.KindComposite,
	RangeKind:     objectidentity.KindRange,
}

// labels name each model kind the way a statement and a refusal name it.
var labels = map[schemaext.Kind]string{
	DomainKind:    "domain",
	CompositeKind: "composite type",
	RangeKind:     "range type",
}

// Label names the kind of the type ref identifies, such as "domain", or is
// empty for a kind this package does not own.
func Label(ref objectidentity.ID) string { return labels[schemaext.Kind(ref.Kind)] }

// Ref builds the identity of the type of kind named schema.name under
// PostgreSQL's identifier rules. An empty schema leaves the type to the
// connection's default schema, which the identity records as defaulted.
func Ref(kind schemaext.Kind, schema, name string) objectidentity.ID {
	return RefWith(identifier.ForDialect(platform.Postgres), kind, schema, name)
}

// RefWith builds the identity under explicit identifier rules, which is how
// a comparison pairs a declaration and an observation that spelled the same
// schema differently.
func RefWith(semantics identifier.Semantics, kind schemaext.Kind, schema, name string) objectidentity.ID {
	return objectidentity.NewBuilder(semantics).SchemaScopedParts(objectidentity.Kind(kind), schema, name)
}

// ValidateRef requires a schema-scoped identity of one of the three kinds,
// with a name and no catalog, table parent or signature.
func ValidateRef(ref objectidentity.ID) error {
	label, owned := labels[schemaext.Kind(ref.Kind)]
	if !owned {
		return fmt.Errorf("%w: %q is not a PostgreSQL user type kind", schemaext.ErrInvalidValue, ref.Kind)
	}
	if strings.TrimSpace(ref.Name.Source) == "" || ref.Name.Normalized == "" || !ref.Parent.Empty() || !ref.Catalog.Empty() ||
		ref.Signature != "" {
		return fmt.Errorf("%w: a %s requires a schema-scoped identity with a name", schemaext.ErrInvalidValue, label)
	}
	return schemaext.ValidText(label+" name", ref.Name.Source)
}

// Subject returns the identity a host reads the type ref identifies under: the
// same schema and name under the common kind of its model.
func Subject(ref objectidentity.ID) objectidentity.ID {
	ref.Kind = commonKinds[schemaext.Kind(ref.Kind)]
	return ref
}

// AuthoredSchema is the schema a reference names, or empty when the reference
// took the connection's default. A statement writes exactly that, as the
// source did.
func AuthoredSchema(ref objectidentity.ID) string { return ref.Schema.Authored() }

// QualifiedName renders a reference the way the source spelled it: schema.name
// when the schema was written, and the bare name when it was defaulted. A
// part that holds a dot is written in double quotes, as a table's qualified
// name is.
func QualifiedName(ref objectidentity.ID) string {
	return qualified(AuthoredSchema(ref), ref.Name.Source)
}

// qualified is schema.name, or name where schema is empty, quoting a part
// that holds a dot.
func qualified(schema, name string) string { return tableref.Canonical(schema, name) }

// definedType is the type the object ref identifies defines, naming
// references.
func definedType(ref objectidentity.ID, references ...string) schemaext.DefinedType {
	return schemaext.DefinedType{ID: Subject(ref), References: references}
}

// validTexts checks field/value pairs in order with [schemaext.ValidText], so
// the first invalid field is the one a refusal names.
func validTexts(subject string, pairs ...string) error {
	for i := 0; i+1 < len(pairs); i += 2 {
		if err := schemaext.ValidText(subject+" "+pairs[i], pairs[i+1]); err != nil {
			return err
		}
	}
	return nil
}

// modelError names the model and representation a refusal is about, as the
// typed error the codec boundary and the census recognize.
func modelError(kind schemaext.Kind, representation schemaext.Representation, err error) error {
	return &schemaext.InvalidModelError{Kind: kind, Representation: representation, Message: err.Error()}
}
