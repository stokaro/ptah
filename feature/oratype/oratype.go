// Package oratype owns Oracle's user-defined data types: domains, which Oracle
// 23 creates with CREATE DOMAIN, and object types, created with CREATE TYPE
// ... AS OBJECT. It holds each as a named object identified by its schema and
// name, in a desired and an observed representation, the change between the
// two, the operations that carry a change to a renderer, and the services that
// compare, convert, plan, reverse, report and render them for the Oracle
// target. It opens no database connection.
//
// An object type here is the subset the model can describe: an ordered list of
// named, typed attributes with no methods, no supertype and no collection
// shape, FINAL and instantiable. Each value defines its type through [schemaext.TypeDefiner] under the
// common kinds [objectidentity.KindDomain] and [objectidentity.KindComposite],
// so a host orders and qualifies by the type without importing this package.
package oratype

import (
	"fmt"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/tableref"
)

// Owner is the provider identity that owns every codec and service of the
// package.
const Owner = "ptah.run/oratype"

// The model kinds.
const (
	// DomainKind identifies one domain.
	DomainKind schemaext.Kind = "ptah.run/oratype/domain"
	// ObjectTypeKind identifies one object type.
	ObjectTypeKind schemaext.Kind = "ptah.run/oratype/object-type"
)

// Kinds are the two model kinds, a domain's first.
func Kinds() []schemaext.Kind { return []schemaext.Kind{DomainKind, ObjectTypeKind} }

// commonKinds maps each model kind to the common kind a host reads it under.
var commonKinds = map[schemaext.Kind]objectidentity.Kind{
	DomainKind:     objectidentity.KindDomain,
	ObjectTypeKind: objectidentity.KindComposite,
}

// labels name each model kind the way a statement and a refusal name it.
var labels = map[schemaext.Kind]string{
	DomainKind:     "domain",
	ObjectTypeKind: "object type",
}

// Label names the kind of the type ref identifies, such as "domain", or is
// empty for a kind this package does not own.
func Label(ref objectidentity.ID) string { return labels[schemaext.Kind(ref.Kind)] }

// Ref builds the identity of the type of kind named schema.name under Oracle's
// identifier rules, which compare names without regard to ASCII case. An empty
// schema leaves the type to the connection's schema.
func Ref(kind schemaext.Kind, schema, name string) objectidentity.ID {
	return RefWith(identifier.ForDialect(platform.Oracle), kind, schema, name)
}

// RefWith builds the identity under explicit identifier rules, which is how a
// comparison pairs a declaration that names the connection's schema with an
// observation that names none. Components are kept as the source wrote them.
func RefWith(semantics identifier.Semantics, kind schemaext.Kind, schema, name string) objectidentity.ID {
	ref := objectidentity.NewBuilder(semantics).TablePartsVerbatim(schema, name)
	ref.Kind = objectidentity.Kind(kind)
	return ref
}

// ValidateRef requires a schema-scoped identity of one of the two kinds, with a
// name and no catalog, table parent or signature.
func ValidateRef(ref objectidentity.ID) error {
	label, owned := labels[schemaext.Kind(ref.Kind)]
	if !owned {
		return fmt.Errorf("%w: %q is not an Oracle user type kind", schemaext.ErrInvalidValue, ref.Kind)
	}
	if strings.TrimSpace(ref.Name.Source) == "" || ref.Name.Normalized == "" || !ref.Parent.Empty() || !ref.Database.Empty() || !ref.Namespace.IsZero() || !ref.Catalog.Empty() ||
		!ref.Signature.IsZero() {
		return fmt.Errorf("%w: the %s identity is schema-scoped and has a name", schemaext.ErrInvalidValue, label)
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
// took the connection's schema.
func AuthoredSchema(ref objectidentity.ID) string { return ref.Schema.Authored() }

// QualifiedName renders a reference the way the source spelled it: schema.name
// when the schema was written, and the bare name otherwise.
func QualifiedName(ref objectidentity.ID) string {
	return tableref.Canonical(AuthoredSchema(ref), ref.Name.Source)
}

// statementName is a qualified name as an Oracle statement writes it, the
// spelling the Oracle renderer gives every other object: the dotted name split
// outside quotes, each part without one level of quoting, bare where Oracle
// accepts it bare and double-quoted otherwise. A plain name is written bare,
// so it folds to upper case as the tables that name the type do.
func statementName(schema, name string) string {
	parts := splitQualified(tableref.Canonical(schema, name))
	for i, part := range parts {
		parts[i] = identifierName(part)
	}
	return strings.Join(parts, ".")
}

// identifierName is one name part as an Oracle statement writes it.
func identifierName(part string) string {
	return sqlident.Ident(platform.Oracle, unquote(part))
}

// unquote removes one level of double quotes or backticks from a part and
// undoubles the quote inside it.
func unquote(part string) string {
	if len(part) >= 2 {
		switch {
		case part[0] == '"' && part[len(part)-1] == '"':
			return strings.ReplaceAll(part[1:len(part)-1], `""`, `"`)
		case part[0] == '`' && part[len(part)-1] == '`':
			return strings.ReplaceAll(part[1:len(part)-1], "``", "`")
		}
	}
	return part
}

// splitQualified splits on the dots outside double quotes and backticks. Each
// part is a slice of the input, so a byte that is not a delimiter comes back
// as it arrived.
func splitQualified(name string) []string {
	var parts []string
	start, inQuotes, inBackticks := 0, false, false
	for i := range len(name) {
		switch name[i] {
		case '"':
			inQuotes = !inQuotes
		case '`':
			inBackticks = !inBackticks
		case '.':
			if !inQuotes && !inBackticks {
				parts = append(parts, name[start:i])
				start = i + 1
			}
		}
	}
	return append(parts, name[start:])
}

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
// typed error the codec boundary recognizes.
func modelError(kind schemaext.Kind, representation schemaext.Representation, err error) error {
	return &schemaext.InvalidModelError{Kind: kind, Representation: representation, Message: err.Error()}
}

// supported reports whether target is Oracle, the one target the services
// accept.
func supported(target string) bool { return platform.NormalizeDialect(target) == platform.Oracle }
