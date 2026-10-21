package pgdefaultprivilege

import (
	"encoding/json"
	"fmt"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

// Identity identifies a default-privilege object. Schema is the IN SCHEMA
// clause; an empty schema is global and never inherits a connection default.
// Grantor and grantee are separate roles. A grantee does not establish ownership.
type Identity struct {
	Grantor    string `json:"grantor"`
	Schema     string `json:"schema"`
	ObjectType string `json:"object_type"`
	Grantee    string `json:"grantee"`
}

// Ref validates identity and constructs its comparison identity under semantics.
// The object has no catalog name. Its owner-defined name encodes the grantor,
// object class, and grantee as a JSON tuple, so punctuation inside an identifier
// cannot create a component boundary. Catalog, Parent, and Signature stay empty.
// Role and schema names are decoded identifiers and retain every byte.
func Ref(identity Identity, semantics identifier.Semantics) (objectidentity.ID, error) {
	identity = identity.canonical()
	if err := identity.Validate(); err != nil {
		return objectidentity.ID{}, err
	}
	source, err := tuple(identity.Grantor, identity.ObjectType, identity.Grantee)
	if err != nil {
		return objectidentity.ID{}, err
	}
	normalized, err := tuple(semantics.TableIdentityKey(identity.Grantor), identity.ObjectType, semantics.TableIdentityKey(identity.Grantee))
	if err != nil {
		return objectidentity.ID{}, err
	}
	ref := objectidentity.ID{Kind: objectidentity.Kind(DefaultPrivilegeKind), Name: objectidentity.Part{Source: source, Normalized: normalized}}
	if identity.Schema != "" {
		ref.Schema = objectidentity.Part{Source: identity.Schema, Normalized: semantics.TableIdentityKey(identity.Schema)}
	}
	return ref, nil
}

func tuple(grantor, objectType, grantee string) (string, error) {
	encoded, err := json.Marshal([3]string{grantor, objectType, grantee})
	if err != nil {
		return "", fmt.Errorf("%w: encode default privilege identity: %v", schemaext.ErrInvalidValue, err)
	}
	return string(encoded), nil
}

// IdentityOf validates the shape of ref and returns its source identity. It
// checks the normalized tuple's shape without choosing the target's folding
// rules. A service holding those rules can rebuild the identity with Ref.
func IdentityOf(ref objectidentity.ID) (Identity, error) {
	if !ref.Database.Empty() || !ref.Namespace.IsZero() || ref.Kind != objectidentity.Kind(DefaultPrivilegeKind) || ref.Catalog != (objectidentity.Part{}) ||
		ref.Parent != (objectidentity.Part{}) || ref.Signature != "" || ref.Name.Quoted || ref.Name.Defaulted ||
		ref.Schema.Quoted || ref.Schema.Defaulted {
		return Identity{}, fmt.Errorf("%w: invalid default privilege reference", schemaext.ErrInvalidValue)
	}
	source, err := decodeTuple(ref.Name.Source)
	if err != nil {
		return Identity{}, err
	}
	normalized, err := decodeTuple(ref.Name.Normalized)
	if err != nil {
		return Identity{}, err
	}
	identity := Identity{Grantor: source[0], Schema: ref.Schema.Source, ObjectType: source[1], Grantee: source[2]}
	if err := identity.Validate(); err != nil {
		return Identity{}, err
	}
	if identity != identity.canonical() || normalized[1] != identity.ObjectType || normalized[0] == "" || normalized[2] == "" ||
		(ref.Schema.Source == "") != (ref.Schema.Normalized == "") {
		return Identity{}, fmt.Errorf("%w: inconsistent default privilege reference", schemaext.ErrInvalidValue)
	}
	if err := validText("normalized schema", ref.Schema.Normalized); err != nil {
		return Identity{}, err
	}
	return identity, nil
}

func decodeTuple(encoded string) ([3]string, error) {
	var values []string
	if err := schemaext.ValidText("default privilege identity", encoded); err != nil {
		return [3]string{}, err
	}
	if err := json.Unmarshal([]byte(encoded), &values); err != nil || len(values) != 3 {
		return [3]string{}, fmt.Errorf("%w: default privilege identity requires a role, object class, and grantee tuple", schemaext.ErrInvalidValue)
	}
	canonical, err := tuple(values[0], values[1], values[2])
	if err != nil {
		return [3]string{}, err
	}
	if canonical != encoded {
		return [3]string{}, fmt.Errorf("%w: noncanonical default privilege identity", schemaext.ErrInvalidValue)
	}
	for _, value := range values {
		if err := validText("identity component", value); err != nil {
			return [3]string{}, err
		}
	}
	return [3]string(values), nil
}

func (i Identity) canonical() Identity {
	return Identity{Grantor: i.Grantor, Schema: i.Schema, ObjectType: strings.ToUpper(strings.TrimSpace(i.ObjectType)), Grantee: i.Grantee}
}

// Validate requires named roles and a supported object class. SCHEMAS and LARGE
// OBJECTS are global defaults and cannot carry an IN SCHEMA clause.
func (i Identity) Validate() error {
	for _, field := range []struct{ name, value string }{{"grantor", i.Grantor}, {"object type", i.ObjectType}, {"grantee", i.Grantee}} {
		if err := requiredText(field.name, field.value); err != nil {
			return err
		}
	}
	if err := validText("schema", i.Schema); err != nil {
		return err
	}
	switch i.ObjectType {
	case "TABLES", "SEQUENCES", "FUNCTIONS", "TYPES":
	case "SCHEMAS", "LARGE OBJECTS":
		if i.Schema != "" {
			return fmt.Errorf("%w: default privileges on %s cannot name a schema", schemaext.ErrInvalidValue, i.ObjectType)
		}
	default:
		return fmt.Errorf("%w: unsupported default privilege object type %q", schemaext.ErrInvalidValue, i.ObjectType)
	}
	return nil
}

// String names the roles, object class, and scope for a diagnostic. It is not
// an identity key and is never parsed back into an identity.
func (i Identity) String() string {
	where := "globally"
	if i.Schema != "" {
		where = "in schema " + i.Schema
	}
	return fmt.Sprintf("%s on %s %s to %s", i.Grantor, i.ObjectType, where, i.Grantee)
}
