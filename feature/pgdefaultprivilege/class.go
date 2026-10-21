package pgdefaultprivilege

import (
	"encoding/json"
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

// ClassSubjectKind identifies a catalog ACL class whose contents are unknown.
// It is a coverage subject, not a value model or a grant to a fabricated role.
const ClassSubjectKind objectidentity.Kind = "ptah.run/pgdefaultprivilege/acl-class"

// Class identifies a catalog ACL before it is split into grantee privileges.
// An empty Grantor means CockroachDB's FOR ALL ROLES. An empty Schema means
// global defaults. ObjectType preserves catalog codes the owner cannot model.
type Class struct {
	Grantor    string `json:"grantor"`
	Schema     string `json:"schema"`
	ObjectType string `json:"object_type"`
}

// ClassRef captures the scope of an unreadable ACL class. Names are decoded
// identifiers, so whitespace and punctuation inside them remain significant.
func ClassRef(class Class, semantics identifier.Semantics) (objectidentity.ID, error) {
	if err := class.Validate(); err != nil {
		return objectidentity.ID{}, err
	}
	source, err := classTuple(class.Grantor, class.ObjectType)
	if err != nil {
		return objectidentity.ID{}, err
	}
	normalized, err := classTuple(semantics.TableIdentityKey(class.Grantor), class.ObjectType)
	if err != nil {
		return objectidentity.ID{}, err
	}
	ref := objectidentity.ID{Kind: ClassSubjectKind, Name: objectidentity.Part{Source: source, Normalized: normalized}}
	if class.Schema != "" {
		ref.Schema = objectidentity.Part{Source: class.Schema, Normalized: semantics.TableIdentityKey(class.Schema)}
	}
	return ref, nil
}

// ClassOf validates a class coverage reference and restores its captured names.
// A caller with target semantics also rebuilds ClassRef to check normalization.
func ClassOf(ref objectidentity.ID) (Class, error) {
	if !ref.Database.Empty() || !ref.Namespace.IsZero() || ref.Kind != ClassSubjectKind || ref.Catalog != (objectidentity.Part{}) || ref.Parent != (objectidentity.Part{}) ||
		ref.Signature != "" || ref.Name.Quoted || ref.Name.Defaulted || ref.Schema.Quoted || ref.Schema.Defaulted {
		return Class{}, fmt.Errorf("%w: invalid default privilege class reference", schemaext.ErrInvalidValue)
	}
	source, err := decodeClassTuple(ref.Name.Source)
	if err != nil {
		return Class{}, err
	}
	normalized, err := decodeClassTuple(ref.Name.Normalized)
	if err != nil {
		return Class{}, err
	}
	class := Class{Grantor: source[0], Schema: ref.Schema.Source, ObjectType: source[1]}
	if err := class.Validate(); err != nil {
		return Class{}, err
	}
	if normalized[1] != class.ObjectType || (normalized[0] == "") != (class.Grantor == "") ||
		(ref.Schema.Source == "") != (ref.Schema.Normalized == "") {
		return Class{}, fmt.Errorf("%w: inconsistent default privilege class reference", schemaext.ErrInvalidValue)
	}
	if err := validText("normalized schema", ref.Schema.Normalized); err != nil {
		return Class{}, err
	}
	return class, nil
}

// Validate requires a catalog object class and valid identifier text. Unknown
// class codes are retained for diagnostics, without making them declarable.
func (cls Class) Validate() error {
	if err := requiredText("object type", cls.ObjectType); err != nil {
		return err
	}
	if err := validText("grantor", cls.Grantor); err != nil {
		return err
	}
	return validText("schema", cls.Schema)
}

// String names the class for a diagnostic. It is never parsed as an identity.
func (cls Class) String() string {
	grantor, scope := cls.Grantor, "globally"
	if grantor == "" {
		grantor = "ALL ROLES"
	}
	if cls.Schema != "" {
		scope = "in schema " + cls.Schema
	}
	return fmt.Sprintf("%s on %s %s", grantor, cls.ObjectType, scope)
}

func classTuple(grantor, objectType string) (string, error) {
	data, err := json.Marshal([2]string{grantor, objectType})
	if err != nil {
		return "", fmt.Errorf("%w: encode default privilege class: %v", schemaext.ErrInvalidValue, err)
	}
	return string(data), nil
}

func decodeClassTuple(encoded string) ([2]string, error) {
	values, err := schemaext.DecodeJSON[[]string]([]byte(encoded))
	if err != nil || len(values) != 2 {
		return [2]string{}, fmt.Errorf("%w: default privilege class requires a grantor and object class tuple", schemaext.ErrInvalidValue)
	}
	canonical, err := classTuple(values[0], values[1])
	if err != nil {
		return [2]string{}, err
	}
	if canonical != encoded {
		return [2]string{}, fmt.Errorf("%w: noncanonical default privilege class", schemaext.ErrInvalidValue)
	}
	for _, value := range values {
		if err := validText("class component", value); err != nil {
			return [2]string{}, err
		}
	}
	return [2]string(values), nil
}
