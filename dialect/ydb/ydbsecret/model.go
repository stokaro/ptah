package ydbsecret

import (
	"fmt"
	"path"
	"strings"
	"unicode/utf8"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

// Kind identifies one YDB secret: a scheme object at a path whose value the
// server keeps and never returns.
const Kind schemaext.Kind = "ptah.run/ydb/secret"

// Desired is a declared secret. It never holds the value.
//
// ValueEnv names the environment variable the value is read from when a
// statement that creates or rotates the secret runs. Empty selects
// [DefaultValueEnv] for the secret's path: the variable Ptah names for a secret
// no author declared, such as one converted from a database read or one a
// rollback creates again. A source declaration always names its variable.
//
// A declaration never asks for a new value. A rotation is a request of one
// comparison (see [RotationRequests]), so it cannot stay on in a schema file
// and reach every later plan.
type Desired struct {
	ValueEnv   string `json:"value_env,omitempty"`
	StructName string `json:"struct_name,omitempty"`
}

// Observed is a secret a database read listed. The server returns the path
// and nothing else, so the observation carries no field: its identity is the
// whole observation. Coverage records whether a read looked.
type Observed struct{}

// Kind returns the secret model identity.
func (*Desired) Kind() schemaext.Kind { return Kind }

// Kind returns the secret model identity.
func (*Observed) Kind() schemaext.Kind { return Kind }

// Clone returns an independent declaration snapshot.
func (v *Desired) Clone() schemaext.Value {
	if v == nil {
		return (*Desired)(nil)
	}
	return new(*v)
}

// Clone returns an independent observation snapshot.
func (v *Observed) Clone() schemaext.Value {
	if v == nil {
		return (*Observed)(nil)
	}
	return &Observed{}
}

// Equal compares captured declarations field by field. It does not resolve an
// empty ValueEnv, because the path that would resolve it is not part of the
// value.
func (v *Desired) Equal(other schemaext.Value) bool {
	right, ok := other.(*Desired)
	if !ok {
		return false
	}
	if v == nil || right == nil {
		return v == right
	}
	return *v == *right
}

// Equal reports whether other is an observed secret as well.
func (v *Observed) Equal(other schemaext.Value) bool {
	right, ok := other.(*Observed)
	if !ok {
		return false
	}
	return (v == nil) == (right == nil)
}

// Desired converts an observation into a declaration that keeps the secret.
// The read names no variable, so the declaration selects the default one.
func (v *Observed) Desired() *Desired {
	if v == nil {
		return nil
	}
	return &Desired{}
}

// Observed predicts what a read lists once the declaration is applied: the
// secret at its path, with nothing else to observe.
func (v *Desired) Observed() *Observed {
	if v == nil {
		return nil
	}
	return &Observed{}
}

// Variable returns the environment variable the secret at ref takes its value
// from: the declared one, or [DefaultValueEnv] for its path when none is
// declared.
func (v *Desired) Variable(ref objectidentity.ID) string {
	if v != nil && v.ValueEnv != "" {
		return v.ValueEnv
	}
	return DefaultValueEnv(ref.Schema.Source, ref.Name.Source)
}

// Validate refuses a declaration whose variable a value may not come from,
// and a holder name that is not valid UTF-8.
func (v *Desired) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: a desired secret is nil", schemaext.ErrInvalidValue)
	}
	if !utf8.ValidString(v.StructName) {
		return fmt.Errorf("%w: a secret's holder must be valid UTF-8", schemaext.ErrInvalidValue)
	}
	if v.ValueEnv == "" {
		return nil
	}
	if err := CheckValueEnv(v.ValueEnv); err != nil {
		return fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
	}
	return nil
}

// Ref builds the exact identity of the secret name in the directory schema,
// relative to the database root. Literal dots stay part of their component.
func Ref(schema, name string) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("ydb")).SchemaScopedParts(objectidentity.Kind(Kind), schema, name)
}

// DesiredObject records one declared secret. Collections clone the value on
// entry.
func DesiredObject(schema, name, structName, valueEnv string) schemaext.Object {
	return schemaext.Object{Ref: Ref(schema, name), Value: &Desired{ValueEnv: valueEnv, StructName: structName}}
}

// ObservedObject records one secret a read listed.
func ObservedObject(schema, name string) schemaext.Object {
	return schemaext.Object{Ref: Ref(schema, name), Value: &Observed{}}
}

// ValidateIdentity requires separate exact YDB directory and leaf names: a
// leaf without a slash, and a directory that is a clean path relative to the
// database root.
func ValidateIdentity(ref objectidentity.ID) error {
	schema, name := ref.Schema.Source, ref.Name.Source
	if strings.TrimSpace(name) == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") || strings.ContainsRune(schema, 0) ||
		!utf8.ValidString(schema) || !utf8.ValidString(name) || ref != Ref(schema, name) ||
		(schema != "" && (path.IsAbs(schema) || path.Clean(schema) != schema || schema == "." || schema == ".." || strings.HasPrefix(schema, "../"))) {
		return fmt.Errorf("%w: a secret requires a schema-scoped YDB identity", schemaext.ErrInvalidValue)
	}
	return nil
}
