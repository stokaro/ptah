package ydbstreaming

import (
	"fmt"
	"path"
	"strings"
	"unicode/utf8"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

// Kind identifies one standalone YDB streaming query.
const Kind schemaext.Kind = "ptah.run/ydb/streaming-query"

// Desired captures authored configuration and permission to reset aggregation
// state when changing the body. The permission is not stored in the database.
type Desired struct {
	Spec            Spec   `json:"spec"`
	StructName      string `json:"struct_name,omitempty"`
	AllowStateReset bool   `json:"allow_state_reset"`
}

// Observed captures stored configuration. Execution status, retries, offsets,
// and checkpoints are runtime data. Coverage records inspection separately.
type Observed struct {
	Spec Spec `json:"spec"`
}

// Kind returns the streaming-query model identity.
func (*Desired) Kind() schemaext.Kind { return Kind }

// Kind returns the streaming-query model identity.
func (*Observed) Kind() schemaext.Kind { return Kind }

// Clone returns an independent declaration, including the optional Run value.
func (v *Desired) Clone() schemaext.Value {
	if v == nil {
		return (*Desired)(nil)
	}
	cloned := *v
	cloned.Spec = v.Spec.Clone()
	return &cloned
}

// Clone returns an independent observation, including the optional Run value.
func (v *Observed) Clone() schemaext.Value {
	if v == nil {
		return (*Observed)(nil)
	}
	return &Observed{Spec: v.Spec.Clone()}
}

// Equal compares captured declarations without normalizing server defaults.
func (v *Desired) Equal(other schemaext.Value) bool {
	right, ok := other.(*Desired)
	if !ok {
		return false
	}
	if v == nil || right == nil {
		return v == right
	}
	return v.StructName == right.StructName && v.AllowStateReset == right.AllowStateReset && v.Spec.SameSettings(right.Spec)
}

// Equal compares raw observations without normalizing query text or defaults.
func (v *Observed) Equal(other schemaext.Value) bool {
	right, ok := other.(*Observed)
	if !ok {
		return false
	}
	if v == nil || right == nil {
		return v == right
	}
	return v.Spec.SameSettings(right.Spec)
}

// Desired preserves inspected configuration without granting reset permission.
func (v *Observed) Desired() *Desired {
	if v == nil {
		return nil
	}
	return &Desired{Spec: v.Spec.Clone()}
}

// Observed projects stored settings without manufacturing inspection evidence.
func (v *Desired) Observed() *Observed {
	if v == nil {
		return nil
	}
	return &Observed{Spec: v.Spec.Clone()}
}

// Ref builds an exact schema-scoped identity from directory and leaf names.
func Ref(schema, name string) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("ydb")).SchemaScopedParts(objectidentity.Kind(Kind), schema, name)
}

// DesiredObject records one authored query. Collections clone the value on entry.
func DesiredObject(schema, name, structName string, spec Spec, allowStateReset bool) schemaext.Object {
	return schemaext.Object{Ref: Ref(schema, name), Value: &Desired{Spec: spec.Clone(), StructName: structName, AllowStateReset: allowStateReset}}
}

// ObservedObject records stored settings without granting reset permission.
func ObservedObject(schema, name string, spec Spec) schemaext.Object {
	return schemaext.Object{Ref: Ref(schema, name), Value: &Observed{Spec: spec.Clone()}}
}

// ValidateIdentity requires separate exact YDB directory and leaf names.
func ValidateIdentity(ref objectidentity.ID) error {
	schema, name := ref.Schema.Source, ref.Name.Source
	if strings.TrimSpace(name) == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") || strings.ContainsRune(schema, 0) ||
		!utf8.ValidString(schema) || !utf8.ValidString(name) || ref != Ref(schema, name) ||
		(schema != "" && (path.IsAbs(schema) || path.Clean(schema) != schema || schema == "." || schema == ".." || strings.HasPrefix(schema, "../"))) {
		return fmt.Errorf("%w: streaming query requires a schema-scoped YDB identity", schemaext.ErrInvalidValue)
	}
	return nil
}
