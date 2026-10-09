package ydbcoordination

import (
	"fmt"
	"path"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

// Kind identifies a standalone coordination node in YDB's scheme namespace.
const Kind schemaext.Kind = "ptah.run/ydb/coordination-node"

// Desired records configuration intent. Unset fields request the server's
// defaults when the node is created or compared, not preservation of an
// existing nondefault value. An ALTER request uses a separate partial Spec.
type Desired struct {
	Spec Spec `json:"spec"`
	// StructName preserves the source holder's identity. It has no server
	// counterpart and does not change the coordination configuration.
	StructName string `json:"struct_name,omitempty"`
}

// Observed records the configuration returned by the coordination service.
// Unset fields remain unset; effective defaults belong to comparison. Read
// completeness is recorded separately in schemaext.Coverage.
type Observed struct {
	Spec Spec `json:"spec"`
}

// Kind returns the coordination-node model identity.
func (*Desired) Kind() schemaext.Kind { return Kind }

// Kind returns the coordination-node model identity.
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
	return new(*v)
}

// Equal compares captured declarations without resolving target defaults.
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

// Equal compares raw observations without resolving target defaults.
func (v *Observed) Equal(other schemaext.Value) bool {
	right, ok := other.(*Observed)
	if !ok {
		return false
	}
	if v == nil || right == nil {
		return v == right
	}
	return v.Spec == right.Spec
}

// Desired preserves every inspected setting as desired configuration.
func (v *Observed) Desired() *Desired {
	if v == nil {
		return nil
	}
	return &Desired{Spec: v.Spec}
}

// Observed projects the declaration's stored configuration. It establishes no
// inspection evidence; callers must preserve the actual coverage separately.
func (v *Desired) Observed() *Observed {
	if v == nil {
		return nil
	}
	return &Observed{Spec: v.Spec}
}

// Ref builds a schema-scoped identity from separate directory and name parts.
// A coordination node has no owning table, even when its path matches one.
func Ref(schema, name string) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("ydb")).SchemaScopedParts(objectidentity.Kind(Kind), schema, name)
}

// DesiredObject captures one authored node with separate path components and
// source provenance. The immutable collection validates and clones its value.
func DesiredObject(schema, name, structName string, spec Spec) schemaext.Object {
	return schemaext.Object{Ref: Ref(schema, name), Value: &Desired{Spec: spec, StructName: structName}}
}

// ObservedObject captures raw server settings without applying defaults or
// manufacturing source provenance. Coverage is recorded independently.
func ObservedObject(schema, name string, spec Spec) schemaext.Object {
	return schemaext.Object{Ref: Ref(schema, name), Value: &Observed{Spec: spec}}
}

// ValidateIdentity requires separate directory and leaf names with YDB's exact
// identity semantics. It permits server-owned names in unreadable coverage;
// declaring or operating on a node additionally requires ValidateRef.
func ValidateIdentity(ref objectidentity.ID) error {
	if ref.Name.Source == "" || ref != Ref(ref.Schema.Source, ref.Name.Source) || strings.Contains(ref.Name.Source, "/") ||
		(ref.Schema.Source != "" && (path.IsAbs(ref.Schema.Source) || path.Clean(ref.Schema.Source) != ref.Schema.Source)) {
		return fmt.Errorf("%w: coordination node requires a schema-scoped YDB identity", schemaext.ErrInvalidValue)
	}
	return nil
}

// ValidateRef validates the identity and protects Ptah's lock node and
// server-owned paths from authored declarations and schema operations.
func ValidateRef(ref objectidentity.ID) error {
	if err := ValidateIdentity(ref); err != nil {
		return err
	}
	if err := RefuseName(ref.Schema.Source, ref.Name.Source); err != nil {
		return fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
	}
	return nil
}
