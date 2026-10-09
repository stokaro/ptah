package ydbworkload

import (
	"fmt"
	"unicode/utf8"

	"ptah.run/core/schemaext"
)

// DesiredPool records authored settings and the Go source holder. Unset
// settings remain unset; source completeness belongs to coverage records.
type DesiredPool struct {
	Spec       PoolSpec `json:"spec"`
	StructName string   `json:"struct_name,omitempty"`
}

// ObservedPool records captured server settings. The enclosing object
// carries the name; coverage independently describes inspection completeness.
type ObservedPool struct {
	Spec PoolSpec `json:"spec"`
}

// Kind returns the workload model identity.
func (*DesiredPool) Kind() schemaext.Kind { return PoolKind }

// Kind returns the workload model identity.
func (*ObservedPool) Kind() schemaext.Kind { return PoolKind }

// Clone returns an independent declaration and its source provenance.
func (v *DesiredPool) Clone() schemaext.Value {
	if v == nil {
		return (*DesiredPool)(nil)
	}
	return &DesiredPool{Spec: v.Spec.Clone(), StructName: v.StructName}
}

// Clone returns an independent observation.
func (v *ObservedPool) Clone() schemaext.Value {
	if v == nil {
		return (*ObservedPool)(nil)
	}
	return &ObservedPool{Spec: v.Spec.Clone()}
}

// Equal compares captured declarations including source provenance.
func (v *DesiredPool) Equal(other schemaext.Value) bool {
	right, ok := other.(*DesiredPool)
	if !ok {
		return false
	}
	if v == nil || right == nil {
		return v == right
	}
	return v.StructName == right.StructName && (PoolsEqual(v.Spec, right.Spec))
}

// Equal compares captured observations without inventing defaults.
func (v *ObservedPool) Equal(other schemaext.Value) bool {
	right, ok := other.(*ObservedPool)
	if !ok {
		return false
	}
	if v == nil || right == nil {
		return v == right
	}
	return PoolsEqual(v.Spec, right.Spec)
}

// Observed projects configuration without manufacturing inspection evidence.
func (v *DesiredPool) Observed() *ObservedPool {
	if v == nil {
		return nil
	}
	return &ObservedPool{Spec: v.Spec.Clone()}
}

// Desired preserves captured configuration without inventing source provenance.
func (v *ObservedPool) Desired() *DesiredPool {
	if v == nil {
		return nil
	}
	return &DesiredPool{Spec: v.Spec.Clone()}
}

// DesiredPoolObject captures authored configuration with an exact database name.
func DesiredPoolObject(name, structName string, spec PoolSpec) schemaext.Object {
	value := &DesiredPool{Spec: spec, StructName: structName}
	return schemaext.Object{Ref: PoolRef(name), Value: value.Clone()}
}

// ObservedPoolObject captures stored settings without an inspection claim.
func ObservedPoolObject(name string, spec PoolSpec) schemaext.Object {
	value := &ObservedPool{Spec: spec}
	return schemaext.Object{Ref: PoolRef(name), Value: value.Clone()}
}

func (v *DesiredPool) validate() error {
	if v == nil || !utf8.ValidString(v.StructName) {
		return fmt.Errorf("%w: workload declaration requires a value and valid source provenance", schemaext.ErrInvalidValue)
	}
	return ValidatePool(v.Spec)
}

func (v *ObservedPool) validate() error {
	if v == nil {
		return fmt.Errorf("%w: workload observation requires a value", schemaext.ErrInvalidValue)
	}
	return ValidatePool(v.Spec)
}
