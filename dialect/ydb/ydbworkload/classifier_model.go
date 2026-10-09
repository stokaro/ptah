package ydbworkload

import (
	"fmt"
	"unicode/utf8"

	"ptah.run/core/schemaext"
)

// DesiredClassifier records authored settings and the Go source holder. Unset
// settings remain unset; source completeness belongs to coverage records.
type DesiredClassifier struct {
	Spec       ClassifierSpec `json:"spec"`
	StructName string         `json:"struct_name,omitempty"`
}

// ObservedClassifier records captured server settings. The enclosing object
// carries the name; coverage independently describes inspection completeness.
type ObservedClassifier struct {
	Spec ClassifierSpec `json:"spec"`
}

// Kind returns the workload model identity.
func (*DesiredClassifier) Kind() schemaext.Kind { return ClassifierKind }

// Kind returns the workload model identity.
func (*ObservedClassifier) Kind() schemaext.Kind { return ClassifierKind }

// Clone returns an independent declaration and its source provenance.
func (v *DesiredClassifier) Clone() schemaext.Value {
	if v == nil {
		return (*DesiredClassifier)(nil)
	}
	return &DesiredClassifier{Spec: v.Spec, StructName: v.StructName}
}

// Clone returns an independent observation.
func (v *ObservedClassifier) Clone() schemaext.Value {
	if v == nil {
		return (*ObservedClassifier)(nil)
	}
	return &ObservedClassifier{Spec: v.Spec}
}

// Equal compares captured declarations including source provenance.
func (v *DesiredClassifier) Equal(other schemaext.Value) bool {
	right, ok := other.(*DesiredClassifier)
	if !ok {
		return false
	}
	if v == nil || right == nil {
		return v == right
	}
	return v.StructName == right.StructName && (v.Spec == right.Spec)
}

// Equal compares captured observations without inventing defaults.
func (v *ObservedClassifier) Equal(other schemaext.Value) bool {
	right, ok := other.(*ObservedClassifier)
	if !ok {
		return false
	}
	if v == nil || right == nil {
		return v == right
	}
	return v.Spec == right.Spec
}

// Observed projects configuration without manufacturing inspection evidence.
func (v *DesiredClassifier) Observed() *ObservedClassifier {
	if v == nil {
		return nil
	}
	return &ObservedClassifier{Spec: v.Spec}
}

// Desired preserves captured configuration without inventing source provenance.
func (v *ObservedClassifier) Desired() *DesiredClassifier {
	if v == nil {
		return nil
	}
	return &DesiredClassifier{Spec: v.Spec}
}

// DesiredClassifierObject captures authored configuration with an exact database name.
func DesiredClassifierObject(name, structName string, spec ClassifierSpec) schemaext.Object {
	value := &DesiredClassifier{Spec: spec, StructName: structName}
	return schemaext.Object{Ref: ClassifierRef(name), Value: value.Clone()}
}

// ObservedClassifierObject captures stored settings without an inspection claim.
func ObservedClassifierObject(name string, spec ClassifierSpec) schemaext.Object {
	value := &ObservedClassifier{Spec: spec}
	return schemaext.Object{Ref: ClassifierRef(name), Value: value.Clone()}
}

func (v *DesiredClassifier) validate() error {
	if v == nil || !utf8.ValidString(v.StructName) {
		return fmt.Errorf("%w: workload declaration requires a value and valid source provenance", schemaext.ErrInvalidValue)
	}
	return ValidateClassifier(v.Spec)
}

func (v *ObservedClassifier) validate() error {
	if v == nil {
		return fmt.Errorf("%w: workload observation requires a value", schemaext.ErrInvalidValue)
	}
	return ValidateClassifier(v.Spec)
}
