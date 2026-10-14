package oratype

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/internal/pgtypealias"
)

// DesiredObjectType declares an object type: an ordered list of named, typed
// attributes, with no methods and no supertype. The type's schema and name are
// its object identity, never fields here.
type DesiredObjectType struct {
	// Attributes are the type's attributes in declaration order, which is the
	// order the server stores them in.
	Attributes []Attribute `json:"attributes"`
	// Comment is written as a SQL comment line before the type's creation.
	// Oracle stores no comment on a type, so a read never reports one.
	Comment string `json:"comment,omitempty"`
	// StructName preserves the Go struct a declaration was read from. It has
	// no server counterpart and does not change the type.
	StructName string `json:"struct_name,omitempty"`
}

// ObservedObjectType is an object type as the catalog reports it.
type ObservedObjectType struct {
	// Attributes are the type's attributes in catalog order, each type in the
	// catalog's own spelling.
	Attributes []Attribute `json:"attributes"`
}

// Attribute is one attribute of an object type.
type Attribute struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// Kind returns the object type model identity.
func (*DesiredObjectType) Kind() schemaext.Kind { return ObjectTypeKind }

// Kind returns the object type model identity.
func (*ObservedObjectType) Kind() schemaext.Kind { return ObjectTypeKind }

// DefinedType reports the type under [objectidentity.KindComposite], naming
// the types of its attributes; see [schemaext.TypeDefiner].
func (v *DesiredObjectType) DefinedType(ref objectidentity.ID) schemaext.DefinedType {
	return definedType(ref, attributeTypes(v.Attributes)...)
}

// DefinedType reports the type under [objectidentity.KindComposite], naming
// the types of its attributes; see [schemaext.TypeDefiner].
func (v *ObservedObjectType) DefinedType(ref objectidentity.ID) schemaext.DefinedType {
	return definedType(ref, attributeTypes(v.Attributes)...)
}

func attributeTypes(attributes []Attribute) []string {
	types := make([]string, 0, len(attributes))
	for _, attribute := range attributes {
		types = append(types, attribute.Type)
	}
	return types
}

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (v *DesiredObjectType) Clone() schemaext.Value { return v.Copy() }

// Copy is [DesiredObjectType.Clone] without the interface. A nil receiver
// returns nil.
func (v *DesiredObjectType) Copy() *DesiredObjectType {
	if v == nil {
		return nil
	}
	copied := *v
	copied.Attributes = slices.Clone(v.Attributes)
	return &copied
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedObjectType) Clone() schemaext.Value { return v.Copy() }

// Copy is [ObservedObjectType.Clone] without the interface. A nil receiver
// returns nil.
func (v *ObservedObjectType) Copy() *ObservedObjectType {
	if v == nil {
		return nil
	}
	return &ObservedObjectType{Attributes: slices.Clone(v.Attributes)}
}

// Equal reports whether other declares the same type in the same words, its
// attributes in the same order. Two nil declarations are equal.
func (v *DesiredObjectType) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredObjectType)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return slices.Equal(v.Attributes, w.Attributes) && v.Comment == w.Comment && v.StructName == w.StructName
}

// Equal reports whether other observes the same type. Two nil observations
// are equal.
func (v *ObservedObjectType) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedObjectType)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return slices.Equal(v.Attributes, w.Attributes)
}

// Desired is the declaration that keeps the observed type as it is.
func (v *ObservedObjectType) Desired() *DesiredObjectType {
	if v == nil {
		return nil
	}
	return &DesiredObjectType{Attributes: slices.Clone(v.Attributes)}
}

// Observed is the observation a declaration predicts: its attributes, as
// written.
func (v *DesiredObjectType) Observed() *ObservedObjectType {
	if v == nil {
		return nil
	}
	return &ObservedObjectType{Attributes: slices.Clone(v.Attributes)}
}

// SameAttributes reports whether two attribute lists describe one type: the
// same names in the same order without regard to case, each with the same
// type once both spellings are canonicalized.
func SameAttributes(declared, held []Attribute) bool {
	return slices.EqualFunc(declared, held, func(a, b Attribute) bool {
		return strings.EqualFold(a.Name, b.Name) && pgtypealias.Canonical(a.Type) == pgtypealias.Canonical(b.Type)
	})
}

// ValidateDesiredObjectType checks representation invariants: at least one
// attribute, each with a name and a type, no name twice, and representable
// text.
func ValidateDesiredObjectType(v *DesiredObjectType) error {
	if v == nil {
		return modelError(ObjectTypeKind, schemaext.Desired, fmt.Errorf("%w: nil object type declaration", schemaext.ErrInvalidValue))
	}
	if err := validAttributes(v.Attributes); err != nil {
		return modelError(ObjectTypeKind, schemaext.Desired, err)
	}
	if err := validTexts("object type", "comment", v.Comment, "struct name", v.StructName); err != nil {
		return modelError(ObjectTypeKind, schemaext.Desired, err)
	}
	return nil
}

// ValidateObservedObjectType checks the same invariants of an observation.
func ValidateObservedObjectType(v *ObservedObjectType) error {
	if v == nil {
		return modelError(ObjectTypeKind, schemaext.Observed, fmt.Errorf("%w: nil object type observation", schemaext.ErrInvalidValue))
	}
	if err := validAttributes(v.Attributes); err != nil {
		return modelError(ObjectTypeKind, schemaext.Observed, err)
	}
	return nil
}

func validAttributes(attributes []Attribute) error {
	if len(attributes) == 0 {
		return fmt.Errorf("%w: an object type requires at least one attribute", schemaext.ErrInvalidValue)
	}
	seen := make(map[string]bool, len(attributes))
	for _, attribute := range attributes {
		if strings.TrimSpace(attribute.Name) == "" || strings.TrimSpace(attribute.Type) == "" {
			return fmt.Errorf("%w: an object type attribute has a name and a type", schemaext.ErrInvalidValue)
		}
		if err := validTexts("object type", "attribute name", attribute.Name, "attribute type", attribute.Type); err != nil {
			return err
		}
		if seen[attribute.Name] {
			return fmt.Errorf("%w: object type attribute %q is declared twice", schemaext.ErrInvalidValue, attribute.Name)
		}
		seen[attribute.Name] = true
	}
	return nil
}
