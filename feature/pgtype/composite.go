package pgtype

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// DesiredComposite declares a composite type: an ordered list of named,
// typed attributes. The type's schema and name are its object identity, never
// fields here.
type DesiredComposite struct {
	// Attributes are the type's attributes in declaration order, which is
	// the order the server stores them in.
	Attributes []Attribute `json:"attributes"`
	// Comment is the type's comment, empty for none.
	Comment string `json:"comment,omitempty"`
	// StructName preserves the Go struct a declaration was read from. It has
	// no server counterpart and does not change the type.
	StructName string `json:"struct_name,omitempty"`
}

// ObservedComposite is a composite type as the catalog reports it.
type ObservedComposite struct {
	// Attributes are the type's attributes in catalog order, each type in the
	// catalog's own spelling.
	Attributes []Attribute `json:"attributes"`
	// Comment is the type's comment, empty for none.
	Comment string `json:"comment,omitempty"`
}

// Attribute is one attribute of a composite type.
type Attribute struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// Kind returns the composite type model identity.
func (*DesiredComposite) Kind() schemaext.Kind { return CompositeKind }

// Kind returns the composite type model identity.
func (*ObservedComposite) Kind() schemaext.Kind { return CompositeKind }

// DefinedType reports the type under [objectidentity.KindComposite], naming
// the types of its attributes; see [schemaext.TypeDefiner].
func (v *DesiredComposite) DefinedType(ref objectidentity.ID) schemaext.DefinedType {
	return definedType(ref, attributeTypes(v.Attributes)...)
}

// DefinedType reports the type under [objectidentity.KindComposite], naming
// the types of its attributes; see [schemaext.TypeDefiner].
func (v *ObservedComposite) DefinedType(ref objectidentity.ID) schemaext.DefinedType {
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
func (v *DesiredComposite) Clone() schemaext.Value { return v.Copy() }

// Copy is [DesiredComposite.Clone] without the interface. A nil receiver
// returns nil.
func (v *DesiredComposite) Copy() *DesiredComposite {
	if v == nil {
		return nil
	}
	copied := *v
	copied.Attributes = slices.Clone(v.Attributes)
	return &copied
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedComposite) Clone() schemaext.Value { return v.Copy() }

// Copy is [ObservedComposite.Clone] without the interface. A nil receiver
// returns nil.
func (v *ObservedComposite) Copy() *ObservedComposite {
	if v == nil {
		return nil
	}
	copied := *v
	copied.Attributes = slices.Clone(v.Attributes)
	return &copied
}

// Equal reports whether other declares the same type in the same words, its
// attributes in the same order. Two nil declarations are equal.
func (v *DesiredComposite) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredComposite)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return slices.Equal(v.Attributes, w.Attributes) && v.Comment == w.Comment && v.StructName == w.StructName
}

// Equal reports whether other observes the same type. Two nil observations
// are equal.
func (v *ObservedComposite) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedComposite)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return slices.Equal(v.Attributes, w.Attributes) && v.Comment == w.Comment
}

// Desired is the declaration that keeps the observed type as it is.
func (v *ObservedComposite) Desired() *DesiredComposite {
	if v == nil {
		return nil
	}
	return &DesiredComposite{Attributes: slices.Clone(v.Attributes), Comment: v.Comment}
}

// Observed is the observation a declaration predicts: its attributes and its
// comment, as written.
func (v *DesiredComposite) Observed() *ObservedComposite {
	if v == nil {
		return nil
	}
	return &ObservedComposite{Attributes: slices.Clone(v.Attributes), Comment: v.Comment}
}

// ValidateDesiredComposite checks representation invariants: at least one
// attribute, each with a name and a type, no name twice, and representable
// text.
func ValidateDesiredComposite(v *DesiredComposite) error {
	if v == nil {
		return modelError(CompositeKind, schemaext.Desired, fmt.Errorf("%w: nil composite type declaration", schemaext.ErrInvalidValue))
	}
	if err := validAttributes(v.Attributes); err != nil {
		return modelError(CompositeKind, schemaext.Desired, err)
	}
	if err := validTexts("composite type", "comment", v.Comment, "struct name", v.StructName); err != nil {
		return modelError(CompositeKind, schemaext.Desired, err)
	}
	return nil
}

// ValidateObservedComposite checks the same invariants of an observation.
func ValidateObservedComposite(v *ObservedComposite) error {
	if v == nil {
		return modelError(CompositeKind, schemaext.Observed, fmt.Errorf("%w: nil composite type observation", schemaext.ErrInvalidValue))
	}
	if err := validAttributes(v.Attributes); err != nil {
		return modelError(CompositeKind, schemaext.Observed, err)
	}
	if err := validTexts("composite type", "comment", v.Comment); err != nil {
		return modelError(CompositeKind, schemaext.Observed, err)
	}
	return nil
}

func validAttributes(attributes []Attribute) error {
	if len(attributes) == 0 {
		return fmt.Errorf("%w: a composite type requires at least one attribute", schemaext.ErrInvalidValue)
	}
	seen := make(map[string]bool, len(attributes))
	for _, attribute := range attributes {
		if strings.TrimSpace(attribute.Name) == "" || strings.TrimSpace(attribute.Type) == "" {
			return fmt.Errorf("%w: a composite type attribute has a name and a type", schemaext.ErrInvalidValue)
		}
		if err := validTexts("composite type", "attribute name", attribute.Name, "attribute type", attribute.Type); err != nil {
			return err
		}
		if seen[attribute.Name] {
			return fmt.Errorf("%w: composite type attribute %q is declared twice", schemaext.ErrInvalidValue, attribute.Name)
		}
		seen[attribute.Name] = true
	}
	return nil
}
