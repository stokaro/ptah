package pgtype

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// The optional attributes of a range type, as a declaration and a change name
// them.
const (
	RangeSubtypeOpClass = "subtype_opclass"
	RangeCollation      = "collation"
	RangeCanonical      = "canonical"
	RangeSubtypeDiff    = "subtype_diff"
)

// RangeAttributes are the optional attributes of a range type, in the order
// CREATE TYPE ... AS RANGE writes them.
func RangeAttributes() []string {
	return []string{RangeSubtypeOpClass, RangeCollation, RangeCanonical, RangeSubtypeDiff}
}

// DesiredRange declares a range type over a subtype. The type's schema and
// name are its object identity, never fields here.
type DesiredRange struct {
	// Subtype is the element type the range is over.
	Subtype string `json:"subtype"`
	// SubtypeOpClass, Collation, Canonical and SubtypeDiff are the optional
	// attributes, each as written and empty where the declaration says
	// nothing about it.
	SubtypeOpClass string `json:"subtype_opclass,omitempty"`
	Collation      string `json:"collation,omitempty"`
	Canonical      string `json:"canonical,omitempty"`
	SubtypeDiff    string `json:"subtype_diff,omitempty"`
	// Cleared names the optional attributes the declaration writes as empty,
	// which is how it asks for none of them. An omitted attribute says
	// nothing, so adopting a database whose range has a SUBTYPE_DIFF does not
	// plan its removal; a cleared one says the range has none
	// (stokaro/ptah#2223). Only a source that can spell an empty value fills
	// it, and an attribute it names is empty above.
	Cleared []string `json:"cleared,omitempty"`
	// Comment is the type's comment, empty for none.
	Comment string `json:"comment,omitempty"`
	// StructName preserves the Go struct a declaration was read from. It has
	// no server counterpart and does not change the type.
	StructName string `json:"struct_name,omitempty"`
}

// ObservedRange is a range type as the catalog reports it.
type ObservedRange struct {
	// Subtype is the element type in the catalog's own spelling.
	Subtype string `json:"subtype"`
	// SubtypeOpClass is the operator class backing the subtype's ordering.
	// The catalog always reports one, including where the declaration named
	// none.
	SubtypeOpClass string `json:"subtype_opclass,omitempty"`
	// Collation, Canonical and SubtypeDiff are empty where the range has none.
	Collation   string `json:"collation,omitempty"`
	Canonical   string `json:"canonical,omitempty"`
	SubtypeDiff string `json:"subtype_diff,omitempty"`
	// Comment is the type's comment, empty for none.
	Comment string `json:"comment,omitempty"`
}

// Kind returns the range type model identity.
func (*DesiredRange) Kind() schemaext.Kind { return RangeKind }

// Kind returns the range type model identity.
func (*ObservedRange) Kind() schemaext.Kind { return RangeKind }

// DefinedType reports the type under [objectidentity.KindRange], naming its
// subtype; see [schemaext.TypeDefiner].
func (v *DesiredRange) DefinedType(ref objectidentity.ID) schemaext.DefinedType {
	return definedType(ref, v.Subtype)
}

// DefinedType reports the type under [objectidentity.KindRange], naming its
// subtype; see [schemaext.TypeDefiner].
func (v *ObservedRange) DefinedType(ref objectidentity.ID) schemaext.DefinedType {
	return definedType(ref, v.Subtype)
}

// Attribute returns the value of the optional attribute name, and empty for
// any other name.
func (v *DesiredRange) Attribute(name string) string {
	return rangeAttribute(name, v.SubtypeOpClass, v.Collation, v.Canonical, v.SubtypeDiff)
}

// Attribute returns the value of the optional attribute name, and empty for
// any other name.
func (v *ObservedRange) Attribute(name string) string {
	return rangeAttribute(name, v.SubtypeOpClass, v.Collation, v.Canonical, v.SubtypeDiff)
}

func rangeAttribute(name, opClass, collation, canonical, subtypeDiff string) string {
	switch name {
	case RangeSubtypeOpClass:
		return opClass
	case RangeCollation:
		return collation
	case RangeCanonical:
		return canonical
	case RangeSubtypeDiff:
		return subtypeDiff
	default:
		return ""
	}
}

// Clears reports whether the declaration asks for the attribute name to be
// absent, as opposed to saying nothing about it.
func (v *DesiredRange) Clears(name string) bool { return slices.Contains(v.Cleared, name) }

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (v *DesiredRange) Clone() schemaext.Value { return v.Copy() }

// Copy is [DesiredRange.Clone] without the interface. A nil receiver returns
// nil.
func (v *DesiredRange) Copy() *DesiredRange {
	if v == nil {
		return nil
	}
	copied := *v
	copied.Cleared = slices.Clone(v.Cleared)
	return &copied
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedRange) Clone() schemaext.Value { return v.Copy() }

// Copy is [ObservedRange.Clone] without the interface. A nil receiver returns
// nil.
func (v *ObservedRange) Copy() *ObservedRange {
	if v == nil {
		return nil
	}
	copied := *v
	return &copied
}

// Equal reports whether other declares the same range type in the same words,
// clearing the same attributes. Two nil declarations are equal.
func (v *DesiredRange) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredRange)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return v.Subtype == w.Subtype && v.SubtypeOpClass == w.SubtypeOpClass && v.Collation == w.Collation &&
		v.Canonical == w.Canonical && v.SubtypeDiff == w.SubtypeDiff && slices.Equal(sortedNames(v.Cleared), sortedNames(w.Cleared)) &&
		v.Comment == w.Comment && v.StructName == w.StructName
}

// Equal reports whether other observes the same range type. Two nil
// observations are equal.
func (v *ObservedRange) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedRange)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return *v == *w
}

func sortedNames(names []string) []string {
	sorted := slices.Clone(names)
	slices.Sort(sorted)
	return slices.Compact(sorted)
}

// Desired is the declaration that keeps the observed range type as it is,
// every attribute the catalog reports stated.
func (v *ObservedRange) Desired() *DesiredRange {
	if v == nil {
		return nil
	}
	return &DesiredRange{Subtype: v.Subtype, SubtypeOpClass: v.SubtypeOpClass, Collation: v.Collation, Canonical: v.Canonical,
		SubtypeDiff: v.SubtypeDiff, Comment: v.Comment}
}

// Observed is the observation a declaration predicts: the attributes it
// writes, as written.
func (v *DesiredRange) Observed() *ObservedRange {
	if v == nil {
		return nil
	}
	return &ObservedRange{Subtype: v.Subtype, SubtypeOpClass: v.SubtypeOpClass, Collation: v.Collation, Canonical: v.Canonical,
		SubtypeDiff: v.SubtypeDiff, Comment: v.Comment}
}

// ValidateDesiredRange checks representation invariants: a subtype is
// required, a cleared attribute is one of the four and is empty, and every
// text is representable.
func ValidateDesiredRange(v *DesiredRange) error {
	if v == nil {
		return modelError(RangeKind, schemaext.Desired, fmt.Errorf("%w: nil range type declaration", schemaext.ErrInvalidValue))
	}
	if strings.TrimSpace(v.Subtype) == "" {
		return modelError(RangeKind, schemaext.Desired, fmt.Errorf("%w: a range type requires a subtype", schemaext.ErrInvalidValue))
	}
	for _, name := range v.Cleared {
		if !slices.Contains(RangeAttributes(), name) {
			return modelError(RangeKind, schemaext.Desired, fmt.Errorf("%w: %q is not a range type attribute a declaration can clear", schemaext.ErrInvalidValue, name))
		}
		if v.Attribute(name) != "" {
			return modelError(RangeKind, schemaext.Desired, fmt.Errorf("%w: range type attribute %s is both stated and cleared", schemaext.ErrInvalidValue, name))
		}
	}
	if err := validTexts("range type", "subtype", v.Subtype, RangeSubtypeOpClass, v.SubtypeOpClass, RangeCollation, v.Collation,
		RangeCanonical, v.Canonical, RangeSubtypeDiff, v.SubtypeDiff, "comment", v.Comment, "struct name", v.StructName); err != nil {
		return modelError(RangeKind, schemaext.Desired, err)
	}
	return nil
}

// ValidateObservedRange checks that an observation carries a subtype and
// representable text.
func ValidateObservedRange(v *ObservedRange) error {
	if v == nil {
		return modelError(RangeKind, schemaext.Observed, fmt.Errorf("%w: nil range type observation", schemaext.ErrInvalidValue))
	}
	if strings.TrimSpace(v.Subtype) == "" {
		return modelError(RangeKind, schemaext.Observed, fmt.Errorf("%w: a range type observation requires a subtype", schemaext.ErrInvalidValue))
	}
	if err := validTexts("range type", "subtype", v.Subtype, RangeSubtypeOpClass, v.SubtypeOpClass, RangeCollation, v.Collation,
		RangeCanonical, v.Canonical, RangeSubtypeDiff, v.SubtypeDiff, "comment", v.Comment); err != nil {
		return modelError(RangeKind, schemaext.Observed, err)
	}
	return nil
}

// ComposeDeclaration combines identical range declarations from independent
// sources. Source struct names do not change the type. Different definitions
// wrap schemaext.ErrDuplicate; neither operand is mutated.
func (v *DesiredRange) ComposeDeclaration(other schemaext.Value) (schemaext.Value, error) {
	if err := ValidateDesiredRange(v); err != nil {
		return nil, err
	}
	right, ok := other.(*DesiredRange)
	if !ok {
		return nil, fmt.Errorf("%w: incompatible range declaration", schemaext.ErrInvalidValue)
	}
	if err := ValidateDesiredRange(right); err != nil {
		return nil, err
	}
	compared := right.Copy()
	compared.StructName = v.StructName
	if !v.Equal(compared) {
		return nil, fmt.Errorf("%w: conflicting range definitions", schemaext.ErrDuplicate)
	}
	return v.Copy(), nil
}
