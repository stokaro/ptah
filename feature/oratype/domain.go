package oratype

import (
	"fmt"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlutil"
)

// DesiredDomain declares a single-column domain: a base type with an optional
// NOT NULL, an optional DEFAULT and an optional CHECK. The domain's schema and
// name are its object identity, never fields here.
type DesiredDomain struct {
	// BaseType is the type the domain constrains, as the declaration writes
	// it, such as "VARCHAR2(255)".
	BaseType string `json:"base_type"`
	// NotNull writes NOT NULL.
	NotNull bool `json:"not_null,omitempty"`
	// Default is a literal default. It is written as it is where it already
	// reads as a SQL literal, and quoted as a string otherwise.
	Default string `json:"default,omitempty"`
	// DefaultExpr is a default expression, written as it is. A declaration
	// that states both writes this one.
	DefaultExpr string `json:"default_expr,omitempty"`
	// Check is the expression of a CHECK, which names the value VALUE.
	Check string `json:"check,omitempty"`
	// Comment is written as a SQL comment line before the domain's creation.
	// Oracle stores no comment on a domain, so a read never reports one and a
	// comparison never compares it.
	Comment string `json:"comment,omitempty"`
	// StructName preserves the Go struct a declaration was read from. It has
	// no server counterpart and does not change the domain.
	StructName string `json:"struct_name,omitempty"`
}

// ObservedDomain is a domain as the catalog reports it.
type ObservedDomain struct {
	// BaseType is the base type in the catalog's own spelling.
	BaseType string `json:"base_type"`
	// NotNull reports NOT NULL.
	NotNull bool `json:"not_null,omitempty"`
	// Default is the default the catalog stores, empty for none.
	Default string `json:"default,omitempty"`
	// Check is the domain's declared CHECK expressions joined with AND, empty
	// for none. The constraint Oracle writes for NOT NULL is not one of them.
	Check string `json:"check,omitempty"`
}

// Kind returns the domain model identity.
func (*DesiredDomain) Kind() schemaext.Kind { return DomainKind }

// Kind returns the domain model identity.
func (*ObservedDomain) Kind() schemaext.Kind { return DomainKind }

// DefinedType reports the domain under [objectidentity.KindDomain], naming its
// base type; see [schemaext.TypeDefiner].
func (v *DesiredDomain) DefinedType(ref objectidentity.ID) schemaext.DefinedType {
	return definedType(ref, v.BaseType)
}

// DefinedType reports the domain under [objectidentity.KindDomain], naming its
// base type; see [schemaext.TypeDefiner].
func (v *ObservedDomain) DefinedType(ref objectidentity.ID) schemaext.DefinedType {
	return definedType(ref, v.BaseType)
}

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (v *DesiredDomain) Clone() schemaext.Value { return v.Copy() }

// Copy is [DesiredDomain.Clone] without the interface. A nil receiver returns
// nil.
func (v *DesiredDomain) Copy() *DesiredDomain { return copyOf(v) }

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedDomain) Clone() schemaext.Value { return v.Copy() }

// Copy is [ObservedDomain.Clone] without the interface. A nil receiver returns
// nil.
func (v *ObservedDomain) Copy() *ObservedDomain { return copyOf(v) }

// Equal reports whether other declares the same domain in the same words. Two
// nil declarations are equal.
func (v *DesiredDomain) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredDomain)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return *v == *w
}

// Equal reports whether other observes the same domain. Two nil observations
// are equal.
func (v *ObservedDomain) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedDomain)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return *v == *w
}

// Desired is the declaration that keeps the observed domain as it is. A
// default the catalog stores reads as an expression where it looks like one
// and as a literal otherwise, the rule a read of a column default follows.
func (v *ObservedDomain) Desired() *DesiredDomain {
	if v == nil {
		return nil
	}
	declared := &DesiredDomain{BaseType: v.BaseType, NotNull: v.NotNull, Check: v.Check}
	if strings.TrimSpace(v.Default) != "" {
		if sqlutil.DefaultLooksLikeExpression(v.Default) {
			declared.DefaultExpr = v.Default
		} else {
			declared.Default = v.Default
		}
	}
	return declared
}

// Observed is the observation a declaration predicts: its base type, NOT
// NULL, the default it writes, which is the expression where it states one,
// and its CHECK, as written.
func (v *DesiredDomain) Observed() *ObservedDomain {
	if v == nil {
		return nil
	}
	defaultValue := v.Default
	if v.DefaultExpr != "" {
		defaultValue = v.DefaultExpr
	}
	return &ObservedDomain{BaseType: v.BaseType, NotNull: v.NotNull, Default: defaultValue, Check: v.Check}
}

// ValidateDesiredDomain checks representation invariants: a base type is
// required, and every text is representable.
func ValidateDesiredDomain(v *DesiredDomain) error {
	if v == nil {
		return modelError(DomainKind, schemaext.Desired, fmt.Errorf("%w: nil domain declaration", schemaext.ErrInvalidValue))
	}
	if strings.TrimSpace(v.BaseType) == "" {
		return modelError(DomainKind, schemaext.Desired, fmt.Errorf("%w: a domain requires a base type", schemaext.ErrInvalidValue))
	}
	if err := validTexts("domain", "base type", v.BaseType, "default", v.Default, "default expression", v.DefaultExpr,
		"check", v.Check, "comment", v.Comment, "struct name", v.StructName); err != nil {
		return modelError(DomainKind, schemaext.Desired, err)
	}
	return nil
}

// ValidateObservedDomain checks that an observation carries a base type and
// that every text is representable.
func ValidateObservedDomain(v *ObservedDomain) error {
	if v == nil {
		return modelError(DomainKind, schemaext.Observed, fmt.Errorf("%w: nil domain observation", schemaext.ErrInvalidValue))
	}
	if strings.TrimSpace(v.BaseType) == "" {
		return modelError(DomainKind, schemaext.Observed, fmt.Errorf("%w: a domain observation requires a base type", schemaext.ErrInvalidValue))
	}
	if err := validTexts("domain", "base type", v.BaseType, "default", v.Default, "check", v.Check); err != nil {
		return modelError(DomainKind, schemaext.Observed, err)
	}
	return nil
}

// copyOf copies a value that holds no reference. A nil pointer returns nil.
func copyOf[T any](v *T) *T {
	if v == nil {
		return nil
	}
	copied := *v
	return &copied
}
