package pgtype

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlutil"
)

// DesiredDomain declares a domain: a base type with an optional NOT NULL, an
// optional DEFAULT and an optional CHECK. The domain's schema and name are its
// object identity, never fields here.
type DesiredDomain struct {
	// BaseType is the type the domain constrains, as the declaration writes
	// it, such as "TEXT" or "numeric(10,2)".
	BaseType string `json:"base_type"`
	// NotNull writes NOT NULL.
	NotNull bool `json:"not_null,omitempty"`
	// Default is a literal default, written as a literal of BaseType.
	Default string `json:"default,omitempty"`
	// DefaultExpr is a default expression, written as it is. CREATE DOMAIN
	// holds one default, and a declaration that states both writes this one.
	DefaultExpr string `json:"default_expr,omitempty"`
	// Check is the expression of a CHECK, which names the value VALUE.
	Check string `json:"check,omitempty"`
	// Comment is the domain's comment, empty for none.
	Comment string `json:"comment,omitempty"`
	// StructName preserves the Go struct a declaration was read from. It has
	// no server counterpart and does not change the domain.
	StructName string `json:"struct_name,omitempty"`
	// Normalized is the connected server's own spelling of Check and of the
	// default, attached by a live comparison before the comparison runs. Nil
	// means no server spelled this declaration: it was not compared live, or
	// the server refused the probe. The CHECK and the default are then
	// written at creation and never compared. A source never writes it.
	Normalized *NormalizedDomain `json:"normalized,omitempty"`
}

// NormalizedDomain is a declaration's CHECK and default after the rewrite the
// catalog form went through, each empty where the declaration states none.
type NormalizedDomain struct {
	Check   string `json:"check,omitempty"`
	Default string `json:"default,omitempty"`
}

// ObservedDomain is a domain as the catalog reports it.
type ObservedDomain struct {
	// BaseType is the base type in the catalog's own spelling.
	BaseType string `json:"base_type"`
	// NotNull reports NOT NULL.
	NotNull bool `json:"not_null,omitempty"`
	// Default is the default expression the catalog stores, empty for none.
	Default string `json:"default,omitempty"`
	// Check is the domain's CHECK expressions joined with AND, empty for
	// none.
	Check string `json:"check,omitempty"`
	// Checks names each CHECK the catalog holds, beside the expression the
	// server stores for it, in catalog order. ALTER DOMAIN removes a
	// constraint by its name, which a declaration cannot supply. It is empty
	// where the domain has none and where the read could not enumerate them.
	Checks []DomainCheck `json:"checks,omitempty"`
	// Comment is the domain's comment, empty for none.
	Comment string `json:"comment,omitempty"`
}

// DomainCheck is one named CHECK of a domain, as the catalog holds it.
type DomainCheck struct {
	Name       string `json:"name"`
	Expression string `json:"expression"`
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
func (v *DesiredDomain) Copy() *DesiredDomain {
	if v == nil {
		return nil
	}
	copied := *v
	if v.Normalized != nil {
		normalized := *v.Normalized
		copied.Normalized = &normalized
	}
	return &copied
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedDomain) Clone() schemaext.Value { return v.Copy() }

// Copy is [ObservedDomain.Clone] without the interface. A nil receiver
// returns nil.
func (v *ObservedDomain) Copy() *ObservedDomain {
	if v == nil {
		return nil
	}
	copied := *v
	copied.Checks = slices.Clone(v.Checks)
	return &copied
}

// Equal reports whether other declares the same domain in the same words.
// Two nil declarations are equal.
func (v *DesiredDomain) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredDomain)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return v.BaseType == w.BaseType && v.NotNull == w.NotNull && v.Default == w.Default && v.DefaultExpr == w.DefaultExpr &&
		v.Check == w.Check && v.Comment == w.Comment && v.StructName == w.StructName &&
		equalNormalized(v.Normalized, w.Normalized)
}

// Equal reports whether other observes the same domain, its CHECK
// constraints in the same order. Two nil observations are equal.
func (v *ObservedDomain) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedDomain)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return v.BaseType == w.BaseType && v.NotNull == w.NotNull && v.Default == w.Default && v.Check == w.Check &&
		slices.Equal(v.Checks, w.Checks) && v.Comment == w.Comment
}

func equalNormalized(a, b *NormalizedDomain) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// Desired is the declaration that keeps the observed domain as it is. A
// default the catalog stores reads as an expression where it looks like one
// and as a literal otherwise, the rule a read of a column default follows.
func (v *ObservedDomain) Desired() *DesiredDomain {
	if v == nil {
		return nil
	}
	declared := &DesiredDomain{BaseType: v.BaseType, NotNull: v.NotNull, Check: v.Check, Comment: v.Comment}
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
// and its CHECK. The CHECK and the default are in the server's spelling where
// [DesiredDomain.Normalized] holds one, since that is what the catalog
// reports, and as written otherwise. It names no CHECK constraint, because
// only the server names one.
func (v *DesiredDomain) Observed() *ObservedDomain {
	if v == nil {
		return nil
	}
	defaultValue, check := v.Default, v.Check
	if v.DefaultExpr != "" {
		defaultValue = v.DefaultExpr
	}
	if v.Normalized != nil {
		defaultValue, check = cmp.Or(v.Normalized.Default, defaultValue), cmp.Or(v.Normalized.Check, check)
	}
	return &ObservedDomain{BaseType: v.BaseType, NotNull: v.NotNull, Default: defaultValue, Check: check, Comment: v.Comment}
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
	normalizedCheck, normalizedDefault := "", ""
	if v.Normalized != nil {
		normalizedCheck, normalizedDefault = v.Normalized.Check, v.Normalized.Default
	}
	if err := validTexts("domain", "base type", v.BaseType, "default", v.Default, "default expression", v.DefaultExpr,
		"check", v.Check, "comment", v.Comment, "struct name", v.StructName,
		"normalized check", normalizedCheck, "normalized default", normalizedDefault); err != nil {
		return modelError(DomainKind, schemaext.Desired, err)
	}
	return nil
}

// ValidateObservedDomain checks that an observation carries a base type, that
// every CHECK it names has a name and an expression, and that every text is
// representable.
func ValidateObservedDomain(v *ObservedDomain) error {
	if v == nil {
		return modelError(DomainKind, schemaext.Observed, fmt.Errorf("%w: nil domain observation", schemaext.ErrInvalidValue))
	}
	if strings.TrimSpace(v.BaseType) == "" {
		return modelError(DomainKind, schemaext.Observed, fmt.Errorf("%w: a domain observation requires a base type", schemaext.ErrInvalidValue))
	}
	if err := validTexts("domain", "base type", v.BaseType, "default", v.Default, "check", v.Check, "comment", v.Comment); err != nil {
		return modelError(DomainKind, schemaext.Observed, err)
	}
	for _, check := range v.Checks {
		if strings.TrimSpace(check.Name) == "" || strings.TrimSpace(check.Expression) == "" {
			return modelError(DomainKind, schemaext.Observed,
				fmt.Errorf("%w: a domain CHECK has a name and an expression", schemaext.ErrInvalidValue))
		}
		if err := validTexts("domain", "check name", check.Name, "check expression", check.Expression); err != nil {
			return modelError(DomainKind, schemaext.Observed, err)
		}
	}
	return nil
}
