package pgroutine

import (
	"encoding/json"
	"fmt"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// PropertiesKind identifies the PostgreSQL function properties LEAKPROOF,
// STRICT and PARALLEL. They belong to the function, including its argument
// signature, and have no meaning on a procedure or a different engine.
const PropertiesKind schemaext.Kind = "ptah.run/postgres/routine-properties"

// PropertiesOwner identifies the provider of PostgreSQL function properties.
const PropertiesOwner = "ptah.run/postgres/routine-properties"

// Properties describes how PostgreSQL may evaluate a function. The zero value
// is NOT LEAKPROOF, CALLED ON NULL INPUT and PARALLEL UNSAFE.
type Properties struct {
	// Leakproof permits evaluation before security-barrier conditions. Only a
	// superuser may grant this property; an incorrect declaration can expose data.
	Leakproof bool `json:"leakproof,omitempty"`
	// Strict returns NULL without evaluating the body when any argument is NULL.
	Strict bool `json:"strict,omitempty"`
	// Parallel is SAFE or RESTRICTED. Empty represents the server default UNSAFE.
	Parallel string `json:"parallel,omitempty"`
}

// Normalize returns an independent value with an uppercase parallel level and
// UNSAFE represented as empty. An unknown level wraps schemaext.ErrInvalidValue.
func (p Properties) Normalize() (Properties, error) {
	p.Parallel = strings.ToUpper(strings.TrimSpace(p.Parallel))
	switch p.Parallel {
	case "UNSAFE":
		p.Parallel = ""
	case "", "SAFE", "RESTRICTED":
	default:
		return Properties{}, fmt.Errorf("%w: parallel must be SAFE, RESTRICTED or UNSAFE, not %q", schemaext.ErrInvalidValue, p.Parallel)
	}
	return p, nil
}

// IsZero reports whether p states only server defaults. Normalize external
// input before calling it.
func (p Properties) IsZero() bool { return p == Properties{} }

// Clauses returns the nondefault SQL clauses in deterministic order. Callers
// validate the value before rendering it.
func (p Properties) Clauses() []string {
	var clauses []string
	if p.Leakproof {
		clauses = append(clauses, "LEAKPROOF")
	}
	if p.Strict {
		clauses = append(clauses, "STRICT")
	}
	if p.Parallel != "" {
		clauses = append(clauses, "PARALLEL "+p.Parallel)
	}
	return clauses
}

// DesiredProperties is a declaration of nondefault PostgreSQL function properties.
type DesiredProperties Properties

// ObservedProperties is a catalog observation of nondefault PostgreSQL function properties.
type ObservedProperties Properties

// Kind returns the owned facet identity.
func (*DesiredProperties) Kind() schemaext.Kind { return PropertiesKind }

// Kind returns the owned facet identity.
func (*ObservedProperties) Kind() schemaext.Kind { return PropertiesKind }

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (p *DesiredProperties) Clone() schemaext.Value {
	if p == nil {
		return (*DesiredProperties)(nil)
	}
	return new(*p)
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (p *ObservedProperties) Clone() schemaext.Value {
	if p == nil {
		return (*ObservedProperties)(nil)
	}
	return new(*p)
}

// Equal compares declarations. Different representations are unequal.
func (p *DesiredProperties) Equal(other schemaext.Value) bool {
	q, ok := other.(*DesiredProperties)
	if !ok || p == nil || q == nil {
		return ok && p == nil && q == nil
	}
	return *p == *q
}

// Equal compares observations. Different representations are unequal.
func (p *ObservedProperties) Equal(other schemaext.Value) bool {
	q, ok := other.(*ObservedProperties)
	if !ok || p == nil || q == nil {
		return ok && p == nil && q == nil
	}
	return *p == *q
}

// ValidateDesiredProperties refuses nil, default-only and nonnormalized values.
// Its errors wrap schemaext.ErrInvalidValue as schemaext.InvalidModelError.
func ValidateDesiredProperties(p *DesiredProperties) error {
	if p == nil {
		return propertiesError(schemaext.Desired, "nil declaration")
	}
	return validProperties(Properties(*p), schemaext.Desired)
}

// ValidateObservedProperties applies the declaration's rules to an observation.
func ValidateObservedProperties(p *ObservedProperties) error {
	if p == nil {
		return propertiesError(schemaext.Observed, "nil observation")
	}
	return validProperties(Properties(*p), schemaext.Observed)
}
func validProperties(p Properties, representation schemaext.Representation) error {
	normalized, err := p.Normalize()
	if err != nil {
		return propertiesError(representation, err.Error())
	}
	if normalized != p {
		return propertiesError(representation, "parallel must be normalized")
	}
	if p.IsZero() {
		return propertiesError(representation, "default function properties carry no facet")
	}
	return nil
}
func propertiesError(representation schemaext.Representation, reason string) error {
	return &schemaext.InvalidModelError{Kind: PropertiesKind, Representation: representation, Message: reason}
}

// WithProperties adds normalized declared properties to facets. Server defaults
// add nothing. A duplicate kind and invalid values are refused without mutation.
func WithProperties(facets schemaext.Facets, properties Properties) (schemaext.Facets, error) {
	normalized, err := properties.Normalize()
	if err != nil {
		return schemaext.Facets{}, err
	}
	if normalized.IsZero() {
		return facets, nil
	}
	return facets.With(new(DesiredProperties(normalized)))
}

// WithObservedProperties adds normalized catalog properties under the same rules
// as WithProperties, in the observed representation.
func WithObservedProperties(facets schemaext.Facets, properties Properties) (schemaext.Facets, error) {
	normalized, err := properties.Normalize()
	if err != nil {
		return schemaext.Facets{}, err
	}
	if normalized.IsZero() {
		return facets, nil
	}
	return facets.With(new(ObservedProperties(normalized)))
}

// PropertiesOf reads either representation. Missing properties return zero and
// false. An invalid or unexpected value wraps schemaext.ErrInvalidValue.
func PropertiesOf(facets schemaext.Facets) (Properties, bool, error) {
	value, found, err := facets.Get(PropertiesKind)
	if err != nil || !found {
		return Properties{}, false, err
	}
	switch p := value.(type) {
	case *DesiredProperties:
		if err := ValidateDesiredProperties(p); err != nil {
			return Properties{}, true, err
		}
		return Properties(*p), true, nil
	case *ObservedProperties:
		if err := ValidateObservedProperties(p); err != nil {
			return Properties{}, true, err
		}
		return Properties(*p), true, nil
	default:
		return Properties{}, true, fmt.Errorf("%w: function properties have unexpected type %T", schemaext.ErrInvalidValue, value)
	}
}

const propertiesDefinition = `{"type":"object","minProperties":1,"additionalProperties":false,"properties":{"leakproof":{"const":true},"strict":{"const":true},"parallel":{"enum":["SAFE","RESTRICTED"]}}}`

// PropertiesCodecs returns independent version-one declaration and observation
// codecs. Unknown fields, explicit defaults and invalid values are refused.
func PropertiesCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredProperties]{Prototype: &DesiredProperties{}, Representation: schemaext.Desired, Version: 1,
			Definition: json.RawMessage(propertiesDefinition), Shape: propertiesShape, Validate: ValidateDesiredProperties}.Codec(),
		schemaext.ModelCodec[*ObservedProperties]{Prototype: &ObservedProperties{}, Representation: schemaext.Observed, Version: 1,
			Definition: json.RawMessage(propertiesDefinition), Shape: propertiesShape, Validate: ValidateObservedProperties}.Codec(),
	}
}
func propertiesShape(data json.RawMessage) error {
	fields, err := schemaext.DecodeObject(data, schemaext.ObjectShape{Name: "function properties", Allowed: []string{"leakproof", "strict", "parallel"}})
	if err != nil {
		return err
	}
	for _, name := range []string{"leakproof", "strict"} {
		if value, found := fields[name]; found && string(value) != "true" {
			return fmt.Errorf("%w: %s is true or omitted", schemaext.ErrInvalidValue, name)
		}
	}
	if value, found := fields["parallel"]; found && string(value) != `"SAFE"` && string(value) != `"RESTRICTED"` {
		return fmt.Errorf("%w: parallel is SAFE, RESTRICTED or omitted", schemaext.ErrInvalidValue)
	}
	return nil
}

var propertiesCoverage = schemaext.OwnedCoverageSource(PropertiesOwner, PropertiesCodecs)

// PropertiesCoverage records what a source knows of function properties.
// Complete knowledge makes a function with no facet mean server defaults.
// Subject claims preserve limits for individual overloads.
func PropertiesCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	return propertiesCoverage(PropertiesKind, representation, knowledge, subjects)
}

// HoldsProperties reports whether subject is a named function.
func HoldsProperties(subject objectidentity.ID) bool {
	return subject.Kind == objectidentity.KindFunction && !subject.Name.Empty()
}
