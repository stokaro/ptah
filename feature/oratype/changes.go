package oratype

import (
	"encoding/json"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// The attributes a change names as different, as the comparison established
// them.
const (
	// DiffBaseType is a domain's base type, compared after both spellings are
	// canonicalized.
	DiffBaseType = "base_type"
	// DiffNotNull is a domain's NOT NULL.
	DiffNotNull = "not_null"
	// DiffAttributes is an object type's attribute list.
	DiffAttributes = "attributes"
)

// The change kinds.
const (
	// DomainChangeKind identifies the transition of one domain.
	DomainChangeKind schemaext.Kind = "ptah.run/oratype/domain-change"
	// ObjectTypeChangeKind identifies the transition of one object type.
	ObjectTypeChangeKind schemaext.Kind = "ptah.run/oratype/object-type-change"
)

// ChangeKinds are the two change kinds, in model order.
func ChangeKinds() []schemaext.Kind { return []schemaext.Kind{DomainChangeKind, ObjectTypeChangeKind} }

// DomainChange records how one domain differs. A nil Before creates it, a nil
// After drops it, and both together record a surviving domain whose Differs
// names what the comparison found different, sorted.
type DomainChange struct {
	Before  *ObservedDomain `json:"before"`
	After   *DesiredDomain  `json:"after"`
	Differs []string        `json:"differs"`
}

// ObjectTypeChange records how one object type differs, as [DomainChange]
// does.
type ObjectTypeChange struct {
	Before  *ObservedObjectType `json:"before"`
	After   *DesiredObjectType  `json:"after"`
	Differs []string            `json:"differs"`
}

// Kind returns the stable change identity.
func (*DomainChange) Kind() schemaext.Kind { return DomainChangeKind }

// Kind returns the stable change identity.
func (*ObjectTypeChange) Kind() schemaext.Kind { return ObjectTypeChangeKind }

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *DomainChange) CloneChange() schemaext.ChangeValue {
	if v == nil {
		return (*DomainChange)(nil)
	}
	return v.Copy()
}

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *ObjectTypeChange) CloneChange() schemaext.ChangeValue {
	if v == nil {
		return (*ObjectTypeChange)(nil)
	}
	return v.Copy()
}

// Copy is [DomainChange.CloneChange] without the interface: it shares no
// operand with v, and a nil receiver returns nil.
func (v *DomainChange) Copy() *DomainChange {
	if v == nil {
		return nil
	}
	return &DomainChange{Before: v.Before.Copy(), After: v.After.Copy(), Differs: slices.Clone(v.Differs)}
}

// Copy is [ObjectTypeChange.CloneChange] without the interface: it shares no
// operand with v, and a nil receiver returns nil.
func (v *ObjectTypeChange) Copy() *ObjectTypeChange {
	if v == nil {
		return nil
	}
	return &ObjectTypeChange{Before: v.Before.Copy(), After: v.After.Copy(), Differs: slices.Clone(v.Differs)}
}

// RemovesState reports a drop: a change without a declaration removes the
// domain; see [schemaext.StateRemoval].
func (v *DomainChange) RemovesState() bool { return v != nil && v.After == nil }

// RemovesState reports a drop, as [DomainChange.RemovesState] does.
func (v *ObjectTypeChange) RemovesState() bool { return v != nil && v.After == nil }

// CreatedType reports the domain a change that declares one creates, naming
// its declared base type; see [schemaext.TypeCreator]. A drop creates none.
func (v *DomainChange) CreatedType(subject objectidentity.ID) (schemaext.DefinedType, bool) {
	if v == nil || v.After == nil {
		return schemaext.DefinedType{}, false
	}
	return v.After.DefinedType(subject), true
}

// CreatedType reports the object type a change that declares one creates or
// replaces, as [DomainChange.CreatedType] does.
func (v *ObjectTypeChange) CreatedType(subject objectidentity.ID) (schemaext.DefinedType, bool) {
	if v == nil || v.After == nil {
		return schemaext.DefinedType{}, false
	}
	return v.After.DefinedType(subject), true
}

// ObservedType reports the current domain definition and its references. An
// addition has no current definition; see [schemaext.TypeObserver].
func (v *DomainChange) ObservedType(subject objectidentity.ID) (schemaext.DefinedType, bool) {
	if v == nil || v.Before == nil {
		return schemaext.DefinedType{}, false
	}
	return v.Before.DefinedType(subject), true
}

// ObservedType reports the current object type definition and its
// references, as [DomainChange.ObservedType] does.
func (v *ObjectTypeChange) ObservedType(subject objectidentity.ID) (schemaext.DefinedType, bool) {
	if v == nil || v.Before == nil {
		return schemaext.DefinedType{}, false
	}
	return v.Before.DefinedType(subject), true
}

// Differ reports whether the change names attribute as different.
func (v *DomainChange) Differ(attribute string) bool {
	return v != nil && slices.Contains(v.Differs, attribute)
}

// Differ reports whether the change names attribute as different.
func (v *ObjectTypeChange) Differ(attribute string) bool {
	return v != nil && slices.Contains(v.Differs, attribute)
}

// Validate refuses a change with neither side, operands the models refuse,
// and differences that are not a surviving domain's attributes, named once
// each in sorted order.
func (v *DomainChange) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: nil domain change", schemaext.ErrInvalidValue)
	}
	if err := validSides(v.Before == nil, v.After == nil, v.Differs, []string{DiffBaseType, DiffNotNull}, "domain"); err != nil {
		return err
	}
	if v.Before != nil {
		if err := ValidateObservedDomain(v.Before); err != nil {
			return err
		}
	}
	if v.After != nil {
		return ValidateDesiredDomain(v.After)
	}
	return nil
}

// Validate refuses what [DomainChange.Validate] refuses, for an object type's
// attributes.
func (v *ObjectTypeChange) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: nil object type change", schemaext.ErrInvalidValue)
	}
	if err := validSides(v.Before == nil, v.After == nil, v.Differs, []string{DiffAttributes}, "object type"); err != nil {
		return err
	}
	if v.Before != nil {
		if err := ValidateObservedObjectType(v.Before); err != nil {
			return err
		}
	}
	if v.After != nil {
		return ValidateDesiredObjectType(v.After)
	}
	return nil
}

// validSides refuses a change with neither side, differences beside a
// creation or a drop, a surviving type's change that names no difference, and
// a difference outside allowed or named twice or out of order.
func validSides(noBefore, noAfter bool, differs, allowed []string, label string) error {
	switch {
	case noBefore && noAfter:
		return fmt.Errorf("%w: the %s change has no current or desired side", schemaext.ErrInvalidValue, label)
	case (noBefore || noAfter) && len(differs) > 0:
		return fmt.Errorf("%w: the %s change of a creation or a drop names no difference", schemaext.ErrInvalidValue, label)
	case !noBefore && !noAfter && len(differs) == 0:
		return fmt.Errorf("%w: the %s change of a surviving type names what differs", schemaext.ErrInvalidValue, label)
	}
	for _, attribute := range differs {
		if !slices.Contains(allowed, attribute) {
			return fmt.Errorf("%w: %q is not a difference the %s change names", schemaext.ErrInvalidValue, attribute, label)
		}
	}
	if !slices.IsSorted(differs) || len(slices.Compact(slices.Clone(differs))) != len(differs) {
		return fmt.Errorf("%w: the %s change names each difference once, sorted", schemaext.ErrInvalidValue, label)
	}
	return nil
}

// changeShape is a change wire: every key present, each may be null.
func changeShape(name string) schemaext.ObjectShape {
	return schemaext.ObjectShape{Name: name, Allowed: []string{"before", "after", "differs"},
		Required: []string{"before", "after", "differs"}, Nullable: []string{"before", "after", "differs"}}
}

// ChangeCodecs returns the version-one codecs of the two changes, in model
// order. A change's operands are encoded by their models' rules and its
// differences as given; an empty list is written null. Each call returns
// independent definitions.
func ChangeCodecs() []schemaext.Codec {
	differs := `"differs":{"anyOf":[{"type":"null"},{"type":"array","minItems":1,"items":` + textDefinition + `}]}`
	return []schemaext.Codec{
		schemaext.ModelCodec[*DomainChange]{
			Prototype: &DomainChange{}, Representation: schemaext.Change, Version: 1,
			Definition: changeDefinition(observedDomainDefinition, desiredDomainDefinition, differs),
			Shape:      changeWire(changeShape("domain change"), objectWire(observedDomainShape), objectWire(desiredDomainShape)),
			Validate:   (*DomainChange).Validate, Clone: (*DomainChange).Copy,
			Canonical: func(value *DomainChange) *DomainChange {
				canonical := value.Copy()
				canonical.Differs = nilWhenEmpty(canonical.Differs)
				return canonical
			},
		}.Codec(),
		schemaext.ModelCodec[*ObjectTypeChange]{
			Prototype: &ObjectTypeChange{}, Representation: schemaext.Change, Version: 1,
			Definition: changeDefinition(observedObjectTypeDefinition, desiredObjectTypeDefinition, differs),
			Shape: changeWire(changeShape("object type change"), attributesWire(observedObjectTypeShape),
				attributesWire(desiredObjectTypeShape)),
			Validate: (*ObjectTypeChange).Validate, Clone: (*ObjectTypeChange).Copy,
			Canonical: func(value *ObjectTypeChange) *ObjectTypeChange {
				canonical := value.Copy()
				canonical.Differs = nilWhenEmpty(canonical.Differs)
				return canonical
			},
		}.Codec(),
	}
}

// changeDefinition is a change's schema: nullable operands of the observed and
// desired definitions, beside the change's own properties.
func changeDefinition(observed, desired, properties string) json.RawMessage {
	return json.RawMessage(`{"type":"object","additionalProperties":false,"properties":{` +
		`"before":{"anyOf":[{"type":"null"},` + observed + `]},"after":{"anyOf":[{"type":"null"},` + desired + `]},` +
		properties + `}}`)
}

// changeWire checks a change's keys and each operand's wire.
func changeWire(shape schemaext.ObjectShape, before, after func(json.RawMessage) error) func(json.RawMessage) error {
	return func(data json.RawMessage) error {
		fields, err := schemaext.DecodeObject(data, shape)
		if err != nil {
			return err
		}
		if err := noEmptyLists(shape, fields); err != nil {
			return err
		}
		if operand := nonNull(fields["before"]); operand != nil {
			if err := before(operand); err != nil {
				return err
			}
		}
		if operand := nonNull(fields["after"]); operand != nil {
			return after(operand)
		}
		return nil
	}
}

// noEmptyLists refuses an empty list under a key of shape that may hold null:
// the encoder writes a list it does not hold as null, so accepting [] would
// give one value two encodings.
func noEmptyLists(shape schemaext.ObjectShape, fields map[string]json.RawMessage) error {
	for _, key := range shape.Nullable {
		if value, found := fields[key]; found && string(value) == "[]" {
			return fmt.Errorf("%w: %s %s is written null when it holds nothing", schemaext.ErrInvalidValue, shape.Name, key)
		}
	}
	return nil
}

// nonNull returns data, or nil where it is absent or JSON null.
func nonNull(data json.RawMessage) json.RawMessage {
	if data == nil || string(data) == "null" {
		return nil
	}
	return data
}

// nilWhenEmpty is values, or nil where it holds nothing.
func nilWhenEmpty[T any](values []T) []T {
	if len(values) == 0 {
		return nil
	}
	return values
}
