package pgtype

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

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
	// DiffCheck is a domain's CHECK, compared in the server's spelling only.
	DiffCheck = "check"
	// DiffDefault is a domain's default, compared in the server's spelling
	// only.
	DiffDefault = "default"
	// DiffAttributes is a composite type's attribute list.
	DiffAttributes = "attributes"
	// DiffSubtype is a range type's subtype. A range type's optional
	// attributes are named by [RangeAttributes].
	DiffSubtype = "subtype"
	// DiffComment is a type's comment.
	DiffComment = "comment"
)

// The change kinds.
const (
	// DomainChangeKind identifies the transition of one domain.
	DomainChangeKind schemaext.Kind = "ptah.run/pgtype/domain-change"
	// CompositeChangeKind identifies the transition of one composite type.
	CompositeChangeKind schemaext.Kind = "ptah.run/pgtype/composite-change"
	// RangeChangeKind identifies the transition of one range type.
	RangeChangeKind schemaext.Kind = "ptah.run/pgtype/range-change"
)

// ChangeKinds are the three change kinds, in model order.
func ChangeKinds() []schemaext.Kind {
	return []schemaext.Kind{DomainChangeKind, CompositeChangeKind, RangeChangeKind}
}

// differences each kind's change may name.
var (
	domainDifferences    = []string{DiffBaseType, DiffNotNull, DiffCheck, DiffDefault, DiffComment}
	compositeDifferences = []string{DiffAttributes, DiffComment}
	rangeDifferences     = append([]string{DiffSubtype}, append(RangeAttributes(), DiffComment)...)
)

// DomainChange records how one domain differs. A nil Before creates it, a nil
// After drops it, and both together change a surviving one, whose Differs
// names what the comparison found different, sorted.
type DomainChange struct {
	Before  *ObservedDomain `json:"before"`
	After   *DesiredDomain  `json:"after"`
	Differs []string        `json:"differs"`
}

// CompositeChange records how one composite type differs, as [DomainChange]
// does. Added and Removed are the attribute delta ALTER TYPE reaches the
// declared attributes with: set only where dropping Removed and then
// appending Added lands exactly on the declared order and no attribute keeps
// its name and changes its type. A changed attribute list without them is
// reconciled by dropping the type and creating it again.
type CompositeChange struct {
	Before  *ObservedComposite `json:"before"`
	After   *DesiredComposite  `json:"after"`
	Differs []string           `json:"differs"`
	Added   []Attribute        `json:"added"`
	Removed []string           `json:"removed"`
}

// RangeChange records how one range type differs, as [DomainChange] does.
// PostgreSQL has no ALTER TYPE ... AS RANGE, so every difference but the
// comment is a drop and a creation.
type RangeChange struct {
	Before  *ObservedRange `json:"before"`
	After   *DesiredRange  `json:"after"`
	Differs []string       `json:"differs"`
}

// Kind returns the stable change identity.
func (*DomainChange) Kind() schemaext.Kind { return DomainChangeKind }

// Kind returns the stable change identity.
func (*CompositeChange) Kind() schemaext.Kind { return CompositeChangeKind }

// Kind returns the stable change identity.
func (*RangeChange) Kind() schemaext.Kind { return RangeChangeKind }

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *DomainChange) CloneChange() schemaext.ChangeValue {
	if v == nil {
		return (*DomainChange)(nil)
	}
	return v.Copy()
}

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *CompositeChange) CloneChange() schemaext.ChangeValue {
	if v == nil {
		return (*CompositeChange)(nil)
	}
	return v.Copy()
}

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *RangeChange) CloneChange() schemaext.ChangeValue {
	if v == nil {
		return (*RangeChange)(nil)
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

// Copy is [CompositeChange.CloneChange] without the interface: it shares no
// operand with v, and a nil receiver returns nil.
func (v *CompositeChange) Copy() *CompositeChange {
	if v == nil {
		return nil
	}
	return &CompositeChange{Before: v.Before.Copy(), After: v.After.Copy(), Differs: slices.Clone(v.Differs),
		Added: slices.Clone(v.Added), Removed: slices.Clone(v.Removed)}
}

// Copy is [RangeChange.CloneChange] without the interface: it shares no
// operand with v, and a nil receiver returns nil.
func (v *RangeChange) Copy() *RangeChange {
	if v == nil {
		return nil
	}
	return &RangeChange{Before: v.Before.Copy(), After: v.After.Copy(), Differs: slices.Clone(v.Differs)}
}

// RemovesState reports a drop: a change without a declaration removes the
// domain. A surviving domain's change keeps it; see [schemaext.StateRemoval].
func (v *DomainChange) RemovesState() bool { return v != nil && v.After == nil }

// RemovesState reports a drop, as [DomainChange.RemovesState] does.
func (v *CompositeChange) RemovesState() bool { return v != nil && v.After == nil }

// RemovesState reports a drop, as [DomainChange.RemovesState] does.
func (v *RangeChange) RemovesState() bool { return v != nil && v.After == nil }

// CreatedType reports the domain a change that declares one creates, rebuilds
// or changes in place, naming its declared base type; see
// [schemaext.TypeCreator]. A drop creates none.
func (v *DomainChange) CreatedType(subject objectidentity.ID) (schemaext.DefinedType, bool) {
	if v == nil || v.After == nil {
		return schemaext.DefinedType{}, false
	}
	return v.After.DefinedType(subject), true
}

// CreatedType reports the composite type a change that declares one creates,
// rebuilds or changes in place, as [DomainChange.CreatedType] does.
func (v *CompositeChange) CreatedType(subject objectidentity.ID) (schemaext.DefinedType, bool) {
	if v == nil || v.After == nil {
		return schemaext.DefinedType{}, false
	}
	return v.After.DefinedType(subject), true
}

// CreatedType reports the range type a change that declares one creates or
// rebuilds, as [DomainChange.CreatedType] does.
func (v *RangeChange) CreatedType(subject objectidentity.ID) (schemaext.DefinedType, bool) {
	if v == nil || v.After == nil {
		return schemaext.DefinedType{}, false
	}
	return v.After.DefinedType(subject), true
}

// Differ reports whether the change names attribute as different.
func (v *DomainChange) Differ(attribute string) bool {
	return v != nil && slices.Contains(v.Differs, attribute)
}

// Differ reports whether the change names attribute as different.
func (v *CompositeChange) Differ(attribute string) bool {
	return v != nil && slices.Contains(v.Differs, attribute)
}

// Differ reports whether the change names attribute as different.
func (v *RangeChange) Differ(attribute string) bool {
	return v != nil && slices.Contains(v.Differs, attribute)
}

// Validate refuses a change with neither side, operands the models refuse,
// and differences that are not a surviving domain's attributes, named once
// each in sorted order.
func (v *DomainChange) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: nil domain change", schemaext.ErrInvalidValue)
	}
	if err := validSides(v.Before == nil, v.After == nil, v.Differs, domainDifferences, "domain"); err != nil {
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

// Validate refuses what [DomainChange.Validate] refuses, and an attribute
// delta beside anything but a changed attribute list of a surviving type: an
// added attribute needs a name and a type, a removed one a name, and no name
// twice in either.
func (v *CompositeChange) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: nil composite type change", schemaext.ErrInvalidValue)
	}
	if err := validSides(v.Before == nil, v.After == nil, v.Differs, compositeDifferences, "composite type"); err != nil {
		return err
	}
	if (len(v.Added) > 0 || len(v.Removed) > 0) && !v.Differ(DiffAttributes) {
		return fmt.Errorf("%w: a composite type's attribute delta belongs to a changed attribute list", schemaext.ErrInvalidValue)
	}
	if len(v.Added) > 0 {
		if err := validAttributes(v.Added); err != nil {
			return err
		}
	}
	if err := validNames("composite type removed attribute", v.Removed); err != nil {
		return err
	}
	if v.Before != nil {
		if err := ValidateObservedComposite(v.Before); err != nil {
			return err
		}
	}
	if v.After != nil {
		return ValidateDesiredComposite(v.After)
	}
	return nil
}

// Validate refuses what [DomainChange.Validate] refuses, for a range type's
// attributes.
func (v *RangeChange) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: nil range type change", schemaext.ErrInvalidValue)
	}
	if err := validSides(v.Before == nil, v.After == nil, v.Differs, rangeDifferences, "range type"); err != nil {
		return err
	}
	if v.Before != nil {
		if err := ValidateObservedRange(v.Before); err != nil {
			return err
		}
	}
	if v.After != nil {
		return ValidateDesiredRange(v.After)
	}
	return nil
}

// validSides refuses a change with neither side, differences beside a
// creation or a drop, a surviving type's change that names no difference, and
// a difference outside allowed or named twice or out of order.
func validSides(noBefore, noAfter bool, differs, allowed []string, label string) error {
	switch {
	case noBefore && noAfter:
		return fmt.Errorf("%w: a %s change has a current or a desired side", schemaext.ErrInvalidValue, label)
	case (noBefore || noAfter) && len(differs) > 0:
		return fmt.Errorf("%w: a %s that is created or dropped names no difference", schemaext.ErrInvalidValue, label)
	case !noBefore && !noAfter && len(differs) == 0:
		return fmt.Errorf("%w: a surviving %s's change names what differs", schemaext.ErrInvalidValue, label)
	}
	for _, attribute := range differs {
		if !slices.Contains(allowed, attribute) {
			return fmt.Errorf("%w: %q is not a difference of a %s", schemaext.ErrInvalidValue, attribute, label)
		}
	}
	if !slices.IsSorted(differs) || len(slices.Compact(slices.Clone(differs))) != len(differs) {
		return fmt.Errorf("%w: a %s change names each difference once, sorted", schemaext.ErrInvalidValue, label)
	}
	return nil
}

func validNames(field string, names []string) error {
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("%w: a %s has a name", schemaext.ErrInvalidValue, field)
		}
		if err := schemaext.ValidText(field, name); err != nil {
			return err
		}
		if seen[name] {
			return fmt.Errorf("%w: %s %q is named twice", schemaext.ErrInvalidValue, field, name)
		}
		seen[name] = true
	}
	return nil
}

// The change wires. Every key is present, operands may be null.
var (
	domainChangeShape = schemaext.ObjectShape{Name: "domain change",
		Allowed: []string{"before", "after", "differs"}, Required: []string{"before", "after", "differs"},
		Nullable: []string{"before", "after", "differs"}}
	compositeChangeShape = schemaext.ObjectShape{Name: "composite type change",
		Allowed:  []string{"before", "after", "differs", "added", "removed"},
		Required: []string{"before", "after", "differs", "added", "removed"},
		Nullable: []string{"before", "after", "differs", "added", "removed"}}
	rangeChangeShape = schemaext.ObjectShape{Name: "range type change",
		Allowed: []string{"before", "after", "differs"}, Required: []string{"before", "after", "differs"},
		Nullable: []string{"before", "after", "differs"}}
)

// ChangeCodecs returns the version-one codecs of the three changes, in model
// order. A change's operands are encoded by their models' rules, and its
// differences and its attribute delta are encoded as given; a list the change
// does not hold is written null. Each call returns independent definitions.
func ChangeCodecs() []schemaext.Codec {
	models := Codecs()
	stringList := `{"anyOf":[{"type":"null"},{"type":"array","minItems":1,"items":{"type":"string","minLength":1}}]}`
	return []schemaext.Codec{
		schemaext.ModelCodec[*DomainChange]{
			Prototype: &DomainChange{}, Representation: schemaext.Change, Version: 1,
			Definition: changeDefinition(models[1].Definition, models[0].Definition, `"differs":`+stringList),
			Shape:      changeWire(domainChangeShape, observedDomainWire, desiredDomainWire, nil),
			Validate:   (*DomainChange).Validate, Clone: (*DomainChange).Copy, Canonical: canonicalDomainChange,
		}.Codec(),
		schemaext.ModelCodec[*CompositeChange]{
			Prototype: &CompositeChange{}, Representation: schemaext.Change, Version: 1,
			Definition: changeDefinition(models[3].Definition, models[2].Definition,
				`"differs":`+stringList+`,"added":{"anyOf":[{"type":"null"},`+attributesDefinition+`]},"removed":`+stringList),
			Shape: changeWire(compositeChangeShape, compositeWire(observedCompositeShape), compositeWire(desiredCompositeShape),
				func(fields map[string]json.RawMessage) error {
					return eachObject(nonNull(fields["added"]), attributeShape)
				}),
			Validate: (*CompositeChange).Validate, Clone: (*CompositeChange).Copy, Canonical: canonicalCompositeChange,
		}.Codec(),
		schemaext.ModelCodec[*RangeChange]{
			Prototype: &RangeChange{}, Representation: schemaext.Change, Version: 1,
			Definition: changeDefinition(models[5].Definition, models[4].Definition, `"differs":`+stringList),
			Shape:      changeWire(rangeChangeShape, objectWire(observedRangeShape), objectWire(desiredRangeShape), nil),
			Validate:   (*RangeChange).Validate, Clone: (*RangeChange).Copy, Canonical: canonicalRangeChange,
		}.Codec(),
	}
}

// canonicalDomainChange writes an empty list as null, the one spelling the
// decoder accepts, and its operands in their models' canonical forms.
func canonicalDomainChange(value *DomainChange) *DomainChange {
	canonical := value.Copy()
	canonical.Differs = nilWhenEmpty(canonical.Differs)
	return canonical
}

// canonicalCompositeChange is [canonicalDomainChange] for a composite type's
// change, its attribute delta included.
func canonicalCompositeChange(value *CompositeChange) *CompositeChange {
	canonical := value.Copy()
	canonical.Differs = nilWhenEmpty(canonical.Differs)
	canonical.Added = nilWhenEmpty(canonical.Added)
	canonical.Removed = nilWhenEmpty(canonical.Removed)
	return canonical
}

// canonicalRangeChange is [canonicalDomainChange] for a range type's change,
// whose declared operand encodes its cleared attributes sorted.
func canonicalRangeChange(value *RangeChange) *RangeChange {
	canonical := value.Copy()
	canonical.Differs = nilWhenEmpty(canonical.Differs)
	if canonical.After != nil {
		canonical.After = canonicalDesiredRange(canonical.After)
	}
	return canonical
}

// changeDefinition is a change's schema: nullable operands of the observed
// and desired definitions, beside the change's own properties.
func changeDefinition(observed, desired json.RawMessage, properties string) json.RawMessage {
	return json.RawMessage(`{"type":"object","additionalProperties":false,"properties":{` +
		`"before":{"anyOf":[{"type":"null"},` + string(observed) + `]},"after":{"anyOf":[{"type":"null"},` + string(desired) + `]},` +
		properties + `}}`)
}

// changeWire checks a change's keys, each operand's wire and, where rest is
// not nil, the change's own nested values.
func changeWire(shape schemaext.ObjectShape, before, after func(json.RawMessage) error,
	rest func(map[string]json.RawMessage) error,
) func(json.RawMessage) error {
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
			if err := after(operand); err != nil {
				return err
			}
		}
		if rest != nil {
			return rest(fields)
		}
		return nil
	}
}

// noEmptyLists refuses an empty list under a key of shape that may hold
// null: the encoder writes a list it does not hold as null, so accepting []
// would give one value two encodings.
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
