package synonym

import (
	"encoding/json"
	"fmt"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
)

// ChangeKind identifies a change of one synonym.
const ChangeKind schemaext.Kind = "ptah.run/synonym/change"

// OperationKind identifies the statements that make one change.
const OperationKind schemaext.Kind = "ptah.run/synonym/operation"

const definition = `{"$id":"ptah.run/synonym/v1",` +
	`"synonym":{"schema":"the schema the alias lives in; omitted for the default schema","name":"the alias",` +
	`"target":"the object it stands for, as one to four dot-separated parts"},` +
	`"desired":{"comment":"optional documentation written above the statement","struct_name":"the Go struct that declared it"}}`

var (
	synonymFields = []string{"schema", "name", "target"}
	desiredShape  = schemaext.ObjectShape{Name: "synonym",
		Allowed: append(synonymFields, "comment", "struct_name"), Required: []string{"name", "target"},
		NonEmpty: []string{"schema", "comment", "struct_name"}}
	observedShape = schemaext.ObjectShape{Name: "synonym observation",
		Allowed: synonymFields, Required: []string{"name", "target"}, NonEmpty: []string{"schema"}}
)

// Codecs returns the version-one desired and observed synonym codecs, the
// change codec and the operation codec. A decoder accepts only what the
// encoder writes: a key in another letter case, a null, and an omitted value
// written out are refused. Every refusal is a [schemaext.InvalidModelError].
func Codecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredSynonym]{
			Prototype: &DesiredSynonym{}, Representation: schemaext.Desired, Version: 1, Definition: json.RawMessage(definition),
			Shape: func(data json.RawMessage) error { _, err := schemaext.DecodeObject(data, desiredShape); return err }, Validate: ValidateDesired,
		}.Codec(),
		schemaext.ModelCodec[*ObservedSynonym]{
			Prototype: &ObservedSynonym{}, Representation: schemaext.Observed, Version: 1, Definition: json.RawMessage(definition),
			Shape: func(data json.RawMessage) error { _, err := schemaext.DecodeObject(data, observedShape); return err }, Validate: ValidateObserved,
		}.Codec(),
		changeCodec(),
		operationCodec(),
	}
}

// Coverage records what one source knows about synonyms: knowledge for every
// synonym, with subjects overriding it. A source that can declare them
// records complete knowledge, which makes a synonym it leaves out one the plan
// drops.
func Coverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	codecs := Codecs()[:2]
	owned := make([]schemaext.OwnedCodec, 0, len(codecs))
	for _, codec := range codecs {
		owned = append(owned, schemaext.OwnedCodec{Owner: Owner, Codec: codec})
	}
	registry, err := schemaext.NewRegistry(owned...)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	for _, model := range registry.Definitions() {
		if model.Kind == Kind && model.Representation == representation {
			return schemaext.NewCoverage(representation, []schemaext.KindCoverage{{Model: model, Knowledge: knowledge}}, subjects)
		}
	}
	return schemaext.Coverage{}, fmt.Errorf("%w: synonym coverage requires a schema representation", schemaext.ErrInvalidValue)
}

// Change retains the synonym the database holds and the one the declaration
// asks for. A nil Before adds the synonym and a nil After drops it; with both,
// the target changes, which neither engine can do in place.
type Change struct {
	Before *ObservedSynonym `json:"before,omitzero"`
	After  *DesiredSynonym  `json:"after,omitzero"`
}

// Kind returns the owned change identity.
func (*Change) Kind() schemaext.Kind { return ChangeKind }

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *Change) CloneChange() schemaext.ChangeValue { return v.Copy() }

// Copy is [Change.CloneChange] without the interface: it shares nothing with
// v, and a nil receiver returns nil.
func (v *Change) Copy() *Change {
	if v == nil {
		return nil
	}
	result := &Change{}
	if v.Before != nil {
		before := *v.Before
		result.Before = &before
	}
	if v.After != nil {
		after := *v.After
		result.After = &after
	}
	return result
}

// RemovesState reports whether the change drops a synonym the declaration no
// longer names. A retarget drops and creates one alias and removes nothing a
// caller applying additively wants kept.
func (v *Change) RemovesState() bool { return v != nil && v.After == nil }

// ValidateChange requires an operand, valid operands, and one synonym on both
// sides.
func ValidateChange(v *Change) error {
	if v == nil || (v.Before == nil && v.After == nil) {
		return fmt.Errorf("%w: a synonym change requires an operand", schemaext.ErrInvalidValue)
	}
	if v.Before != nil {
		if err := ValidateObserved(v.Before); err != nil {
			return err
		}
	}
	if v.After != nil {
		if err := ValidateDesired(v.After); err != nil {
			return err
		}
	}
	if v.Before != nil && v.After != nil && !sameAlias(v.Before.Synonym, v.After.Synonym) {
		return fmt.Errorf("%w: synonym change operands name different synonyms", schemaext.ErrInvalidValue)
	}
	return nil
}

// sameAlias reports whether two synonyms can be one alias. A read leaves the
// connection's default schema out, and only the comparison knows that schema,
// so a side without a schema matches either; two schemas must agree.
func sameAlias(a, b Synonym) bool {
	if fold(a.Name) != fold(b.Name) {
		return false
	}
	return strings.TrimSpace(a.Schema) == "" || strings.TrimSpace(b.Schema) == "" || fold(a.Schema) == fold(b.Schema)
}

func changeCodec() schemaext.Codec {
	shape := schemaext.ObjectShape{Name: "synonym change", Allowed: []string{"before", "after"}}
	return schemaext.ModelCodec[*Change]{
		Prototype: &Change{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(`{"before":"the observed synonym; omitted when the change adds it","after":"the declared synonym; omitted when the change drops it"}`),
		Shape: func(data json.RawMessage) error {
			fields, err := schemaext.DecodeObject(data, shape)
			if err != nil {
				return err
			}
			codecs := Codecs()
			for key, codec := range map[string]schemaext.Codec{"after": codecs[0], "before": codecs[1]} {
				if raw, found := fields[key]; found {
					if _, err := codec.Decode(raw); err != nil {
						return err
					}
				}
			}
			return nil
		},
		Validate: ValidateChange,
		Clone:    (*Change).Copy,
	}.Codec()
}

// Action is what an [Operation] does.
type Action string

// The statements that create, drop and retarget a synonym. Neither engine has
// an ALTER SYNONYM, so a retarget drops the alias and creates it again.
const (
	Create   Action = "create"
	Drop     Action = "drop"
	Retarget Action = "retarget"
)

// Operation creates, drops or retargets Synonym. A drop names the synonym if
// it exists, and so does the drop a retarget starts with.
type Operation struct {
	Action  Action         `json:"action"`
	Synonym DesiredSynonym `json:"synonym"`
}

// Kind returns the stable operation identity.
func (*Operation) Kind() schemaext.Kind { return OperationKind }

// CloneExtension returns an independent operation; a nil receiver remains
// typed nil.
func (v *Operation) CloneExtension() ast.ExtensionPayload { return v.Copy() }

// Copy is [Operation.CloneExtension] without the interface.
func (v *Operation) Copy() *Operation {
	if v == nil {
		return nil
	}
	clone := *v
	return &clone
}

// Effect classifies the statements. No statement here reads or writes a row:
// a create adds a name, a drop takes one away from whatever resolves through
// it, and a retarget changes what the name resolves to.
func (v *Operation) Effect() schemaext.Effect {
	switch {
	case v != nil && v.Action == Drop:
		return schemaext.Effect{Impact: schemaext.Destructive,
			Reason: "dropping a synonym takes the name away from every query and routine that resolves through it"}
	case v != nil && v.Action == Retarget:
		return schemaext.Effect{Impact: schemaext.Behavioral,
			Reason: "a retargeted synonym resolves to another object, and the name is missing between the drop and the create"}
	default:
		return schemaext.Effect{Impact: schemaext.Additive, Reason: "creating a synonym adds a name and touches no row"}
	}
}

// Validate requires a known action and a valid synonym.
func (v *Operation) Validate() error {
	if v == nil {
		return fmt.Errorf("%w: synonym operation is nil", schemaext.ErrInvalidValue)
	}
	if v.Action != Create && v.Action != Drop && v.Action != Retarget {
		return fmt.Errorf("%w: synonym operation has action %q", schemaext.ErrInvalidValue, v.Action)
	}
	return ValidateDesired(&v.Synonym)
}

func operationCodec() schemaext.Codec {
	shape := schemaext.ObjectShape{Name: "synonym operation", Allowed: []string{"action", "synonym"}, Required: []string{"action", "synonym"}}
	return schemaext.ModelCodec[*Operation]{
		Prototype: &Operation{}, Representation: schemaext.Operation, Version: 1,
		Definition: json.RawMessage(`{"action":"create, drop or retarget","synonym":"the declared synonym the statements name"}`),
		Shape: func(data json.RawMessage) error {
			fields, err := schemaext.DecodeObject(data, shape)
			if err != nil {
				return err
			}
			_, err = Codecs()[0].Decode(fields["synonym"])
			return err
		},
		Validate: (*Operation).Validate,
		Clone:    (*Operation).Copy,
	}.Codec()
}
