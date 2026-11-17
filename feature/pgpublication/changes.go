package pgpublication

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/schemaext"
)

// Change records how one publication differs. A nil Before creates it, a nil
// After drops it, and both together change a surviving one. Both describe
// established state, never an unread one.
//
// For a surviving publication the change also records what the comparison
// established and the operands cannot show, each member qualified by the
// schema the target resolves it in. Rebuild is set when the membership form
// changes to or from FOR ALL TABLES, which ALTER PUBLICATION cannot do, so the
// publication is dropped and created again. Otherwise AddTables and
// DropTables are the tables it starts and stops publishing, AddSchemas and
// DropSchemas the schemas. Refiltered are the tables both sides name whose row
// filter or column list differs: Ptah writes neither, so the planner refuses
// the change rather than publish more of the table than the publication does.
// Options is set when the publish set, publish_via_partition_root or
// publish_generated_columns differs, and Owner is the declared owner when it
// differs from the one the publication has, or when a rebuild gives the
// publication to the role that creates it again.
type Change struct {
	Before      *ObservedPublication `json:"before"`
	After       *DesiredPublication  `json:"after"`
	Rebuild     bool                 `json:"rebuild"`
	AddTables   []Member             `json:"add_tables,omitempty"`
	DropTables  []Member             `json:"drop_tables,omitempty"`
	AddSchemas  []string             `json:"add_schemas,omitempty"`
	DropSchemas []string             `json:"drop_schemas,omitempty"`
	Refiltered  []Member             `json:"refiltered,omitempty"`
	Options     bool                 `json:"options"`
	Owner       string               `json:"owner,omitempty"`
}

// Kind returns the stable change identity.
func (*Change) Kind() schemaext.Kind { return ChangeKind }

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (v *Change) CloneChange() schemaext.ChangeValue {
	if v == nil {
		return (*Change)(nil)
	}
	return v.Copy()
}

// Copy is [Change.CloneChange] without the interface: it shares no operand
// or list with v, and a nil receiver returns nil.
func (v *Change) Copy() *Change {
	if v == nil {
		return nil
	}
	return &Change{Before: v.Before.Copy(), After: v.After.Copy(), Rebuild: v.Rebuild,
		AddTables: copyMembers(v.AddTables), DropTables: copyMembers(v.DropTables),
		AddSchemas: slices.Clone(v.AddSchemas), DropSchemas: slices.Clone(v.DropSchemas), Refiltered: copyMembers(v.Refiltered),
		Options: v.Options, Owner: v.Owner}
}

// RemovesState reports a drop: a change without a declaration removes the
// publication.
func (v *Change) RemovesState() bool { return v != nil && v.After == nil }

// ChangesMembership reports whether a surviving publication starts or stops
// publishing a table or a schema in place.
func (v *Change) ChangesMembership() bool {
	return v != nil && (len(v.AddTables) != 0 || len(v.DropTables) != 0 || len(v.AddSchemas) != 0 || len(v.DropSchemas) != 0)
}

// Validate refuses missing or invalid operands, a membership change, a
// rebuild, an option or an owner on a publication the change creates or
// drops, a rebuild together with a membership change, and a change of a
// surviving publication that changes nothing.
func (v *Change) Validate() error {
	if v == nil || (v.Before == nil && v.After == nil) {
		return fmt.Errorf("%w: a publication change requires a before or after operand", schemaext.ErrInvalidValue)
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
	for _, members := range [][]Member{v.AddTables, v.DropTables, v.Refiltered} {
		for _, member := range members {
			if member.Schema == "" || member.Table == "" || member.Filtered() {
				return fmt.Errorf("%w: a publication change names each table by schema and name alone", schemaext.ErrInvalidValue)
			}
		}
	}
	if err := validNames("schema", slices.Concat(v.AddSchemas, v.DropSchemas)); err != nil {
		return err
	}
	if err := validTexts("owner", v.Owner); err != nil {
		return err
	}
	return v.validateDelta()
}

// validateDelta refuses a record of what changes that its operands cannot
// carry: a member, option or owner change of a publication that is created or
// dropped, a rebuild that also names members, and a surviving publication
// whose change names nothing.
func (v *Change) validateDelta() error {
	if v.Before == nil || v.After == nil {
		if v.Rebuild || v.ChangesMembership() || len(v.Refiltered) != 0 || v.Options || v.Owner != "" {
			return fmt.Errorf("%w: only a surviving publication changes its members, its options or its owner", schemaext.ErrInvalidValue)
		}
		return nil
	}
	if v.Rebuild && (v.ChangesMembership() || len(v.Refiltered) != 0) {
		return fmt.Errorf("%w: a rebuilt publication is created with its members", schemaext.ErrInvalidValue)
	}
	if !v.Rebuild && !v.ChangesMembership() && len(v.Refiltered) == 0 && !v.Options && v.Owner == "" {
		return fmt.Errorf("%w: a change of a surviving publication changes its members, its options or its owner", schemaext.ErrInvalidValue)
	}
	return nil
}

// Effect records what the change does to the publication and to the
// consumers that read it. Every change of what a publication publishes is
// behavioral, whether it publishes more or less: a subscriber receives other
// rows afterwards.
func (v *Change) Effect() schemaext.Effect {
	if v == nil || v.Validate() != nil {
		return schemaext.Effect{}
	}
	switch {
	case v.Before == nil:
		return schemaext.Effect{Impact: schemaext.Additive, Reason: "creates a publication; no consumer reads it until a subscription does"}
	case v.After == nil:
		return schemaext.Effect{Impact: schemaext.Destructive, Reason: "DROP PUBLICATION stops logical replication to every subscriber that reads the publication"}
	}
	var reasons []string
	if v.Rebuild {
		reasons = append(reasons, "drops and creates the publication again to change its membership form")
	}
	if len(v.AddTables) != 0 || len(v.AddSchemas) != 0 {
		reasons = append(reasons, "publishes more tables to the publication's subscribers")
	}
	if len(v.DropTables) != 0 || len(v.DropSchemas) != 0 {
		reasons = append(reasons, "stops publishing tables to the publication's subscribers")
	}
	if len(v.Refiltered) != 0 {
		reasons = append(reasons, "changes which rows or columns of a table the publication publishes")
	}
	if v.Options {
		reasons = append(reasons, "changes which changes the publication publishes")
	}
	if v.Owner != "" {
		reasons = append(reasons, "gives the publication to another role, which the publication's subscribers then depend on")
	}
	return schemaext.Effect{Impact: schemaext.Behavioral, Reason: strings.Join(reasons, "; ")}
}

// changeShape is the change wire. A list key is left out while it is empty;
// every other key is required, and an operand is null where the change has
// none.
var changeShape = schemaext.ObjectShape{Name: "publication change",
	Allowed: []string{"before", "after", "rebuild", "add_tables", "drop_tables", "add_schemas", "drop_schemas", "refiltered", "options",
		"owner"},
	Required: []string{"before", "after", "rebuild", "options"}, Nullable: []string{"before", "after"},
	NonEmpty: []string{"add_tables", "drop_tables", "add_schemas", "drop_schemas", "refiltered", "owner"}}

// ChangeCodec describes the complete wire of a publication change: both
// operands, each in its model's own wire form and null when absent, and what
// the comparison established. Every refusal is a
// [schemaext.InvalidModelError].
func ChangeCodec() schemaext.Codec {
	models := Codecs()
	desired, observed := models[0], models[1]
	return schemaext.ModelCodec[*Change]{
		Prototype: &Change{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(fmt.Sprintf(`{"type":"object","required":["%s"],"additionalProperties":false,"properties":{`+
			`"before":{"anyOf":[{"type":"null"},%s]},"after":{"anyOf":[{"type":"null"},%s]},"rebuild":{"type":"boolean"},`+
			`"add_tables":{"type":"array","minItems":1,"items":%s},"drop_tables":{"type":"array","minItems":1,"items":%s},`+
			`"add_schemas":%s,"drop_schemas":%s,"refiltered":{"type":"array","minItems":1,"items":%s},`+
			`"options":{"type":"boolean"},"owner":{"type":"string","minLength":1}}}`,
			strings.Join(changeShape.Required, `","`), observed.Definition, desired.Definition,
			changeMemberDefinition, changeMemberDefinition, namesDefinition, namesDefinition, changeMemberDefinition)),
		Shape:     operandsShape(observed, desired),
		Validate:  (*Change).Validate,
		Canonical: canonicalChange,
		Clone:     (*Change).Copy,
	}.Codec()
}

// changeMemberDefinition is a member a change adds or drops: always qualified,
// never filtered.
const changeMemberDefinition = `{"type":"object","required":["schema","table"],"additionalProperties":false,"properties":{` +
	`"schema":{"type":"string","minLength":1},"table":{"type":"string","minLength":1}}}`

var changeMemberShape = schemaext.ObjectShape{Name: "publication change member",
	Allowed: []string{"schema", "table"}, Required: []string{"schema", "table"}}

// operandsShape checks a change's keys, then each operand it holds with the
// model's codec and each member it adds or drops. A struct decoder alone would
// accept an operand key in another letter case or an omitted value spelled
// out.
func operandsShape(observed, desired schemaext.Codec) func(json.RawMessage) error {
	return func(data json.RawMessage) error {
		fields, err := schemaext.DecodeObject(data, changeShape)
		if err != nil {
			return err
		}
		for _, operand := range []struct {
			key   string
			codec schemaext.Codec
		}{{"before", observed}, {"after", desired}} {
			if string(fields[operand.key]) == "null" {
				continue
			}
			if _, err := operand.codec.Decode(fields[operand.key]); err != nil {
				return err
			}
		}
		for _, key := range []string{"add_tables", "drop_tables", "refiltered"} {
			raw, found := fields[key]
			if !found {
				continue
			}
			var members []json.RawMessage
			if err := json.Unmarshal(raw, &members); err != nil {
				return fmt.Errorf("%w: publication change %s: %w", schemaext.ErrInvalidValue, key, err)
			}
			for _, member := range members {
				if _, err := schemaext.DecodeObject(member, changeMemberShape); err != nil {
					return err
				}
			}
		}
		return nil
	}
}

// canonicalChange orders the operands and the lists as their codecs do, so a
// change encodes to the same bytes however the comparison built it.
func canonicalChange(value *Change) *Change {
	canonical := value.Copy()
	if canonical.Before != nil {
		canonical.Before = canonicalObserved(canonical.Before)
	}
	if canonical.After != nil {
		canonical.After = canonicalDesired(canonical.After)
	}
	canonical.AddTables = nilIfEmpty(sortedMembers(canonical.AddTables))
	canonical.DropTables = nilIfEmpty(sortedMembers(canonical.DropTables))
	canonical.AddSchemas = nilIfEmpty(sortedNames(canonical.AddSchemas))
	canonical.DropSchemas = nilIfEmpty(sortedNames(canonical.DropSchemas))
	canonical.Refiltered = nilIfEmpty(sortedMembers(canonical.Refiltered))
	return canonical
}
