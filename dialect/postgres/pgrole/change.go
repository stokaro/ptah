package pgrole

import (
	"encoding/json"
	"fmt"
	"slices"

	"ptah.run/core/schemaext"
)

// SettingsChangeKind identifies a change of the settings a role holds.
const SettingsChangeKind schemaext.Kind = "ptah.run/postgres/role-settings-change"

// SettingChange is one setting a change sets or resets. Before is the value
// the catalog stores, nil where the role holds none or where it is not known,
// as for a RESET a SQL document writes. After is the value in SQL a
// declaration asks for, nil for a reset.
type SettingChange struct {
	Name   string  `json:"name"`
	Before *string `json:"before"`
	After  *string `json:"after"`
}

// SettingsChange is the settings one role changes, in name order with each
// name once. A setting nobody declared is never in it: the change sets what a
// declaration asks for and resets only what a declaration resets.
type SettingsChange struct {
	Settings []SettingChange `json:"settings"`
}

// Kind returns the owned change identity.
func (*SettingsChange) Kind() schemaext.Kind { return SettingsChangeKind }

// Effect describes the change: the role's sessions start with other values.
func (*SettingsChange) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Behavioral,
		Reason: "sessions that log in as the role start with other parameter values; open sessions keep theirs"}
}

// CloneChange returns independent operands. A nil receiver remains typed nil.
func (c *SettingsChange) CloneChange() schemaext.ChangeValue { return c.Copy() }

// Copy returns independent operands, or nil for a nil receiver.
func (c *SettingsChange) Copy() *SettingsChange {
	if c == nil {
		return nil
	}
	result := &SettingsChange{Settings: make([]SettingChange, len(c.Settings))}
	for i, setting := range c.Settings {
		result.Settings[i] = SettingChange{Name: setting.Name, Before: cloneText(setting.Before), After: cloneText(setting.After)}
	}
	return result
}

func cloneText(text *string) *string {
	if text == nil {
		return nil
	}
	return new(*text)
}

// ValidateSettingsChange refuses nil, an empty change, a list out of name
// order or naming one setting twice, a value set to what the role holds, and
// an operand that is not valid. Every error wraps schemaext.ErrInvalidValue.
func ValidateSettingsChange(c *SettingsChange) error {
	if c == nil || len(c.Settings) == 0 {
		return fmt.Errorf("%w: a role settings change requires a setting", schemaext.ErrInvalidValue)
	}
	for i, setting := range c.Settings {
		if i > 0 && Key(c.Settings[i-1].Name) >= Key(setting.Name) {
			return fmt.Errorf("%w: a role settings change names each setting once, in name order", schemaext.ErrInvalidValue)
		}
		if err := validSettingChange(setting); err != nil {
			return err
		}
	}
	return nil
}

func validSettingChange(setting SettingChange) error {
	if err := validDeclared(Setting{Name: setting.Name, Reset: true}); err != nil {
		return fmt.Errorf("%w: %s", schemaext.ErrInvalidValue, err.Error())
	}
	if setting.Before != nil {
		if err := schemaext.ValidText("role setting value", *setting.Before); err != nil {
			return err
		}
	}
	if setting.After != nil {
		if err := validDeclared(Setting{Name: setting.Name, Value: *setting.After}); err != nil {
			return fmt.Errorf("%w: %s", schemaext.ErrInvalidValue, err.Error())
		}
		if setting.Before != nil && Matches(setting.Name, *setting.After, *setting.Before) {
			return fmt.Errorf("%w: role setting %q changes to the value it holds", schemaext.ErrInvalidValue, setting.Name)
		}
	}
	return nil
}

// Reverse returns the change that restores what the role held before c,
// without changing c: a value set where the role held none is reset, a reset
// restores the stored value, and a changed value goes back to the stored one,
// written in SQL ([SQLValue]). It refuses what [ValidateSettingsChange]
// refuses, and a reset whose value before it is not known.
func (c *SettingsChange) Reverse() (*SettingsChange, error) {
	if err := ValidateSettingsChange(c); err != nil {
		return nil, err
	}
	result := &SettingsChange{Settings: make([]SettingChange, 0, len(c.Settings))}
	for _, setting := range c.Settings {
		if setting.Before == nil && setting.After == nil {
			return nil, fmt.Errorf("%w: the value role setting %q held before its reset is not known", schemaext.ErrInvalidValue, setting.Name)
		}
		reversed := SettingChange{Name: setting.Name}
		if setting.After != nil {
			reversed.Before = new(StoredValue(setting.Name, *setting.After))
		}
		if setting.Before != nil {
			value, _, err := ParseValue(SQLValue(setting.Name, *setting.Before))
			if err != nil {
				return nil, err
			}
			reversed.After = &value
		}
		result.Settings = append(result.Settings, reversed)
	}
	if err := ValidateSettingsChange(result); err != nil {
		return nil, err
	}
	return result, nil
}

// SettingsChangeCodec returns an independent version-one change codec. Each
// setting requires both operand keys; an absent operand is null.
func SettingsChangeCodec() schemaext.Codec {
	return schemaext.ModelCodec[*SettingsChange]{
		Prototype: &SettingsChange{}, Representation: schemaext.Change, Version: 1,
		Definition: json.RawMessage(settingsChangeDefinition),
		Shape:      settingsChangeShape, Validate: ValidateSettingsChange, Clone: (*SettingsChange).Copy,
	}.Codec()
}

func settingsChangeShape(data json.RawMessage) error {
	fields, err := schemaext.DecodeObject(data, schemaext.ObjectShape{Name: "role settings change", Allowed: []string{"settings"}, Required: []string{"settings"}})
	if err != nil {
		return err
	}
	var items []json.RawMessage
	if err := json.Unmarshal(fields["settings"], &items); err != nil || len(items) == 0 {
		return fmt.Errorf("%w: a role settings change holds a list of at least one setting", schemaext.ErrInvalidValue)
	}
	for _, item := range items {
		if _, err := schemaext.DecodeObject(item, schemaext.ObjectShape{Name: "role setting change", Allowed: []string{"name", "before", "after"},
			Required: []string{"name", "before", "after"}, Nullable: []string{"before", "after"}}); err != nil {
			return err
		}
	}
	return nil
}

const settingChangeDefinition = `{"type":"object","required":["name","before","after"],"additionalProperties":false,"properties":{` +
	`"name":{"type":"string","minLength":1},"before":{"type":["string","null"]},"after":{"type":["string","null"],"minLength":1}}}`

const settingsChangeDefinition = `{"type":"object","required":["settings"],"additionalProperties":false,"properties":{` +
	`"settings":{"type":"array","minItems":1,"items":` + settingChangeDefinition + `}}}`

// Changes compares a declaration with an observation of one role and returns
// the settings that differ, or nil when none does. Only declared settings are
// compared: a value the role does not hold, or holds in another value ([Matches]),
// is set, and a reset is planned only for a setting the role holds. A setting
// the role holds and the declaration does not name is left alone.
func Changes(declared []Setting, observed []Setting) *SettingsChange {
	held := make(map[string]Setting, len(observed))
	for _, setting := range observed {
		held[Key(setting.Name)] = setting
	}
	var changes []SettingChange
	for _, setting := range declared {
		current, holds := held[Key(setting.Name)]
		switch {
		case setting.Reset && holds:
			changes = append(changes, SettingChange{Name: setting.Name, Before: new(current.Value)})
		case setting.Reset:
		case !holds:
			changes = append(changes, SettingChange{Name: setting.Name, After: new(setting.Value)})
		case !Matches(setting.Name, setting.Value, current.Value):
			changes = append(changes, SettingChange{Name: setting.Name, Before: new(current.Value), After: new(setting.Value)})
		}
	}
	if len(changes) == 0 {
		return nil
	}
	slices.SortFunc(changes, func(a, b SettingChange) int { return compareSettings(Setting{Name: a.Name}, Setting{Name: b.Name}) })
	return &SettingsChange{Settings: changes}
}
