// Package pgrole owns PostgreSQL role configuration settings: the defaults
// `ALTER ROLE name SET parameter = value` stores for a role in every database,
// which PostgreSQL keeps in pg_db_role_setting with setdatabase 0. The model
// is a facet of the role, in a declared and an observed representation. It
// contains no catalog access, SQL frontend or provider selection.
//
// A declaration manages only the settings it names. Each is a value or an
// explicit reset, and a setting the declaration does not name is left as the
// server holds it: omission never means RESET, and nothing here plans RESET
// ALL. A default stored for one database (ALTER ROLE ... IN DATABASE) or for
// every role (ALTER ROLE ALL) is a different object, which this model does
// not describe.
//
// A setting takes effect when a session logs in as the role. SET ROLE does not
// load it, and a session already open keeps the value it started with.
package pgrole

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
)

// Owner identifies the provider of PostgreSQL role settings.
const Owner = "ptah.run/postgres/role"

// SettingsKind identifies the configuration defaults a role holds in every
// database.
const SettingsKind schemaext.Kind = "ptah.run/postgres/role-settings"

// SupportsSettings reports whether target stores per-role defaults the way
// this model reads them. Only PostgreSQL does: CockroachDB and YugabyteDB keep
// role settings elsewhere or apply them differently, and neither is measured.
func SupportsSettings(target string) bool {
	return platform.NormalizeDialect(target) == platform.Postgres
}

// DesiredSettings is the settings a declaration manages, each a value or an
// explicit reset, in name order with each name once. An empty list manages
// none, as a role without the facet does.
type DesiredSettings struct {
	Settings []Setting `json:"settings"`
}

// ObservedSettings is the per-role defaults a read found, each with the value
// the catalog stores, in name order with each name once. An empty list holds
// none, which a declaration holding only resets converts to.
type ObservedSettings struct {
	Settings []Setting `json:"settings"`
}

// Kind returns the owned facet identity.
func (*DesiredSettings) Kind() schemaext.Kind { return SettingsKind }

// Kind returns the owned facet identity.
func (*ObservedSettings) Kind() schemaext.Kind { return SettingsKind }

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (s *DesiredSettings) Clone() schemaext.Value {
	if s == nil {
		return (*DesiredSettings)(nil)
	}
	return &DesiredSettings{Settings: slices.Clone(s.Settings)}
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (s *ObservedSettings) Clone() schemaext.Value {
	if s == nil {
		return (*ObservedSettings)(nil)
	}
	return &ObservedSettings{Settings: slices.Clone(s.Settings)}
}

// Equal compares declarations. Different representations are unequal.
func (s *DesiredSettings) Equal(other schemaext.Value) bool {
	o, ok := other.(*DesiredSettings)
	if !ok || s == nil || o == nil {
		return ok && s == nil && o == nil
	}
	return slices.Equal(s.Settings, o.Settings)
}

// Equal compares observations. Different representations are unequal.
func (s *ObservedSettings) Equal(other schemaext.Value) bool {
	o, ok := other.(*ObservedSettings)
	if !ok || s == nil || o == nil {
		return ok && s == nil && o == nil
	}
	return slices.Equal(s.Settings, o.Settings)
}

// Declare returns a declaration of settings in name order. It refuses a name
// given twice, in any case, and an invalid name or value.
func Declare(settings []Setting) (*DesiredSettings, error) {
	sorted := slices.Clone(settings)
	slices.SortStableFunc(sorted, compareSettings)
	declared := &DesiredSettings{Settings: sorted}
	if err := ValidateDesiredSettings(declared); err != nil {
		return nil, err
	}
	return declared, nil
}

// Observe returns an observation of settings in name order. It refuses what
// [ValidateObservedSettings] refuses.
func Observe(settings []Setting) (*ObservedSettings, error) {
	sorted := slices.Clone(settings)
	slices.SortStableFunc(sorted, compareSettings)
	observed := &ObservedSettings{Settings: sorted}
	if err := ValidateObservedSettings(observed); err != nil {
		return nil, err
	}
	return observed, nil
}

func compareSettings(a, b Setting) int {
	return cmp.Or(cmp.Compare(Key(a.Name), Key(b.Name)), cmp.Compare(a.Name, b.Name))
}

// ValidateDesiredSettings refuses nil, a list out of name order
// or naming one setting twice, a setting with both a value and a reset or
// neither, and a name or value that [ParseName] or [ParseValue] would not
// return. Its errors wrap schemaext.ErrInvalidValue as
// schemaext.InvalidModelError.
func ValidateDesiredSettings(s *DesiredSettings) error {
	if s == nil {
		return settingsError(schemaext.Desired, "nil declaration")
	}
	if err := validList(s.Settings); err != nil {
		return settingsError(schemaext.Desired, err.Error())
	}
	for _, setting := range s.Settings {
		if err := validDeclared(setting); err != nil {
			return settingsError(schemaext.Desired, err.Error())
		}
	}
	return nil
}

// ValidateObservedSettings refuses nil, a list out of name order or naming one setting twice, a reset, and a name or value that is not
// valid text. A stored value is the server's and is not read as SQL.
func ValidateObservedSettings(s *ObservedSettings) error {
	if s == nil {
		return settingsError(schemaext.Observed, "nil observation")
	}
	if err := validList(s.Settings); err != nil {
		return settingsError(schemaext.Observed, err.Error())
	}
	for _, setting := range s.Settings {
		if setting.Reset {
			return settingsError(schemaext.Observed, fmt.Sprintf("an observed setting %q holds a value, not a reset", setting.Name))
		}
		if err := validObservedName(setting.Name); err != nil {
			return settingsError(schemaext.Observed, err.Error())
		}
		if err := schemaext.ValidText("role setting value", setting.Value); err != nil {
			return settingsError(schemaext.Observed, err.Error())
		}
	}
	return nil
}

func validList(settings []Setting) error {
	for i := 1; i < len(settings); i++ {
		if Key(settings[i-1].Name) == Key(settings[i].Name) {
			return fmt.Errorf("role setting %q is named twice", settings[i].Name)
		}
		if compareSettings(settings[i-1], settings[i]) > 0 {
			return fmt.Errorf("role settings are held in name order")
		}
	}
	return nil
}

func validDeclared(setting Setting) error {
	if err := validObservedName(setting.Name); err != nil {
		return err
	}
	if parsed, err := ParseName(QuoteName(setting.Name)); err != nil || parsed != setting.Name {
		return fmt.Errorf("role setting name %q is not held as a name is read", setting.Name)
	}
	if setting.Reset {
		if setting.Value != "" {
			return fmt.Errorf("role setting %q is a reset and holds no value", setting.Name)
		}
		return nil
	}
	value, form, err := ParseValue(setting.Value)
	if err != nil {
		return err
	}
	if form == ValueDefault {
		return fmt.Errorf("role setting %q holds DEFAULT, which is a reset", setting.Name)
	}
	if value != setting.Value {
		return fmt.Errorf("role setting %q holds a value that is not normalized", setting.Name)
	}
	return nil
}

func validObservedName(name string) error {
	if err := schemaext.ValidText("role setting name", name); err != nil {
		return err
	}
	if name == "" {
		return fmt.Errorf("a role setting needs a parameter name")
	}
	return nil
}

func settingsError(representation schemaext.Representation, reason string) error {
	return &schemaext.InvalidModelError{Kind: SettingsKind, Representation: representation, Message: reason}
}

// WithSettings adds a declaration of settings to facets without changing
// them. No settings add nothing. A duplicate kind and an invalid setting are
// refused.
func WithSettings(facets schemaext.Facets, settings []Setting) (schemaext.Facets, error) {
	if len(settings) == 0 {
		return facets, nil
	}
	declared, err := Declare(settings)
	if err != nil {
		return schemaext.Facets{}, err
	}
	return facets.With(declared)
}

// WithObservedSettings adds an observation under WithSettings's rules.
func WithObservedSettings(facets schemaext.Facets, settings []Setting) (schemaext.Facets, error) {
	if len(settings) == 0 {
		return facets, nil
	}
	observed, err := Observe(settings)
	if err != nil {
		return schemaext.Facets{}, err
	}
	return facets.With(observed)
}

// SettingsOf reads either representation. Missing settings return nil and
// false. An invalid or unexpected value wraps schemaext.ErrInvalidValue.
func SettingsOf(facets schemaext.Facets) ([]Setting, bool, error) {
	value, found, err := facets.Get(SettingsKind)
	if err != nil || !found {
		return nil, found, err
	}
	switch s := value.(type) {
	case *DesiredSettings:
		if err := ValidateDesiredSettings(s); err != nil {
			return nil, true, err
		}
		return slices.Clone(s.Settings), true, nil
	case *ObservedSettings:
		if err := ValidateObservedSettings(s); err != nil {
			return nil, true, err
		}
		return slices.Clone(s.Settings), true, nil
	default:
		return nil, true, fmt.Errorf("%w: role settings have unexpected type %T", schemaext.ErrInvalidValue, value)
	}
}

const settingDefinition = `{"type":"object","required":["name"],"additionalProperties":false,"properties":{` +
	`"name":{"type":"string","minLength":1},"value":{"type":"string","minLength":1},"reset":{"const":true}}}`

const settingsDefinition = `{"type":"object","required":["settings"],"additionalProperties":false,"properties":{` +
	`"settings":{"type":"array","items":` + settingDefinition + `}}}`

// SettingsCodecs returns independent version-one declaration and observation
// codecs. Unknown fields and invalid settings are refused.
func SettingsCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		schemaext.ModelCodec[*DesiredSettings]{Prototype: &DesiredSettings{}, Representation: schemaext.Desired, Version: 1,
			Definition: json.RawMessage(settingsDefinition), Shape: settingsShape, Validate: ValidateDesiredSettings,
			Clone: func(s *DesiredSettings) *DesiredSettings { return s.Clone().(*DesiredSettings) }}.Codec(),
		schemaext.ModelCodec[*ObservedSettings]{Prototype: &ObservedSettings{}, Representation: schemaext.Observed, Version: 1,
			Definition: json.RawMessage(settingsDefinition), Shape: settingsShape, Validate: ValidateObservedSettings,
			Clone: func(s *ObservedSettings) *ObservedSettings { return s.Clone().(*ObservedSettings) }}.Codec(),
	}
}

func settingsShape(data json.RawMessage) error {
	fields, err := schemaext.DecodeObject(data, schemaext.ObjectShape{Name: "role settings", Allowed: []string{"settings"}, Required: []string{"settings"}})
	if err != nil {
		return err
	}
	var items []json.RawMessage
	if err := json.Unmarshal(fields["settings"], &items); err != nil || items == nil {
		return fmt.Errorf("%w: role settings hold a list of settings", schemaext.ErrInvalidValue)
	}
	for _, item := range items {
		if _, err := schemaext.DecodeObject(item, schemaext.ObjectShape{Name: "role setting", Allowed: []string{"name", "value", "reset"}, Required: []string{"name"}}); err != nil {
			return err
		}
	}
	return nil
}

var settingsCoverage = schemaext.OwnedCoverageSource(Owner, SettingsCodecs)

// SettingsCoverage records what a source knows of role settings. A read that
// describes a role's settings claims complete knowledge of them, so a role
// without the facet holds none. Subject records keep limits for one role.
func SettingsCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	return settingsCoverage(SettingsKind, representation, knowledge, subjects)
}

// HoldsSettings reports whether subject is a named role.
func HoldsSettings(subject objectidentity.ID) bool {
	return subject.Kind == objectidentity.KindRole && !subject.Name.Empty()
}
