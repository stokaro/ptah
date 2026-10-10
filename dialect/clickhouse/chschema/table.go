// Package chschema owns the ClickHouse models in their desired and observed
// representations: the storage settings of tables and data-skipping indexes,
// the refresh schedule of a materialized view, and row policies as feature
// objects of their own. It contains no provider selection, database access, or
// DDL.
package chschema

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"ptah.run/core/schemaext"
)

// TableKind identifies ClickHouse storage settings attached to a common table.
const TableKind schemaext.Kind = "ptah.run/clickhouse/table"

// SettingState distinguishes omitted intent, a requested default, and an exact
// value. An exact empty value is distinct from requesting a default.
type SettingState string

const (
	// Unspecified leaves an existing setting unmanaged. Creation still needs
	// the owner's default resolution; it does not establish an observed value.
	Unspecified SettingState = ""
	// Explicit requests the exact value, including an empty optional setting.
	Explicit SettingState = "explicit"
	// Default requests the target's default instead of retaining observed state.
	Default SettingState = "default"
)

// Setting is one declaration's intent. Value contains the engine expression,
// key expression, TTL rule, or SETTINGS body when State is Explicit. Other
// states require an empty Value. Its zero value is unspecified.
type Setting struct {
	State SettingState `json:"state"`
	Value string       `json:"value,omitempty"`
}

// IsZero reports an omitted setting for explicit omitzero serialization.
func (s Setting) IsZero() bool { return s.State == Unspecified && s.Value == "" }

// DesiredTable records intent independently for every owned setting. Missing
// settings and requested defaults remain distinct through cloning and codecs.
// SQL expressions retain their order and spelling; equality is structural.
type DesiredTable struct {
	Engine      Setting `json:"engine,omitzero"`
	OrderBy     Setting `json:"order_by,omitzero"`
	PrimaryKey  Setting `json:"primary_key,omitzero"`
	PartitionBy Setting `json:"partition_by,omitzero"`
	SampleBy    Setting `json:"sample_by,omitzero"`
	TTL         Setting `json:"ttl,omitzero"`
	Settings    Setting `json:"settings,omitzero"`
}

// ObservedTable records the complete storage state reported by a table read.
// Engine includes its parameters. OrderBy and PrimaryKey retain their complete
// expressions even when they agree; physical ordering and the sparse index are
// separate properties. Settings retains the table settings reported by the
// server; it does not enumerate configuration inherited outside the table. An empty
// optional value is an observed absence, not an unread value. Incomplete reads
// must report coverage limits instead of constructing this value.
type ObservedTable struct {
	Engine      string `json:"engine"`
	OrderBy     string `json:"order_by"`
	PrimaryKey  string `json:"primary_key"`
	PartitionBy string `json:"partition_by"`
	SampleBy    string `json:"sample_by"`
	TTL         string `json:"ttl"`
	Settings    string `json:"settings"`
}

// Kind returns the owned table model identity.
func (*DesiredTable) Kind() schemaext.Kind { return TableKind }

// Kind returns the owned table model identity.
func (*ObservedTable) Kind() schemaext.Kind { return TableKind }

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (v *DesiredTable) Clone() schemaext.Value {
	if v == nil {
		return (*DesiredTable)(nil)
	}
	return new(*v)
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedTable) Clone() schemaext.Value {
	if v == nil {
		return (*ObservedTable)(nil)
	}
	return new(*v)
}

// Equal compares declaration intent without resolving defaults or SQL semantics.
func (v *DesiredTable) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredTable)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return *v == *w
}

// Equal compares complete observations without normalizing SQL expressions.
func (v *ObservedTable) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedTable)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return *v == *w
}

// Desired captures every observed property as exact desired state, including
// empty optional settings. It does not turn an observed absence into a default
// request. A nil receiver remains nil.
func (v *ObservedTable) Desired() *DesiredTable {
	if v == nil {
		return nil
	}
	return &DesiredTable{
		Engine: Setting{State: Explicit, Value: v.Engine}, OrderBy: Setting{State: Explicit, Value: v.OrderBy},
		PrimaryKey: Setting{State: Explicit, Value: v.PrimaryKey}, PartitionBy: Setting{State: Explicit, Value: v.PartitionBy},
		SampleBy: Setting{State: Explicit, Value: v.SampleBy}, TTL: Setting{State: Explicit, Value: v.TTL},
		Settings: Setting{State: Explicit, Value: v.Settings},
	}
}

// Observed projects fully resolved desired state without inspecting a server.
// Unspecified settings and default requests are refused with ErrInvalidValue;
// they cannot establish an observed absence or a server default. Nil is invalid.
func (v *DesiredTable) Observed() (*ObservedTable, error) {
	if err := ValidateDesired(v); err != nil {
		return nil, err
	}
	for _, property := range v.properties() {
		if property.setting.State != Explicit {
			return nil, fmt.Errorf("%w: ClickHouse %s needs target resolution before observation projection", schemaext.ErrInvalidValue, property.name)
		}
	}
	return &ObservedTable{
		Engine: v.Engine.Value, OrderBy: v.OrderBy.Value, PrimaryKey: v.PrimaryKey.Value,
		PartitionBy: v.PartitionBy.Value, SampleBy: v.SampleBy.Value, TTL: v.TTL.Value, Settings: v.Settings.Value,
	}, nil
}

type tableProperty struct {
	name    string
	setting Setting
}

func (v *DesiredTable) properties() []tableProperty {
	return []tableProperty{
		{name: "engine", setting: v.Engine}, {name: "order_by", setting: v.OrderBy},
		{name: "primary_key", setting: v.PrimaryKey}, {name: "partition_by", setting: v.PartitionBy},
		{name: "sample_by", setting: v.SampleBy}, {name: "ttl", setting: v.TTL}, {name: "settings", setting: v.Settings},
	}
}

// ValidateDesired checks representation invariants without selecting defaults,
// parsing SQL, or making a target capability claim. The zero declaration is
// valid. Nil, unknown setting states, and contradictory intent return a
// schemaext.InvalidModelError wrapping schemaext.ErrInvalidValue.
func ValidateDesired(v *DesiredTable) error {
	return modelValidation(schemaext.Desired, validateDesired(v))
}

func validateDesired(v *DesiredTable) error {
	if v == nil {
		return fmt.Errorf("%w: nil ClickHouse table declaration", schemaext.ErrInvalidValue)
	}
	for _, property := range v.properties() {
		if err := validateSetting(property.setting); err != nil {
			return fmt.Errorf("ClickHouse %s: %w", property.name, err)
		}
	}
	if v.Engine.State == Explicit && strings.TrimSpace(v.Engine.Value) == "" {
		return fmt.Errorf("%w: an explicit ClickHouse engine cannot be empty", schemaext.ErrInvalidValue)
	}
	return nil
}

// ValidateObserved checks that a complete observation names its engine and has
// representable text. Nil and the zero observation are invalid. Server support
// and SQL equivalence are decisions for the selected target adapter. Invalid
// data returns a schemaext.InvalidModelError for the observed representation.
func ValidateObserved(v *ObservedTable) error {
	if v == nil {
		return modelValidation(schemaext.Observed, fmt.Errorf("%w: nil ClickHouse table observation", schemaext.ErrInvalidValue))
	}
	return modelValidation(schemaext.Observed, validateDesired(v.Desired()))
}

func modelValidation(representation schemaext.Representation, err error) error {
	if err == nil {
		return nil
	}
	return &schemaext.InvalidModelError{Kind: TableKind, Representation: representation, Message: err.Error()}
}

func validateSetting(s Setting) error {
	if !utf8.ValidString(s.Value) {
		return fmt.Errorf("%w: a table setting is not valid UTF-8", schemaext.ErrInvalidValue)
	}
	if strings.ContainsRune(s.Value, '\x00') {
		return fmt.Errorf("%w: a table setting contains a NUL byte", schemaext.ErrInvalidValue)
	}
	switch s.State {
	case Explicit:
		return nil
	case Unspecified, Default:
		if s.Value != "" {
			return fmt.Errorf("%w: a non-explicit setting cannot carry a value", schemaext.ErrInvalidValue)
		}
		return nil
	default:
		return fmt.Errorf("%w: unknown setting state %q", schemaext.ErrInvalidValue, s.State)
	}
}
