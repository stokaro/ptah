package mysqlschema

import (
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
)

// ColumnSettingsKind identifies the MySQL-family settings of one column that
// no other target has a clause for: its character set, `CHARACTER SET x`, and
// its `ON UPDATE expr` clause, the value MySQL and MariaDB assign to the column
// whenever another column of the row changes.
//
// The comparison does not compare these settings. A column whose type or
// default changes is rewritten with MODIFY COLUMN, and that statement carries
// the declared settings; a column whose only difference is one of these
// settings is not planned. A declaration therefore never becomes a change on
// its own, and a value that is absent states nothing.
const ColumnSettingsKind schemaext.Kind = "ptah.run/mysql/column-settings"

// ColumnSettings is the content both representations carry. A field left
// empty is a setting the value does not state.
type ColumnSettings struct {
	// Charset is the column's character set as written or read, such as
	// utf8mb4. Case is kept.
	Charset string
	// OnUpdate is the expression after ON UPDATE, kept verbatim, such as
	// CURRENT_TIMESTAMP(6).
	OnUpdate string
}

// DesiredColumnSettings is what a declaration states for one column. A
// setting it leaves empty is not written: a new column takes the table's
// character set, and has no ON UPDATE clause.
type DesiredColumnSettings struct {
	Charset  string `json:"charset,omitempty"`
	OnUpdate string `json:"on_update,omitempty"`
}

// ObservedColumnSettings is what a read found for one column. The server
// reports a character set for every text column, including one that inherits
// it from its table, so an observed character set does not mean a
// declaration wrote it.
type ObservedColumnSettings struct {
	Charset  string `json:"charset,omitempty"`
	OnUpdate string `json:"on_update,omitempty"`
}

// Kind returns the owned column settings identity.
func (*DesiredColumnSettings) Kind() schemaext.Kind { return ColumnSettingsKind }

// Kind returns the owned column settings identity.
func (*ObservedColumnSettings) Kind() schemaext.Kind { return ColumnSettingsKind }

// Clone returns an independent declaration. A nil receiver remains typed nil.
func (v *DesiredColumnSettings) Clone() schemaext.Value {
	if v == nil {
		return (*DesiredColumnSettings)(nil)
	}
	return new(*v)
}

// Clone returns an independent observation. A nil receiver remains typed nil.
func (v *ObservedColumnSettings) Clone() schemaext.Value {
	if v == nil {
		return (*ObservedColumnSettings)(nil)
	}
	return new(*v)
}

// Equal compares the settings as written: character set names and
// expressions are compared byte for byte.
func (v *DesiredColumnSettings) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredColumnSettings)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return *v == *w
}

// Equal compares the settings as read.
func (v *ObservedColumnSettings) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedColumnSettings)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return *v == *w
}

// Desired returns the declaration that writes the observed settings exactly,
// as a rollback or an export restores them. A nil receiver remains nil.
func (v *ObservedColumnSettings) Desired() *DesiredColumnSettings {
	if v == nil {
		return nil
	}
	return &DesiredColumnSettings{Charset: v.Charset, OnUpdate: v.OnUpdate}
}

// Observed returns the observation a server would report for a column created
// from the declaration. A nil receiver remains nil.
func (v *DesiredColumnSettings) Observed() *ObservedColumnSettings {
	if v == nil {
		return nil
	}
	return &ObservedColumnSettings{Charset: v.Charset, OnUpdate: v.OnUpdate}
}

// ValidateDesiredColumnSettings refuses a declaration that states nothing, a
// setting with surrounding white space, and one that holds a NUL byte. Every
// refusal wraps [schemaext.ErrInvalidValue].
func ValidateDesiredColumnSettings(v *DesiredColumnSettings) error {
	if v == nil {
		return fmt.Errorf("%w: nil MySQL column settings", schemaext.ErrInvalidValue)
	}
	return validateSettings(ColumnSettings{Charset: v.Charset, OnUpdate: v.OnUpdate})
}

// ValidateObservedColumnSettings refuses what [ValidateDesiredColumnSettings]
// refuses.
func ValidateObservedColumnSettings(v *ObservedColumnSettings) error {
	if v == nil {
		return fmt.Errorf("%w: nil MySQL column settings", schemaext.ErrInvalidValue)
	}
	return validateSettings(ColumnSettings{Charset: v.Charset, OnUpdate: v.OnUpdate})
}

func validateSettings(settings ColumnSettings) error {
	if settings == (ColumnSettings{}) {
		return fmt.Errorf("%w: MySQL column settings state nothing; omit the value instead", schemaext.ErrInvalidValue)
	}
	for _, setting := range []struct{ name, value string }{{"charset", settings.Charset}, {"on_update", settings.OnUpdate}} {
		if setting.value != strings.TrimSpace(setting.value) || strings.ContainsRune(setting.value, 0) {
			return fmt.Errorf("%w: MySQL column setting %s %q must be trimmed text", schemaext.ErrInvalidValue, setting.name, setting.value)
		}
	}
	return nil
}

// Settings returns the column settings a facet collection holds, in either
// representation: a declaration from a source, or an observation a rollback
// or an export restores. found is false when the collection holds none. A
// value of another type under the kind is an error.
func Settings(facets schemaext.Facets) (settings ColumnSettings, found bool, err error) {
	value, found, err := facets.Get(ColumnSettingsKind)
	if err != nil || !found {
		return ColumnSettings{}, found, err
	}
	settings, ok := ValueSettings(value)
	if !ok {
		return ColumnSettings{}, true, fmt.Errorf("%w: facet %q has unexpected type %T", schemaext.ErrInvalidValue, ColumnSettingsKind, value)
	}
	return settings, true, nil
}

// ValueSettings returns the settings one value holds, in either
// representation, and false for a value of another type.
func ValueSettings(value schemaext.Value) (ColumnSettings, bool) {
	switch typed := value.(type) {
	case *DesiredColumnSettings:
		if typed != nil {
			return ColumnSettings{Charset: typed.Charset, OnUpdate: typed.OnUpdate}, true
		}
	case *ObservedColumnSettings:
		if typed != nil {
			return ColumnSettings{Charset: typed.Charset, OnUpdate: typed.OnUpdate}, true
		}
	}
	return ColumnSettings{}, false
}

// Targets are the targets the settings belong to. A source that writes them
// in MySQL syntax binds them to these, so another target leaves them out
// rather than refusing a schema written for several engines.
func Targets() []string { return []string{platform.MariaDB, platform.MySQL} }

// WithColumnSettings returns facets with value added as a declaration bound to
// [Targets]. An empty value returns facets unchanged; a collection that
// already holds the kind is refused.
func WithColumnSettings(facets schemaext.Facets, value ColumnSettings) (schemaext.Facets, error) {
	if value == (ColumnSettings{}) {
		return facets, nil
	}
	declared := &DesiredColumnSettings{Charset: value.Charset, OnUpdate: value.OnUpdate}
	if err := ValidateDesiredColumnSettings(declared); err != nil {
		return schemaext.Facets{}, err
	}
	return withScoped(facets, declared)
}

// WithObservedColumnSettings returns facets with value added as an observation
// bound to [Targets], as a read of a MySQL or MariaDB server records it. An
// empty value returns facets unchanged; a collection that already holds the
// kind is refused.
func WithObservedColumnSettings(facets schemaext.Facets, value ColumnSettings) (schemaext.Facets, error) {
	if value == (ColumnSettings{}) {
		return facets, nil
	}
	observed := &ObservedColumnSettings{Charset: value.Charset, OnUpdate: value.OnUpdate}
	if err := ValidateObservedColumnSettings(observed); err != nil {
		return schemaext.Facets{}, err
	}
	return withScoped(facets, observed)
}

func withScoped(facets schemaext.Facets, value schemaext.Value) (schemaext.Facets, error) {
	result, err := facets.With(value)
	if err != nil {
		return schemaext.Facets{}, err
	}
	return result.WithTargetScope(ColumnSettingsKind, Targets()...)
}
