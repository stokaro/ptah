// Package sqlitetable owns what a SQLite table is created as: the options STRICT
// and WITHOUT ROWID, and the module declaration that makes a table virtual. It
// holds the desired and observed table facets with their codecs, the platform
// properties a declaration states the options as, and every stage's service,
// which engine/builtin registers on the sqlite target. No other target has
// either.
//
// Both options apply when a table is created. SQLite has no statement that
// turns either on or off for a table that exists, so the owner's comparison
// plans no change of an existing table's options, as no plan ever has; a
// table created from a declaration is created with them.
//
// A virtual table's declaration defines the table
// ([ptah.run/core/schemaext.TableDefinition]): SQLite has no ALTER VIRTUAL
// TABLE, so the owner refuses a changed declaration rather than plan the
// recreate that destroys the index.
package sqlitetable

import (
	"fmt"

	"ptah.run/core/schemaext"
)

// Owner is the provider identity under which the bundled runtime registers
// the SQLite models.
const Owner = "ptah.run/sqlite"

// TableKind identifies the options of one SQLite table:
// `CREATE TABLE ... STRICT` and `... WITHOUT ROWID`.
const TableKind schemaext.Kind = "ptah.run/sqlite/table"

// Options are the two table options. False is the option left out.
type Options struct {
	// Strict makes the table enforce its column types.
	Strict bool `json:"strict,omitempty"`
	// WithoutRowID stores the table in its primary key, with no rowid.
	WithoutRowID bool `json:"without_rowid,omitempty"`
}

// DesiredTable is the options a declaration states for one table.
type DesiredTable struct {
	Options
}

// ObservedTable is the options a read found a table created with.
type ObservedTable struct {
	Options
}

// Kind returns the owned table options identity.
func (*DesiredTable) Kind() schemaext.Kind { return TableKind }

// Kind returns the owned table options identity.
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

// Equal compares declarations option by option.
func (v *DesiredTable) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredTable)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return *v == *w
}

// Equal compares observations option by option.
func (v *ObservedTable) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedTable)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return *v == *w
}

// SQLiteRowidShape reports the options that decide whether SQLite holds a
// primary key column NOT NULL. internal/sqlitekey reads a table's facets
// through this method, so the comparison that normalizes key nullability does
// not depend on this package.
func (o Options) SQLiteRowidShape() (strict, withoutRowID bool) {
	return o.Strict, o.WithoutRowID
}

// Desired captures the observation as a declaration of the same options. A
// nil receiver remains nil.
func (v *ObservedTable) Desired() *DesiredTable {
	if v == nil {
		return nil
	}
	return &DesiredTable{Options: v.Options}
}

// Observed projects a declaration as the options a read of a table created
// from it reports. Nil is refused with schemaext.ErrInvalidValue.
func (v *DesiredTable) Observed() (*ObservedTable, error) {
	if err := ValidateDesiredTable(v); err != nil {
		return nil, err
	}
	return &ObservedTable{Options: v.Options}, nil
}

// ValidateDesiredTable refuses nil. Every combination of the two options is a
// table SQLite creates. Errors are schemaext.InvalidModelError values wrapping
// schemaext.ErrInvalidValue.
func ValidateDesiredTable(v *DesiredTable) error {
	if v == nil {
		return tableValidation(schemaext.Desired, fmt.Errorf("%w: nil SQLite table options declaration", schemaext.ErrInvalidValue))
	}
	return nil
}

// ValidateObservedTable refuses nil.
func ValidateObservedTable(v *ObservedTable) error {
	if v == nil {
		return tableValidation(schemaext.Observed, fmt.Errorf("%w: nil SQLite table options observation", schemaext.ErrInvalidValue))
	}
	return nil
}

func tableValidation(representation schemaext.Representation, err error) error {
	if err == nil {
		return nil
	}
	return &schemaext.InvalidModelError{Kind: TableKind, Representation: representation, Message: err.Error()}
}
