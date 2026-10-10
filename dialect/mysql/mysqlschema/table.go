package mysqlschema

import (
	"fmt"
	"strings"
	"unicode"

	"ptah.run/core/schemaext"
)

// Owner is the provider identity under which the bundled runtime registers the
// MySQL-family models.
const Owner = "ptah.run/mysql"

// TableKind identifies the options a MySQL or MariaDB table is created with:
// `CREATE TABLE ... ENGINE=... AUTO_INCREMENT=... CHARSET=...`. They apply when
// the table is created. A read reports the character set; the engine and the
// next auto-increment value are not read, so no plan changes the options of
// a table that exists.
const TableKind schemaext.Kind = "ptah.run/mysql/table"

// DesiredTable is the options a declaration states for one table. An empty
// field is an option the declaration leaves out, which the server's default
// then decides.
type DesiredTable struct {
	// Engine is the storage engine, such as InnoDB.
	Engine string `json:"engine,omitempty"`
	// AutoIncrement is the first value the table's auto-increment column
	// takes, as a decimal number.
	AutoIncrement string `json:"auto_increment,omitempty"`
	// Charset is the table's default character set, such as utf8mb4.
	Charset string `json:"charset,omitempty"`
}

// ObservedTable is the options a table created from a declaration holds as a
// read would report them: its default character set, which the server reports
// as part of its collation. Empty is a table whose collation names none. The
// reader keeps the character set it finds on the catalog table, so an
// observation comes from converting a declaration, as a file compared with a
// file is.
type ObservedTable struct {
	Charset string `json:"charset,omitempty"`
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

// Equal compares declarations option by option, as written.
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

// Desired captures the observation as a declaration that keeps it. A nil
// receiver remains nil.
func (v *ObservedTable) Desired() *DesiredTable {
	if v == nil {
		return nil
	}
	return &DesiredTable{Charset: v.Charset}
}

// Observed projects a declaration as the options a read of a table created
// from it reports: its character set. Nil and invalid declarations are
// refused with schemaext.ErrInvalidValue.
func (v *DesiredTable) Observed() (*ObservedTable, error) {
	if err := ValidateDesiredTable(v); err != nil {
		return nil, err
	}
	return &ObservedTable{Charset: v.Charset}, nil
}

// ValidateDesiredTable refuses options no statement can write: an engine or a
// character set that is not a name of letters, digits and underscores, and an
// auto-increment value that is not a whole decimal number. Nil is invalid.
// Errors are schemaext.InvalidModelError values wrapping
// schemaext.ErrInvalidValue.
func ValidateDesiredTable(v *DesiredTable) error {
	if v == nil {
		return tableValidation(schemaext.Desired, fmt.Errorf("%w: nil MySQL table options declaration", schemaext.ErrInvalidValue))
	}
	for _, option := range []struct{ name, value string }{{"engine", v.Engine}, {"charset", v.Charset}} {
		if err := validName(option.name, option.value); err != nil {
			return tableValidation(schemaext.Desired, err)
		}
	}
	if v.AutoIncrement != "" && strings.TrimFunc(v.AutoIncrement, unicode.IsDigit) != "" {
		return tableValidation(schemaext.Desired, fmt.Errorf("%w: MySQL table auto_increment %q is not a whole decimal number",
			schemaext.ErrInvalidValue, v.AutoIncrement))
	}
	return nil
}

// ValidateObservedTable refuses a character set that is not a name. Nil is
// invalid.
func ValidateObservedTable(v *ObservedTable) error {
	if v == nil {
		return tableValidation(schemaext.Observed, fmt.Errorf("%w: nil MySQL table options observation", schemaext.ErrInvalidValue))
	}
	return tableValidation(schemaext.Observed, validName("charset", v.Charset))
}

// validName accepts empty, and otherwise a name of ASCII letters, digits and
// underscores, the spelling engines and character sets have.
func validName(option, value string) error {
	for _, r := range value {
		if r != '_' && (r > unicode.MaxASCII || (!unicode.IsLetter(r) && !unicode.IsDigit(r))) {
			return fmt.Errorf("%w: MySQL %s %q is not a name of letters, digits and underscores", schemaext.ErrInvalidValue, option, value)
		}
	}
	return nil
}

func tableValidation(representation schemaext.Representation, err error) error {
	if err == nil {
		return nil
	}
	return &schemaext.InvalidModelError{Kind: TableKind, Representation: representation, Message: err.Error()}
}
