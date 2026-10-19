package descriptor

import (
	"fmt"
	"slices"
)

// Names states how a server resolves a name a schema file writes. Each field
// is a comparison spelled as core/platform/identifier spells its Comparison
// values: "exact", "ascii_case_insensitive" or "unicode_case_insensitive".
// The name is recorded under the comparison's identity key. Empty means
// exactly as written.
type Names struct {
	// Unquoted is the comparison an unquoted name is recorded under: the
	// server folds it where it records it, and a quoted name keeps every
	// character it was written with.
	Unquoted string `json:"unquoted,omitempty"`
	// Roles is the comparison every role name is recorded under, quoted or
	// not. Empty reads a role name like any other name.
	Roles string `json:"roles,omitempty"`
	// Lookup is the comparison a name a statement writes reaches a declared
	// name with when no declared name matches it exactly, and exactly one
	// matches under the comparison.
	Lookup string `json:"lookup,omitempty"`
	// PublicKeyword reports that PUBLIC as a grantee is a keyword for every
	// role rather than a role, in every spelling that reads as `public` as a
	// role name; a grantee spelled otherwise is a role. Without it, PUBLIC is
	// the keyword unquoted in any case and quoted in lower case.
	PublicKeyword bool `json:"public_keyword,omitempty"`
}

// NameComparisons returns the comparisons a [Names] field may spell.
func NameComparisons() []string {
	return []string{"exact", "ascii_case_insensitive", "unicode_case_insensitive"}
}

// IndexNaming states how a server names an index a schema file leaves
// unnamed: after its first key column, in the table's index namespace, with
// `_2` and on where the name is held.
type IndexNaming struct {
	// MaxBytes is the longest index name the server accepts. Zero means the
	// naming is not modeled.
	MaxBytes int `json:"max_bytes,omitempty"`
	// FunctionalBase is the name the server gives an index whose first key
	// part is an expression, such as "functional_index". Empty means the
	// server has no such index.
	FunctionalBase string `json:"functional_base,omitempty"`
	// DescendingCovers reports that a descending index can back a foreign
	// key, so the key builds no index of its own beside it.
	DescendingCovers bool `json:"descending_covers,omitempty"`
	// UniqueIsIndex reports that a UNIQUE constraint is an index in the
	// table's index namespace, so `DROP INDEX ix ON t` drops a UNIQUE
	// constraint named ix as `ALTER TABLE t DROP INDEX ix` does.
	UniqueIsIndex bool `json:"unique_is_index,omitempty"`
	// PrimaryKeySuffix names the index behind a primary key the file leaves
	// unnamed `<table>_<suffix>`, such as "pkey". Empty leaves it unnamed.
	PrimaryKeySuffix string `json:"primary_key_suffix,omitempty"`
}

// CheckNaming is how a server names a CHECK a schema file leaves unnamed.
type CheckNaming string

const (
	// CheckNamingTableNumbered names every unnamed CHECK `<table>_chk_<n>`,
	// numbered in statement order: MySQL's naming.
	CheckNamingTableNumbered CheckNaming = "table-numbered"
	// CheckNamingColumnOrConstraint names a CHECK written on a column after
	// the column, and one written on the table `CONSTRAINT_<n>`, the first
	// number the table's names leave free: MariaDB's naming.
	CheckNamingColumnOrConstraint CheckNaming = "column-or-constraint"
)

// ConstraintNaming is how a server names a constraint a schema file leaves
// unnamed.
type ConstraintNaming string

// ConstraintNamingTableColumnsSuffix names an unnamed constraint
// `<table>_<columns>_<suffix>`, with `1` and on where a relation or a
// constraint of the schema holds the name: `_check`, `_key` and `_fkey`.
// PostgreSQL's naming.
const ConstraintNamingTableColumnsSuffix ConstraintNaming = "table-columns-suffix"

// ForeignKeyNaming is how a server names a foreign key a schema file leaves
// unnamed, and the index it builds for the key.
type ForeignKeyNaming string

// ForeignKeyNamingTableNumbered names an unnamed foreign key
// `<table>_ibfk_<n>`, numbered by the statement that declares it, and builds
// the key an index of its own where no index covers the key, named after the
// key's first column by the rule [IndexNaming] states: the MySQL family's
// naming, as internal/mysqlname measures it.
const ForeignKeyNamingTableNumbered ForeignKeyNaming = "table-numbered"

// Columns states how a server reads a column definition.
type Columns struct {
	// IdentityNotNull reports that an identity column is NOT NULL whether or
	// not the definition says so.
	IdentityNotNull bool `json:"identity_not_null,omitempty"`
	// CheckOwnColumnOnly reports that a CHECK written on a column may name
	// that column alone, and one naming another column is refused.
	CheckOwnColumnOnly bool `json:"check_own_column_only,omitempty"`
	// AlterColumnKeepsRest reports that an ALTER of a column's definition
	// changes its type, nullability and collation and keeps the rest of the
	// column as it was.
	AlterColumnKeepsRest bool `json:"alter_column_keeps_rest,omitempty"`
}

// Statements states how a server reads statements a schema file writes about
// roles and tables.
type Statements struct {
	// CreateRoleOnce reports that the server refuses a CREATE ROLE for a role
	// the file already declared, wherever the first declaration was.
	CreateRoleOnce bool `json:"create_role_once,omitempty"`
	// ViewsBlockDropTable reports that a plain DROP TABLE is refused while a
	// view or a materialized view reads the table.
	ViewsBlockDropTable bool `json:"views_block_drop_table,omitempty"`
	// DoBlocks is the procedural language an anonymous DO block of a schema
	// file is written in where the file declares roles in one, such as
	// "plpgsql". Empty means the engine has no such block. A read with no
	// dialect reads a DO block as the first dialect that names a language.
	DoBlocks string `json:"do_blocks,omitempty"`
}

// TableOption is one option an engine's grammar writes after a CREATE TABLE.
type TableOption struct {
	// Option is the option as the parser reports it, such as "STRICT" or
	// "WITHOUT_ROWID".
	Option string `json:"option"`
	// Property is the property of the dialect's own group the option sets to
	// true, such as "strict".
	Property string `json:"property"`
}

// CheckNamings returns every declared value of [CheckNaming].
func CheckNamings() []CheckNaming {
	return []CheckNaming{CheckNamingTableNumbered, CheckNamingColumnOrConstraint}
}

// validateNames checks the enumerated values of the schema-file reading
// fields.
func (d Dialect) validateNames() error {
	for _, field := range []struct{ name, value string }{
		{"unquoted", d.Names.Unquoted}, {"roles", d.Names.Roles}, {"lookup", d.Names.Lookup},
	} {
		if field.value != "" && !slices.Contains(NameComparisons(), field.value) {
			return fmt.Errorf("%w: dialect %q: %s names comparison %q is not one of %q",
				ErrInvalidDescriptor, d.Name, field.name, field.value, NameComparisons())
		}
	}
	for _, option := range d.TableOptions {
		if option.Option == "" || option.Property == "" {
			return fmt.Errorf("%w: dialect %q: table option %+v names no option or no property", ErrInvalidDescriptor, d.Name, option)
		}
	}
	if d.IndexNaming.MaxBytes < 0 {
		return fmt.Errorf("%w: dialect %q: index naming max bytes %d is negative", ErrInvalidDescriptor, d.Name, d.IndexNaming.MaxBytes)
	}
	if naming := d.ConstraintNaming; naming != "" && naming != ConstraintNamingTableColumnsSuffix {
		return fmt.Errorf("%w: dialect %q: constraint naming %q is not %q", ErrInvalidDescriptor, d.Name, naming, ConstraintNamingTableColumnsSuffix)
	}
	if naming := d.CheckNaming; naming != "" && !slices.Contains(CheckNamings(), naming) {
		return fmt.Errorf("%w: dialect %q: check naming %q is not one of %q", ErrInvalidDescriptor, d.Name, naming, CheckNamings())
	}
	return d.validateForeignKeyNaming()
}

// validateForeignKeyNaming checks ForeignKeyNaming and the fields it depends
// on. A dialect that names a foreign key here does not name it again through
// ConstraintNaming, and the index it builds for the key is named by
// IndexNaming, so a reader would ignore half of a naming stated without it.
func (d Dialect) validateForeignKeyNaming() error {
	naming := d.ForeignKeyNaming
	if naming == "" {
		return nil
	}
	if naming != ForeignKeyNamingTableNumbered {
		return fmt.Errorf("%w: dialect %q: foreign key naming %q is not %q",
			ErrInvalidDescriptor, d.Name, naming, ForeignKeyNamingTableNumbered)
	}
	if d.ConstraintNaming != "" {
		return fmt.Errorf("%w: dialect %q: foreign key naming %q and constraint naming %q both name an unnamed foreign key",
			ErrInvalidDescriptor, d.Name, naming, d.ConstraintNaming)
	}
	if d.IndexNaming.MaxBytes == 0 {
		return fmt.Errorf("%w: dialect %q: foreign key naming %q names the index a key builds, and the index naming states no rule",
			ErrInvalidDescriptor, d.Name, naming)
	}
	return nil
}
