package descriptor

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"unicode"
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
	// TrailingSpacesIgnored reports that the server compares names with their
	// trailing spaces ignored, as SQL Server does: `[a ]` names `[a]`. A
	// leading space, and white space other than a trailing space, is part of
	// the name. A schema file's grantee is read without its trailing spaces,
	// and one with other white space around its name is refused by the
	// SQL Server schema and catalog readers.
	TrailingSpacesIgnored bool `json:"trailing_spaces_ignored,omitempty"`
	// RegclassConstants reports that a string constant the server converts to
	// regclass names a relation, which the server looks up as it reads the
	// statement: a constant cast to regclass, as in 'app.ids'::regclass, and
	// the first argument of nextval, currval and setval written as a constant,
	// as in nextval('app.ids'). A bare name is found along the session's
	// search path, and a statement that names a relation the database lacks is
	// refused. internal/pgregclass reads such constants.
	RegclassConstants bool `json:"regclass_constants,omitempty"`
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
	// BaseBytes is how much of the name an index is named after, in bytes,
	// the server keeps before the `_2` and on it appends where the name is
	// held, cut before a character the count would split. Zero keeps the
	// name whole.
	BaseBytes int `json:"base_bytes,omitempty"`
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
	// CheckNamingRequired is a server that refuses a CHECK without a name, so
	// Ptah names each table CHECK a schema file leaves unnamed the way it
	// names an entry of a table's checks list: `<table>_check`, then
	// `<table>_check1` and on, past the names the table holds. ClickHouse
	// answers `Code: 62 ... SYNTAX_ERROR` to an unnamed one.
	CheckNamingRequired CheckNaming = "required"
)

// CodePointRange is the code points from First to Last, both included.
type CodePointRange struct {
	First rune `json:"first"`
	Last  rune `json:"last"`
}

// CheckNames states where a server keeps the name of a CHECK unique, and how
// it refuses a CHECK whose name is taken there.
type CheckNames struct {
	// Scope is where two CHECK constraints may not share a name. Empty states
	// no scope, and no name is refused.
	Scope CheckNameScope `json:"scope,omitempty"`
	// Comparison is how two names in the scope are compared, spelled as a
	// [Names] field spells its comparison. Empty compares them exactly as the
	// model records them.
	Comparison string `json:"comparison,omitempty"`
	// Refusal is the first line of the error the dialect's driver reports when
	// the server refuses the second CHECK, with {name} where the server writes
	// that CHECK's name.
	Refusal string `json:"refusal,omitempty"`
	// TableRefusal is the first line of that error where both CHECKs belong to
	// one table, where it differs from Refusal. Empty means Refusal.
	TableRefusal string `json:"table_refusal,omitempty"`
	// FoldedRanges are the code points beyond ASCII whose simple lowercase the
	// server folds when it compares two names, under an ASCII comparison. A
	// letter outside them compares exactly, so a pair the server accepts is
	// never refused, and a pair it refuses outside them reaches the server.
	FoldedRanges []CodePointRange `json:"folded_ranges,omitempty"`
}

// CheckNameScope is where a server keeps the name of a CHECK unique.
type CheckNameScope string

const (
	// CheckNameScopeTable keeps one CHECK of a name per table.
	CheckNameScopeTable CheckNameScope = "table"
	// CheckNameScopeSchema keeps one CHECK of a name per schema, across its
	// tables.
	CheckNameScopeSchema CheckNameScope = "schema"
)

// CheckNameScopes returns every declared value of [CheckNameScope].
func CheckNameScopes() []CheckNameScope {
	return []CheckNameScope{CheckNameScopeTable, CheckNameScopeSchema}
}

// ConstraintNaming is how a server names a constraint a schema file leaves
// unnamed.
type ConstraintNaming string

// ConstraintNamingTableColumnsSuffix names an unnamed constraint
// `<table>_<columns>_<suffix>`, with `1` and on where a relation or a
// constraint of the schema holds the name: `_check`, `_key` and `_fkey`, and
// `_not_null` on a release that names a NOT NULL. PostgreSQL's naming.
const ConstraintNamingTableColumnsSuffix ConstraintNaming = "table-columns-suffix"

// ConstraintNamingServerHash is a server that names an unnamed constraint with a hash
// that differs from one database to the next, `UQ__t__3BD0198F3A610101`, so no file can
// name it later and a read cannot tell it from a declared one. A SQL schema file's unnamed
// UNIQUE on the table takes `<table>_<columns>_key` instead and its unnamed CHECK
// `<table>_<column>_check`, which the file then writes. SQL Server's naming.
const ConstraintNamingServerHash ConstraintNaming = "server-hash"

// ForeignKeyNaming is how a server names a foreign key a schema file leaves
// unnamed, and the index it builds for the key. [ForeignKeyNameRules] states
// where servers of one naming keep different names.
type ForeignKeyNaming string

// ForeignKeyNamingTableNumbered names an unnamed foreign key
// `<table>_ibfk_<n>`, numbered by the statement that declares it, and builds
// the key an index of its own where no index covers the key, named after the
// key's first column by the rule [IndexNaming] states: the MySQL family's
// naming, as internal/mysqlname measures it.
const ForeignKeyNamingTableNumbered ForeignKeyNaming = "table-numbered"

// ForeignKeyNameRules states where servers that name an unnamed foreign key
// by the same [ForeignKeyNaming] keep different names for it. The zero value
// keeps every derived name up to the dialect's IdentifierLimit and reads only
// the names the naming gives.
type ForeignKeyNameRules struct {
	// CreateTableMaxChars is the longest derived name, in characters, that
	// CREATE TABLE keeps for an unnamed key. Zero means the dialect's
	// IdentifierLimit, which is the longest name ALTER TABLE keeps.
	CreateTableMaxChars int `json:"create_table_max_chars,omitempty"`
	// BareNumbers reports that some release lines of the server name an
	// unnamed key by its number alone, `<n>`, where the naming gives
	// `<table>_ibfk_<n>`. A comparison reads a database key named so as the
	// key a schema file left unnamed.
	BareNumbers bool `json:"bare_numbers,omitempty"`
}

// Columns states how a server reads a column definition.
type Columns struct {
	// NationalCharAsChar reports that NCHAR reads back as CHAR. SQL Server
	// and Oracle keep a distinct national character type.
	NationalCharAsChar bool `json:"national_char_as_char,omitempty"`
	// NationalCharCharset is the character set NCHAR implies where a CHAR
	// column may have another character set. Empty means no separate set.
	NationalCharCharset string `json:"national_char_charset,omitempty"`
	// IdentityNotNull reports that an identity column is NOT NULL whether or
	// not the definition says so.
	IdentityNotNull bool `json:"identity_not_null,omitempty"`
	// AutoIncrementNotNull reports that a column declared auto-increment, by
	// its flag, an identity generation or a type that names a counter, is NOT
	// NULL whether or not the definition says so, as Oracle builds each as an
	// identity column. It
	// differs from IdentityNotNull where the target writes an auto-increment
	// flag as something other than an identity column, or not at all.
	AutoIncrementNotNull bool `json:"auto_increment_not_null,omitempty"`
	// SerialNotNull reports that a column of a serial type, SERIAL, BIGSERIAL
	// or SMALLSERIAL, is NOT NULL whether or not the definition says so, as
	// PostgreSQL makes it.
	SerialNotNull bool `json:"serial_not_null,omitempty"`
	// CheckOwnColumnOnly reports that a CHECK written on a column may name
	// that column alone, and one naming another column is refused.
	CheckOwnColumnOnly bool `json:"check_own_column_only,omitempty"`
	// AlterColumnKeepsRest reports that an ALTER of a column's definition
	// changes its type, nullability and collation and keeps the rest of the
	// column as it was.
	AlterColumnKeepsRest bool `json:"alter_column_keeps_rest,omitempty"`
	// Collations reports that a column's own collation is written on the
	// column and compared with the one the catalog reports the column
	// declaring. A column that declares none takes its type's collation, so
	// one the catalog reports declaring a collation is changed back, unless
	// InheritedCollationVaries holds. Names compare exactly where
	// Grammar.Columns.QualifiedCollations holds, and ignoring ASCII case
	// elsewhere.
	Collations bool `json:"collations,omitempty"`
	// InheritedCollation is the collation every column that declares none
	// compares with, whatever its type: BINARY on SQLite. A declaration
	// naming it holds against a column that declares none. Empty means the
	// type decides, and the catalog reports the collation the column takes.
	// Where InheritedCollationVaries holds too, as on Oracle, it is the
	// collation of a column the read reports none for, and a declaration
	// naming it holds against that column.
	InheritedCollation string `json:"inherited_collation,omitempty"`
	// InheritedCollationVaries reports that a column declaring no collation
	// takes one the server picks: its table's default, or the default
	// collation of a character set the column names, as MySQL and MariaDB
	// do. Only the server knows which that is, so the comparison compares a
	// collation only where the column declares one, against the collation
	// the column holds.
	InheritedCollationVaries bool `json:"inherited_collation_varies,omitempty"`
	// CollationNames states how the server resolves a collation name a column
	// declares where InheritedCollationVaries holds. The zero value resolves
	// every name to itself.
	CollationNames CollationNames `json:"collation_names,omitzero"`
	// FunctionAliases maps, in lower case, a function name the server takes
	// in place of another to that other name. A call to either name in an
	// expression default is compared as a call to the second, so neither
	// spelling plans the column again. MySQL and MariaDB take ucase and lcase
	// for upper and lower, and MariaDB reports a default calling upper as
	// ucase.
	FunctionAliases map[string]string `json:"function_aliases,omitempty"`
}

// CollationNames states the collation names a server stores under another
// name, or keeps no collation for. A column that declares such a name holds
// the name the server stores, and a comparison that read the name as written
// would restate the column on every run.
type CollationNames struct {
	// CharsetAliases maps a character set name a definition may write to the
	// name the server stores it under, in lower case: utf8 to utf8mb3 on MySQL
	// 8.4 and MariaDB 11.8. A collation named after the alias is stored under
	// the other name, so `COLLATE utf8_bin` holds as utf8mb3_bin.
	CharsetAliases map[string]string `json:"charset_aliases,omitempty"`
	// CharsetDefaults maps each character set name the server stores, in
	// lower case, to its default collation on the newest release line the
	// dialect is tested on, with no session setting changing it. A
	// prediction with no server to ask reads it, as a diff of two schema
	// files with no dev database does; a server's own read takes its place
	// wherever there is one, because a session setting or an older release
	// line selects another default.
	CharsetDefaults map[string]string `json:"charset_defaults,omitempty"`
	// WithoutCharset reports that a collation may be named without its
	// character set and takes the column's: `COLLATE uca1400_ai_ci` on a
	// utf8mb4 column of MariaDB 11.8 holds as utf8mb4_uca1400_ai_ci.
	WithoutCharset bool `json:"without_charset,omitempty"`
	// ListedWithoutCharset are the name prefixes, in lower case, of the
	// collations information_schema.COLLATIONS lists once with no character
	// set rather than once for each character set: MariaDB 10.11 to 12.3 list
	// `uca1400_ai_ci` and no `utf8mb4_uca1400_ai_ci`. A catalog read that
	// names a table's character set through that table finds none for a
	// collation of such a family. A dialect whose URL scheme may name a server
	// of another dialect declares that server's families too.
	ListedWithoutCharset []string `json:"listed_without_charset,omitempty"`
	// CharsetDefault is the name that selects the default collation of the
	// column's character set, `DEFAULT` on MariaDB 11.8. A column that
	// declares it is compared as one that declares none.
	CharsetDefault string `json:"charset_default,omitempty"`
	// Types are the type names, in lower case and without a size, whose
	// columns hold a collation. The server accepts a collation on another
	// type and keeps none, as MySQL 8.4 does for `int COLLATE utf8mb4_bin`,
	// so a collation declared there is not compared.
	Types []string `json:"types,omitempty"`
}

// Statements states how a server reads statements a schema file writes about
// roles, tables and the objects a feature owner declares.
type Statements struct {
	// CreateRoleOnce reports that the server refuses a CREATE ROLE for a role
	// the file already declared, wherever the first declaration was.
	CreateRoleOnce bool `json:"create_role_once,omitempty"`
	// OwnerCreateOnce reports that the server refuses a feature owner's
	// CREATE of an object the file already created when the statement is
	// written without IF NOT EXISTS, as PostgreSQL refuses a second CREATE
	// EXTENSION with `extension "pgcrypto" already exists`. It covers the
	// objects whose owner reports whether their statement carried the
	// guard. False means the server accepts such a statement, as
	// CockroachDB accepts CREATE EXTENSION of an extension it has.
	OwnerCreateOnce bool `json:"owner_create_once,omitempty"`
	// ViewsBlockDropTable reports that a plain DROP TABLE is refused while a
	// view or a materialized view reads the table.
	ViewsBlockDropTable bool `json:"views_block_drop_table,omitempty"`
	// DropTableReachesViews reports that DROP TABLE also removes a view or a
	// materialized view holding the name.
	DropTableReachesViews bool `json:"drop_table_reaches_views,omitempty"`
	// DropViewIfExistsSkipsOtherKinds reports that DROP VIEW IF EXISTS does nothing
	// when another kind of object holds the name. Without it the server refuses the
	// statement, as it refuses one with no IF EXISTS.
	DropViewIfExistsSkipsOtherKinds bool `json:"drop_view_if_exists_skips_other_kinds,omitempty"`
	// RelationsShareNames reports that tables, views, materialized views and
	// sequences take their names from one namespace per schema, and that a
	// CREATE TABLE IF NOT EXISTS or a CREATE SEQUENCE IF NOT EXISTS is skipped
	// when the schema holds a relation of any of those kinds under its name,
	// not only one of its own kind.
	RelationsShareNames bool `json:"relations_share_names,omitempty"`
	// DoBlocks is the procedural language an anonymous DO block of a schema
	// file is written in where the file declares roles in one, one of
	// [DoBlockLanguages]. Empty means the engine has no such block. A read
	// with no dialect reads a DO block as the first dialect that names a
	// language.
	DoBlocks string `json:"do_blocks,omitempty"`
}

// DoBlocksPLpgSQL is PL/pgSQL, the language of PostgreSQL's procedural
// blocks, as [Statements.DoBlocks] and [RoutineGrammar.DefaultDoBlockLanguage]
// name it.
const DoBlocksPLpgSQL = "plpgsql"

// DoBlockLanguages returns every declared [Statements.DoBlocks] value.
func DoBlockLanguages() []string {
	return []string{DoBlocksPLpgSQL}
}

// TableOption is one option an engine's grammar writes after a CREATE TABLE.
type TableOption struct {
	// Option is the option as the parser reports it, such as "STRICT" or
	// "WITHOUT_ROWID".
	Option string `json:"option"`
	// Words are the words that spell the option, in order and without a
	// value, such as WITHOUT ROWID. The parser reads them in any case as the
	// option set to true, among the table options of every dialect, because
	// a file is also parsed with no dialect. A word the shared table-option
	// grammar reads itself, such as ENGINE or WITH, is read as that
	// grammar's. Empty means another grammar reports the option, such as a
	// storage parameter of a WITH clause.
	Words []string `json:"words,omitempty"`
	// Property is the property of the dialect's own group the option sets to
	// true, such as "strict".
	Property string `json:"property"`
}

// CheckNamings returns every declared value of [CheckNaming].
func CheckNamings() []CheckNaming {
	return []CheckNaming{CheckNamingTableNumbered, CheckNamingColumnOrConstraint, CheckNamingRequired}
}

// validateFunctionAliases refuses a function alias that is not two different
// lower-case names. The aliases are read in name order, so the first refused
// one is the same on every run.
func (d Dialect) validateFunctionAliases() error {
	for _, alias := range slices.Sorted(maps.Keys(d.Columns.FunctionAliases)) {
		name := d.Columns.FunctionAliases[alias]
		if alias == "" || name == "" || alias == name || alias != strings.ToLower(alias) || name != strings.ToLower(name) {
			return fmt.Errorf("%w: dialect %q: function alias %q for %q is not two different lower-case names",
				ErrInvalidDescriptor, d.Name, alias, name)
		}
	}
	return nil
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
	if language := d.Statements.DoBlocks; language != "" && !slices.Contains(DoBlockLanguages(), language) {
		return fmt.Errorf("%w: dialect %q: DO-block language %q is not one of %q",
			ErrInvalidDescriptor, d.Name, language, DoBlockLanguages())
	}
	if err := d.validateTableOptions(); err != nil {
		return err
	}
	if err := d.validateFunctionAliases(); err != nil {
		return err
	}
	if d.IndexNaming.MaxBytes < 0 {
		return fmt.Errorf("%w: dialect %q: index naming max bytes %d is negative", ErrInvalidDescriptor, d.Name, d.IndexNaming.MaxBytes)
	}
	if err := d.validateIndexBaseBytes(); err != nil {
		return err
	}
	if naming := d.ConstraintNaming; naming != "" && naming != ConstraintNamingTableColumnsSuffix && naming != ConstraintNamingServerHash {
		return fmt.Errorf("%w: dialect %q: constraint naming %q is neither %q nor %q",
			ErrInvalidDescriptor, d.Name, naming, ConstraintNamingTableColumnsSuffix, ConstraintNamingServerHash)
	}
	if naming := d.CheckNaming; naming != "" && !slices.Contains(CheckNamings(), naming) {
		return fmt.Errorf("%w: dialect %q: check naming %q is not one of %q", ErrInvalidDescriptor, d.Name, naming, CheckNamings())
	}
	if d.CheckNaming == CheckNamingTableNumbered && !d.IdentifierLimit.countsCharacters() {
		return fmt.Errorf("%w: dialect %q: check naming %q refuses a derived name longer than the identifier limit "+
			"in characters, and the identifier limit states no length in characters",
			ErrInvalidDescriptor, d.Name, d.CheckNaming)
	}
	if err := d.validateCheckNames(); err != nil {
		return err
	}
	return d.validateForeignKeyNaming()
}

// validateCheckNames refuses a CHECK name scope that is not a declared one, a
// comparison that is not one [Names] spells, and a scope without the refusal
// it answers, or a refusal without a scope to give it.
func (d Dialect) validateCheckNames() error {
	names := d.CheckNames
	if names.Scope != "" && !slices.Contains(CheckNameScopes(), names.Scope) {
		return fmt.Errorf("%w: dialect %q: CHECK name scope %q is not one of %q",
			ErrInvalidDescriptor, d.Name, names.Scope, CheckNameScopes())
	}
	if names.Comparison != "" && !slices.Contains(NameComparisons(), names.Comparison) {
		return fmt.Errorf("%w: dialect %q: CHECK names comparison %q is not one of %q",
			ErrInvalidDescriptor, d.Name, names.Comparison, NameComparisons())
	}
	if (names.Scope == "") != (names.Refusal == "") {
		return fmt.Errorf("%w: dialect %q: CHECK names state a scope and the refusal it answers together, or neither",
			ErrInvalidDescriptor, d.Name)
	}
	for _, folded := range names.FoldedRanges {
		if folded.First < 0x80 || folded.Last < folded.First {
			return fmt.Errorf("%w: dialect %q: CHECK names fold the code points %U to %U, which are not a range beyond ASCII",
				ErrInvalidDescriptor, d.Name, folded.First, folded.Last)
		}
	}
	if names.Scope == "" && (names.Comparison != "" || names.TableRefusal != "" || len(names.FoldedRanges) != 0) {
		return fmt.Errorf("%w: dialect %q: CHECK names state a comparison or a table refusal without a scope",
			ErrInvalidDescriptor, d.Name)
	}
	return nil
}

// validateTableOptions checks TableOptions: each names an option and a
// property, and each word that spells one is a single word.
func (d Dialect) validateTableOptions() error {
	for _, option := range d.TableOptions {
		if option.Option == "" || option.Property == "" {
			return fmt.Errorf("%w: dialect %q: table option {Option:%s Property:%s} names no option or no property",
				ErrInvalidDescriptor, d.Name, option.Option, option.Property)
		}
		for _, word := range option.Words {
			if word == "" || strings.ContainsFunc(word, unicode.IsSpace) {
				return fmt.Errorf("%w: dialect %q: table option %q is spelled with %q, which is not one word",
					ErrInvalidDescriptor, d.Name, option.Option, word)
			}
		}
	}
	return nil
}

// validateIndexBaseBytes checks IndexNaming.BaseBytes. A cut name needs room
// for the suffix the server appends to it, so the cut stays below the longest
// index name.
func (d Dialect) validateIndexBaseBytes() error {
	naming := d.IndexNaming
	switch {
	case naming.BaseBytes < 0:
		return fmt.Errorf("%w: dialect %q: index naming base bytes %d is negative", ErrInvalidDescriptor, d.Name, naming.BaseBytes)
	case naming.BaseBytes > 0 && naming.BaseBytes >= naming.MaxBytes:
		return fmt.Errorf("%w: dialect %q: index naming base bytes %d leaves no room for a suffix within max bytes %d",
			ErrInvalidDescriptor, d.Name, naming.BaseBytes, naming.MaxBytes)
	}
	return nil
}

// countsCharacters reports whether the limit states a length in characters.
// The unit is spelled as core/platform/capability spells IdentifierCharacters.
func (l IdentifierLimit) countsCharacters() bool {
	return l.Max > 0 && l.Unit == "characters"
}

// validateForeignKeyNaming checks ForeignKeyNaming and the fields it depends
// on. A dialect that names a foreign key here does not name it again through
// ConstraintNaming, and the index it builds for the key is named by
// IndexNaming, so a reader would ignore half of a naming stated without it.
// The longest derived name is counted in characters against the
// IdentifierLimit, and ForeignKeyNameRules refine this naming alone.
func (d Dialect) validateForeignKeyNaming() error {
	naming := d.ForeignKeyNaming
	if naming == "" {
		if d.ForeignKeyNameRules != (ForeignKeyNameRules{}) {
			return fmt.Errorf("%w: dialect %q: foreign key name rules are stated, and no foreign key naming is",
				ErrInvalidDescriptor, d.Name)
		}
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
	if !d.IdentifierLimit.countsCharacters() {
		return fmt.Errorf("%w: dialect %q: foreign key naming %q refuses a derived name longer than the identifier limit "+
			"in characters, and the identifier limit states no length in characters",
			ErrInvalidDescriptor, d.Name, naming)
	}
	if limit := d.ForeignKeyNameRules.CreateTableMaxChars; limit < 0 || limit > d.IdentifierLimit.Max {
		return fmt.Errorf("%w: dialect %q: foreign key name rules: CREATE TABLE max chars %d is not between 0 and the identifier limit %d",
			ErrInvalidDescriptor, d.Name, limit, d.IdentifierLimit.Max)
	}
	return nil
}
