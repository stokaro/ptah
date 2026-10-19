// Package descriptor declares what a database engine states about itself as
// data: its names, its protocol family, how it compares identifiers and how a
// query marks a bound parameter. Shared packages read a [Dialect] instead of
// switching on a dialect's name, so an engine can be added by describing it
// rather than by editing every package that branches on the name.
//
// A [Dialect] holds data only. It has no functions, interfaces or channels, so
// it can cross a process boundary, and a [Registry] can be built from
// descriptors another process sent. What an engine does differently, rather
// than states differently, belongs to its provider's services, not here.
//
// A [Registry] is an immutable, validated set of descriptors. The registry
// every package-level function of core/platform reads is the process
// registry: core/platform seeds it with the built-in engines and extends it
// through platform.Register. Code that needs a set of its own, such as a test
// of an engine Ptah does not ship, builds one with [NewRegistry].
//
// This package checks the parts of a descriptor every reader shares: names,
// aliases, family, the placeholder style, the enumerated schema, role and lock
// values of [Schemas], [Roles] and [Lock], the names a dialect claims in a URL
// through [URLs] and [Atlas], and the values of [Grammar]. A package that
// interprets a field checks that field's values when it reads them; see
// core/platform/identifier.FromDescriptor.
package descriptor

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
)

// ErrInvalidDescriptor identifies a descriptor whose content is malformed:
// an empty or badly spelled name, an alias that repeats a spelling, a family
// that names no family head, or an undeclared value of an enumerated field.
var ErrInvalidDescriptor = errors.New("invalid dialect descriptor")

// ErrConflict identifies a descriptor that claims a spelling, a file scheme,
// or a docker image or driver another, different descriptor of the same
// registry already owns.
var ErrConflict = errors.New("conflicting dialect descriptor")

// Dialect is the description one database engine gives of itself. The zero
// value is not a valid descriptor: Name and Placeholder are required.
//
// Every field is data. A field added here is added to the built-in
// descriptors and read by the package it is for; a field that would hold
// behavior is a provider service instead.
type Dialect struct {
	// Name is the canonical name every other spelling folds onto, such as
	// "postgres". It uses lowercase ASCII letters, digits, '-', '_' and '+',
	// and begins with a letter.
	Name string `json:"name"`
	// Aliases are the other spellings that select this dialect, in the same
	// alphabet as Name: a driver's name, a URL scheme, a transport spelling
	// such as "mysql+unix". A spelling belongs to one dialect of a registry.
	Aliases []string `json:"aliases,omitempty"`
	// Family names the dialect whose wire protocol and catalog this one
	// speaks, such as "postgres" for CockroachDB. The head of a family names
	// itself; a dialect that shares its protocol with no other leaves it
	// empty. Membership answers which reader and renderer serve a dialect. It
	// says nothing about features: CockroachDB is in the PostgreSQL family and
	// refuses constructs PostgreSQL accepts.
	Family string `json:"family,omitempty"`
	// DefaultSchema is the schema an unqualified name resolves to offline,
	// such as "public". Empty means the connection decides, or the engine has
	// no schemas.
	DefaultSchema string `json:"default_schema,omitempty"`
	// Identifiers states how the engine compares the names of indexes,
	// tables and columns.
	Identifiers Identifiers `json:"identifiers"`
	// Placeholder is how a statement marks a bound parameter.
	Placeholder Placeholder `json:"placeholder"`

	// Capabilities states how a capability preset is chosen for the dialect,
	// offline and from a server's version string.
	Capabilities Capabilities `json:"capabilities,omitzero"`
	// IdentifierLimit is the longest identifier the engine accepts.
	IdentifierLimit IdentifierLimit `json:"identifier_limit,omitzero"`
	// GrantOptionPerObject reports that a WITH GRANT OPTION grant belongs to
	// the grantee at the whole object rather than to one privilege, so
	// revoking the option from one privilege revokes it from all of them.
	GrantOptionPerObject bool `json:"grant_option_per_object,omitempty"`

	// Driver is the database/sql driver a connection to the dialect opens,
	// such as "pgx". A connection may take another driver where the URL asks
	// for another transport: a libsql:// URL selects SQLite and reaches its
	// server through the libsql driver. Empty means Ptah does not connect to
	// the dialect. It uses the alphabet of Name.
	Driver string `json:"driver,omitempty"`
	// WireScheme is the URL scheme the driver reads in place of the dialect's
	// own, such as "postgres" for CockroachDB, whose server speaks the
	// PostgreSQL protocol to a driver that does not know its name. Empty keeps
	// the scheme the URL carries. It uses the alphabet of Name.
	WireScheme string `json:"wire_scheme,omitempty"`
	// URLs states how a database URL of the dialect is read and compared
	// before anything is connected; see urls.go.
	URLs URLs `json:"urls,omitzero"`

	// Query states how the query builder spells what engines spell
	// differently.
	Query Query `json:"query,omitzero"`
	// Scripts states how a script of several statements is cut into them.
	Scripts Scripts `json:"scripts,omitzero"`

	// ConcurrentIndexes reports that the engine's grammar builds and drops an
	// index without blocking writes, as PostgreSQL's CREATE INDEX CONCURRENTLY
	// and DROP INDEX CONCURRENTLY do. Whether a given server takes the
	// statements is a capability; an engine without the grammar has no such
	// statement whatever a capability set claims.
	ConcurrentIndexes bool `json:"concurrent_indexes,omitempty"`
	// OnlineAlter is the grammar in which the engine is asked to apply an
	// ALTER TABLE without blocking the writes running against the table:
	// [OnlineAlterAlgorithmLock] or [OnlineAlterNotValid]. Empty has none.
	OnlineAlter OnlineAlter `json:"online_alter,omitempty"`

	// Migrations: what the migrator reads.

	// DDLTransactions is the transaction contract the migrator implements for
	// the engine's schema statements, spelled as internal/ddltx spells its
	// classes: "transactional", "implicit_commit" or "no_transaction". Empty
	// means nobody decided, which internal/ddltx reports as unclassified.
	DDLTransactions string `json:"ddl_transactions,omitempty"`
	// RevisionTable states how the migrator keeps its own tables.
	RevisionTable RevisionTable `json:"revision_table,omitzero"`

	// Schema scope, locking and URL display; see scope.go.

	// Schemas states what a schema is on the engine and how a database URL
	// selects one.
	Schemas Schemas `json:"schemas,omitzero"`
	// SystemSchemas states the schemas a PostgreSQL-family engine owns
	// beyond the ones every member of the family owns, as
	// internal/pgsystemschema reads them; see systemschemas.go.
	SystemSchemas SystemSchemas `json:"system_schemas,omitzero"`
	// Roles states which of the roles Ptah manages a schema read leaves out of
	// a description, and whether that can be widened.
	Roles Roles `json:"roles,omitzero"`
	// Lock states how the engine holds the advisory lock that serializes
	// schema-changing runs.
	Lock Lock `json:"lock,omitzero"`
	// URLValuedParameters are the query parameters of a database URL whose
	// value is a URL of its own, such as the monitoring endpoint of a YDB
	// cluster. A display of the URL hides the credentials inside them too.
	// They are matched without regard to case.
	URLValuedParameters []string `json:"url_valued_parameters,omitempty"`
	// Literals states how a value is written into a statement as a literal.
	Literals Literals `json:"literals,omitzero"`
	// ManagedData states how managed rows are compared and changed.
	ManagedData ManagedData `json:"managed_data,omitzero"`
	// Seeds states how seed files run and are recorded.
	Seeds Seeds `json:"seeds,omitzero"`

	// Identifier spelling and key naming; see identifiers.go.

	// Quoting states how the engine writes an identifier.
	Quoting Quoting `json:"quoting,omitzero"`
	// ColumnKeys is how the server names the key a column's own UNIQUE
	// builds. Empty means the naming is not measured.
	ColumnKeys ColumnKeyNaming `json:"column_keys,omitempty"`

	// Cleanup: what internal/schemaclean reads.

	// Cleanup states what a development database cleanup destroys and how it
	// spells the statements.
	Cleanup Cleanup `json:"cleanup,omitzero"`

	// Lint: what migration lint reads.

	// Lint states how migration lint reads the dialect's migrations.
	Lint Lint `json:"lint,omitzero"`

	// The Atlas contract: what ptah-compat reads; see atlas.go.

	// Atlas states where the engine stands in the Atlas CLI contract: the
	// names an Atlas dev URL gives it and the parts of an Atlas HCL schema
	// that belong to it alone.
	Atlas Atlas `json:"atlas,omitzero"`

	// Reading a schema file; see names.go.

	// Names states how the server resolves a name a schema file writes.
	Names Names `json:"names,omitzero"`
	// IndexNaming states how the server names an index a schema file leaves
	// unnamed. The zero value means the naming is not modeled.
	IndexNaming IndexNaming `json:"index_naming,omitzero"`
	// CheckNaming is how the server names a CHECK a schema file leaves
	// unnamed. Empty keeps the name the rest of Ptah derives.
	CheckNaming CheckNaming `json:"check_naming,omitempty"`
	// ConstraintNaming is how the server names a CHECK, UNIQUE or FOREIGN
	// KEY a schema file leaves unnamed. Empty keeps the name the rest of
	// Ptah derives.
	ConstraintNaming ConstraintNaming `json:"constraint_naming,omitempty"`
	// Columns states how the server reads a column definition.
	Columns Columns `json:"columns,omitzero"`
	// Statements states how the server reads statements a schema file
	// writes about roles and tables.
	Statements Statements `json:"statements,omitzero"`
	// TableOptions are the options the engine's grammar writes after a
	// CREATE TABLE, such as SQLite's STRICT, each with the property of the
	// dialect's own group it sets on the table.
	TableOptions []TableOption `json:"table_options,omitempty"`

	// Dev databases.

	// DevRealm states how Ptah treats a disposable database on this engine:
	// how it names one, keeps two runs from using one at once, gives a test
	// case one of its own and resets one. The zero value names none, and a
	// command that replays migrations into a dev database refuses one.
	DevRealm DevRealm `json:"dev_realm,omitzero"`
	// SchemaIsDatabase reports that a schema is a database: the namespace an
	// unqualified name resolves in is the database the connection selected,
	// read from the URL, rather than a rule of the engine. Two connections to
	// two databases of one server therefore resolve names in two places.
	SchemaIsDatabase bool `json:"schema_is_database,omitempty"`

	// Lowering: how the common model is lowered for the engine.

	// GeneratedTimeouts are the lock and statement timeouts a generated
	// migration file states when one of its statements waits for a lock on a
	// table. Empty states none.
	GeneratedTimeouts GeneratedTimeouts `json:"generated_timeouts,omitzero"`
	// GeneratedColumnDefault is the persistence a generated column is created
	// with when its declaration names none, such as "STORED", "VIRTUAL" or
	// "PERSISTED". Empty creates the column as the declaration states it.
	GeneratedColumnDefault string `json:"generated_column_default,omitempty"`
	// InlineEnum is how a column of an enum is written on an engine that
	// keeps the values on the column rather than in a type of their own.
	// Zero writes the column as declared.
	InlineEnum InlineEnum `json:"inline_enum,omitzero"`
	// ForeignKeysInCreateTable reports that a foreign key is declared inside
	// its table's CREATE TABLE and never added to an existing table, as on
	// SQLite, which has no ALTER TABLE ... ADD CONSTRAINT.
	ForeignKeysInCreateTable bool `json:"foreign_keys_in_create_table,omitempty"`
	// IndexesBeforeForeignKeys reports that every index a schema declares is
	// created before the schema's foreign keys are added, because the server
	// builds an index of its own for a foreign key no declared index covers, as
	// the MySQL family does.
	IndexesBeforeForeignKeys bool `json:"indexes_before_foreign_keys,omitempty"`
	// Lexical is how the engine's SQL text differs from the standard: how
	// the shared SQL lexer tokenizes it and how the scanner of routine and
	// view bodies reads it.
	Lexical Lexical `json:"lexical,omitzero"`
	// Comparison: what migration/schemadiff and its helpers read.

	// Comparison states facts about the engine that comparing a declaration
	// with a catalog depends on.
	Comparison Comparison `json:"comparison,omitzero"`

	// Preparation states how a declared schema is prepared for the engine
	// before it is rendered or compared.
	Preparation Preparation `json:"preparation,omitzero"`

	// Conversion states what the readers and converters of schema documents
	// read of the engine where engines differ.
	Conversion Conversion `json:"conversion,omitzero"`

	// Reach states the calls of the engine's grammar that reach outside the
	// database a statement is sent to; see reach.go.
	Reach Reach `json:"reach,omitzero"`

	// Grammar states where the engine's DDL grammar differs, as the shared
	// DDL parser reads it; see grammar.go.
	Grammar Grammar `json:"grammar,omitzero"`

	// Clean checks, schema attributes and plan qualifiers: what
	// internal/migrateclean, internal/atlasschema and internal/atlasmigrate
	// read.

	// CleanCheck states how Ptah tells whether a database holds anything
	// before it adopts the database or resets it as a dev database; see
	// cleancheck.go.
	CleanCheck CleanCheck `json:"clean_check,omitzero"`
	// SchemaAttributesQuery is the catalog query that selects the default
	// character set and the default collation of one schema, binding the
	// schema's name. Empty means a schema carries neither.
	SchemaAttributesQuery string `json:"schema_attributes_query,omitempty"`
	// QualifierRefusal says why a planned migration is not rewritten to name
	// another schema on the engine, as a schema qualifier such as `migrate
	// diff --qualifier` asks. Empty means the rewrite runs.
	QualifierRefusal string `json:"qualifier_refusal,omitempty"`

	// Foreign key naming: what internal/sqlschema, the comparison and the
	// generator's backing index cleanup read; see names.go.

	// ForeignKeyNaming is how the server names a foreign key a schema file
	// leaves unnamed, and the index it builds for the key. Empty leaves the
	// key's name to ConstraintNaming.
	ForeignKeyNaming ForeignKeyNaming `json:"foreign_key_naming,omitempty"`
}

// DevRealm states how Ptah names the dev database a connection selected and
// serializes the runs that use it. internal/devlock reads it, and checks the
// enumerated values when it does.
type DevRealm struct {
	// Identity is where the name of the dev database comes from. "query"
	// runs Query, which selects the name of the database the session is in.
	// "file" runs Query, which selects the path of the file that holds the
	// database, and names the file by its identity on disk; an empty path is
	// an in-memory database. "writer" asks the connection's schema writer,
	// for an engine whose dev database is a realm only the writer can name.
	// Empty names none.
	Identity string `json:"identity,omitempty"`
	// Query is the statement Identity "query" and "file" run, such as
	// "SELECT current_database()". It selects one value.
	Query string `json:"query,omitempty"`
	// ServerQuery selects what names the server itself, for a connection that
	// holds a whole server rather than one database. Empty means no
	// connection to the engine holds a whole server.
	ServerQuery string `json:"server_query,omitempty"`
	// FileLock is how runs are serialized where the server takes no advisory
	// lock: "local" with a lock file on this machine, and "loopback" with one
	// too, refused unless the URL names this machine, since a file lock
	// cannot serialize runs on two machines. Empty refuses the dev database.
	FileLock string `json:"file_lock,omitempty"`
	// ScratchDatabase reports that the server gives a test case a database
	// of its own: CREATE DATABASE creates one beside the database the URL
	// names, DROP DATABASE removes it, and the URL with its name in place of
	// the database's addresses it. An engine without it may still isolate a
	// case through a service of its own.
	ScratchDatabase bool `json:"scratch_database,omitempty"`
	// DropSchema is the statement that drops a schema and everything in it
	// when a disposable database is reset, with %s standing for the schema
	// name in double quotes. Empty means a reset leaves the schemas alone,
	// and the default schema is always left.
	DropSchema string `json:"drop_schema,omitempty"`
}

// OnlineAlter is a grammar for asking the server to apply an ALTER TABLE
// without blocking writes.
type OnlineAlter string

const (
	// OnlineAlterAlgorithmLock is the MySQL family's ALGORITHM= and LOCK=
	// clauses, which the server refuses when it cannot honor them.
	OnlineAlterAlgorithmLock OnlineAlter = "algorithm-lock"
	// OnlineAlterNotValid is PostgreSQL's NOT VALID constraint, validated by a
	// second statement whose scan takes a weaker lock.
	OnlineAlterNotValid OnlineAlter = "not-valid"
)

// GeneratedTimeouts are the timeouts a generated migration file states, each a
// duration in the spelling the migration directives read, such as "3s".
type GeneratedTimeouts struct {
	// Lock bounds how long a statement waits for a lock.
	Lock string `json:"lock,omitempty"`
	// Statement bounds how long a statement runs.
	Statement string `json:"statement,omitempty"`
}

// InlineEnum is how a column of an enum holds its values.
type InlineEnum struct {
	// Type is the column's type. [InlineEnumValues] lists the values in the
	// type, as MySQL's ENUM('a', 'b'); [InlineEnumNumbered] lists them
	// numbered, as ClickHouse's Enum8('a' = 1, 'b' = 2). Any other type, such
	// as "TEXT", holds the values under a CHECK.
	Type string `json:"type,omitempty"`
	// Check is how that CHECK names the column: [InlineEnumCheckBare] as
	// written, or [InlineEnumCheckQuoted] spelled as the engine spells an
	// identifier. Empty writes no CHECK.
	Check string `json:"check,omitempty"`
}

const (
	// InlineEnumValues is the [InlineEnum.Type] that lists the values in the
	// column type.
	InlineEnumValues = "values"
	// InlineEnumNumbered is the [InlineEnum.Type] that lists the values
	// numbered in the column type.
	InlineEnumNumbered = "numbered"
	// InlineEnumCheckBare is the [InlineEnum.Check] that names the column as
	// written.
	InlineEnumCheckBare = "bare"
	// InlineEnumCheckQuoted is the [InlineEnum.Check] that names the column as
	// the engine spells an identifier.
	InlineEnumCheckQuoted = "quoted"
)

// Identifiers states how an engine compares identifiers offline. The values
// are spelled as core/platform/identifier spells its IndexNamespace and
// Comparison values, and that package checks them when it reads them.
type Identifiers struct {
	// IndexNamespace is where an index name must be unique: "table" or
	// "schema".
	IndexNamespace string `json:"index_namespace"`
	// IndexNames is how two index names compare, such as "exact".
	IndexNames string `json:"index_names"`
	// TableNames is how two table names compare.
	TableNames string `json:"table_names"`
	// ColumnNames is how two column names compare.
	ColumnNames string `json:"column_names"`
}

// Placeholder is how a statement marks a bound parameter.
type Placeholder string

const (
	// PlaceholderQuestion marks every parameter with "?".
	PlaceholderQuestion Placeholder = "question"
	// PlaceholderDollar numbers parameters "$1", "$2" and so on.
	PlaceholderDollar Placeholder = "dollar"
	// PlaceholderAtP numbers parameters "@p1", "@p2" and so on.
	PlaceholderAtP Placeholder = "at-p"
	// PlaceholderColon numbers parameters ":1", ":2" and so on.
	PlaceholderColon Placeholder = "colon"
	// PlaceholderDollarNamed names parameters "$p1", "$p2" and so on, as
	// declared query parameters rather than positions.
	PlaceholderDollarNamed Placeholder = "dollar-named"
)

// Placeholders returns every declared placeholder style.
func Placeholders() []Placeholder {
	return []Placeholder{PlaceholderQuestion, PlaceholderDollar, PlaceholderAtP, PlaceholderColon, PlaceholderDollarNamed}
}

// Spellings returns Name followed by Aliases, as a new slice.
func (d Dialect) Spellings() []string {
	return append([]string{d.Name}, d.Aliases...)
}

// Clone returns a deep copy of d: nothing reachable from the copy is shared
// with d.
func (d Dialect) Clone() Dialect {
	var clone Dialect
	deepCopy(reflect.ValueOf(&clone).Elem(), reflect.ValueOf(d))
	return clone
}

// Equal reports whether d and other state the same things. A nil slice or map
// and an empty one are equal.
func (d Dialect) Equal(other Dialect) bool {
	return reflect.DeepEqual(d.canonical(), other.canonical())
}

// canonical returns a copy in which every empty slice and map is nil, so
// Equal does not tell them apart.
func (d Dialect) canonical() Dialect {
	clone := d.Clone()
	emptyToNil(reflect.ValueOf(&clone).Elem())
	return clone
}

// validate checks the fields every reader shares. The family is checked by
// the registry, which knows the other descriptors.
func (d Dialect) validate() error {
	if !validSpelling(d.Name) {
		return fmt.Errorf("%w: name %q must be lowercase ASCII letters, digits, '-', '_' or '+', beginning with a letter", ErrInvalidDescriptor, d.Name)
	}
	seen := map[string]bool{d.Name: true}
	for _, alias := range d.Aliases {
		if !validSpelling(alias) {
			return fmt.Errorf("%w: dialect %q: alias %q must be lowercase ASCII letters, digits, '-', '_' or '+', beginning with a letter", ErrInvalidDescriptor, d.Name, alias)
		}
		if seen[alias] {
			return fmt.Errorf("%w: dialect %q spells %q twice", ErrInvalidDescriptor, d.Name, alias)
		}
		seen[alias] = true
	}
	if !slices.Contains(Placeholders(), d.Placeholder) {
		return fmt.Errorf("%w: dialect %q: placeholder %q is not one of %q", ErrInvalidDescriptor, d.Name, d.Placeholder, Placeholders())
	}
	if err := d.validateConnection(); err != nil {
		return err
	}
	if err := d.validateURLs(); err != nil {
		return err
	}
	if err := d.validateAtlas(); err != nil {
		return err
	}
	if err := d.validateScope(); err != nil {
		return err
	}
	if err := d.validateIdentifiers(); err != nil {
		return err
	}
	if err := d.validateNames(); err != nil {
		return err
	}
	if err := d.validateLexical(); err != nil {
		return err
	}
	if err := d.validateScripts(); err != nil {
		return err
	}
	if err := d.validateReach(); err != nil {
		return err
	}
	if err := d.validateGrammar(); err != nil {
		return err
	}
	if err := d.validateCleanCheck(); err != nil {
		return err
	}
	if err := d.validateSystemSchemas(); err != nil {
		return err
	}
	return nil
}

// validSpelling reports whether value is a name or alias: lowercase ASCII
// letters, digits, '-', '_' and '+', beginning with a letter.
func validSpelling(value string) bool {
	if value == "" || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	return strings.IndexFunc(value, func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' && r != '_' && r != '+'
	}) < 0
}

// deepCopy copies src into dst, allocating every slice and map anew. A
// descriptor holds data only, which is what lets one function copy every
// field a later change adds.
func deepCopy(dst, src reflect.Value) {
	switch src.Kind() {
	case reflect.Struct:
		for i := range src.NumField() {
			deepCopy(dst.Field(i), src.Field(i))
		}
	case reflect.Slice:
		if src.IsNil() {
			return
		}
		dst.Set(reflect.MakeSlice(src.Type(), src.Len(), src.Len()))
		for i := range src.Len() {
			deepCopy(dst.Index(i), src.Index(i))
		}
	case reflect.Map:
		if src.IsNil() {
			return
		}
		dst.Set(reflect.MakeMapWithSize(src.Type(), src.Len()))
		iter := src.MapRange()
		for iter.Next() {
			value := reflect.New(src.Type().Elem()).Elem()
			deepCopy(value, iter.Value())
			dst.SetMapIndex(iter.Key(), value)
		}
	default:
		dst.Set(src)
	}
}

// emptyToNil replaces every empty slice and map reachable from value with nil.
func emptyToNil(value reflect.Value) {
	switch value.Kind() {
	case reflect.Struct:
		for _, field := range value.Fields() {
			emptyToNil(field)
		}
	case reflect.Slice:
		if value.Len() == 0 {
			value.Set(reflect.Zero(value.Type()))
			return
		}
		for i := range value.Len() {
			emptyToNil(value.Index(i))
		}
	case reflect.Map:
		if value.Len() == 0 {
			value.Set(reflect.Zero(value.Type()))
			return
		}
		iter := value.MapRange()
		for iter.Next() {
			element := reflect.New(value.Type().Elem()).Elem()
			element.Set(iter.Value())
			emptyToNil(element)
			value.SetMapIndex(iter.Key(), element)
		}
	default:
	}
}
