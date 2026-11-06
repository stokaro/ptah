package descriptor

// Comparison states facts about an engine that comparing a declared schema
// with the catalog it is applied to depends on. migration/schemadiff,
// internal/indexbacking, internal/foreignkeyscope, internal/dbexprprobe and
// internal/tableidentity read it and check the enumerated values
// when they do. The zero value is an engine with none of these properties.
type Comparison struct {
	// ForeignKeysOutside reports that a foreign key may name a table outside
	// the schemas a description holds, and the engine accepts it; an
	// unqualified table is then in DefaultSchema.
	ForeignKeysOutside bool `json:"foreign_keys_outside,omitempty"`
	// VerbatimTableNames reports that a table is identified by the exact
	// bytes the catalog holds rather than by the name comparison rules.
	VerbatimTableNames bool `json:"verbatim_table_names,omitempty"`
	// UniqueIndexStandsAlone reports that a unique index backs no UNIQUE
	// constraint: the two are separate objects, or the engine has no UNIQUE
	// constraint at all.
	UniqueIndexStandsAlone bool `json:"unique_index_stands_alone,omitempty"`
	// DeclaredSchemas reports that a schema the desired state declares and the
	// read does not hold is planned for creation, whether or not anything is in
	// it. It holds where a schema is a namespace inside one database and the
	// reader reports the schemas a comparison reads. ServerDatabases is the same
	// question where a schema is a database of a server.
	DeclaredSchemas bool `json:"declared_schemas,omitempty"`
	// ForeignKeyBackingIndex reports that the server creates and reports an
	// index for every foreign key.
	ForeignKeyBackingIndex bool `json:"foreign_key_backing_index,omitempty"`
	// ForeignKeyHoldsItsKey reports that a foreign key depends on the one index
	// that enforces the key it references, whatever else covers those columns,
	// and that the server refuses to drop that index while the foreign key
	// exists. A plan that removes the key drops the foreign key first and adds
	// it back once a key is there to reference.
	ForeignKeyHoldsItsKey bool `json:"foreign_key_holds_its_key,omitempty"`
	// ForeignKeyMatchesKeyInAnyOrder reports that a foreign key may reference a
	// unique key's columns in another order than the key lists them. PostgreSQL and
	// CockroachDB take REFERENCES parent (a, b) over PRIMARY KEY (b, a), while MySQL
	// answers with Error 6125 and MariaDB with errno 150.
	ForeignKeyMatchesKeyInAnyOrder bool `json:"foreign_key_matches_key_in_any_order,omitempty"`
	// ColumnPrimaryKeyIsTheTablesKey reports that a primary key a column declares
	// is compared as the table's key where the table holds another key or none:
	// the planner has a statement for a change of the key, in both directions, and
	// none that a column's primary_key flag can say on its own.
	ColumnPrimaryKeyIsTheTablesKey bool `json:"column_primary_key_is_the_tables_key,omitempty"`
	// InternalIndexPrefix begins the name of an index the engine builds for
	// itself and no statement can name, such as SQLite's autoindexes.
	InternalIndexPrefix string `json:"internal_index_prefix,omitempty"`
	// ExpressionProbes is the catalog the server is asked how it stores a
	// declared expression: "pg-catalog" (CHECK, column, trigger, view and
	// routine spellings, through PostgreSQL's catalog functions),
	// "sys-catalog" (CHECK expressions, through SQL Server's), or empty for
	// none.
	ExpressionProbes string `json:"expression_probes,omitempty"`
	// ProbeViewsInSchema reports that an expression probe creates its view
	// in a schema rather than as a temporary view.
	ProbeViewsInSchema bool `json:"probe_views_in_schema,omitempty"`
	// RewritesGeneratedExpressions reports that the server stores a
	// generated column's expression in a form of its own, so only the server
	// can say what a declaration becomes.
	RewritesGeneratedExpressions bool `json:"rewrites_generated_expressions,omitempty"`
	// RewritesRoutineBodies reports that the server stores a routine body in
	// a form of its own, reflowed and qualified, rather than as written.
	RewritesRoutineBodies bool `json:"rewrites_routine_bodies,omitempty"`
	// ProbesSQLStandardBodies reports that a routine probe may carry a
	// declared SQL-standard body, `BEGIN ATOMIC ... END` or `RETURN
	// expression`. The server keeps such a body parsed and prints it with
	// pg_get_function_sqlbody in its own spelling, and a routine the probe
	// creates in pg_temp leaves with the probe's rollback.
	ProbesSQLStandardBodies bool `json:"probes_sql_standard_bodies,omitempty"`
	// RolePasswords reports that the catalog can say whether a role has a
	// password, which a comparison that sets one must establish first.
	RolePasswords bool `json:"role_passwords,omitempty"`
	// DoubleQuotedStrings reports that a double-quoted token is a string
	// literal, as it is under MySQL's default SQL mode, rather than a name.
	DoubleQuotedStrings bool `json:"double_quoted_strings,omitempty"`
	// RestrictIsNoAction reports that a RESTRICT referential action means
	// what NO ACTION does, as InnoDB implements both.
	RestrictIsNoAction bool `json:"restrict_is_no_action,omitempty"`
	// PrimaryKeyRenames reports that a statement renames a primary key in place,
	// so a declared name over the columns the key holds is a rename, with no
	// rebuild of the key and nothing to do for a foreign key over it.
	PrimaryKeyRenames bool `json:"primary_key_renames,omitempty"`

	// IdentityColumns reports that a read says which identity generation a
	// column holds, and that ALTER COLUMN adds, changes and drops it, so a
	// column's identity is compared with its declaration.
	IdentityColumns bool `json:"identity_columns,omitempty"`
	// PrimaryKeyName is the name the server gives every primary key, such as
	// "PRIMARY". Empty means a primary key keeps the name it is declared
	// with, and one declared with none is named after its table.
	PrimaryKeyName string `json:"primary_key_name,omitempty"`
	// ConstraintComments reports that a constraint carries a comment the
	// catalog reports, so a changed comment is a changed constraint.
	ConstraintComments bool `json:"constraint_comments,omitempty"`
	// RoutineNamesFold reports that the server folds the case of a routine
	// name it is given, so two spellings that fold together name one routine.
	RoutineNamesFold bool `json:"routine_names_fold,omitempty"`
	// RoutineTypeAliases is the alias table a routine's parameter and return
	// types are compared through: "mysqlroutine" (internal/mysqlroutine's
	// synonyms) or "pg-aliases" (the type aliases PostgreSQL accepts and
	// reports back under another name), or empty for none.
	RoutineTypeAliases string `json:"routine_type_aliases,omitempty"`
	// ArgumentModeAfterName reports that a routine argument's mode follows
	// its name, as PL/SQL writes `p IN NUMBER`.
	ArgumentModeAfterName bool `json:"argument_mode_after_name,omitempty"`
	// ViewPredicatesAsIntegers reports that the server stores a view's
	// boolean predicate as an integer comparison.
	ViewPredicatesAsIntegers bool `json:"view_predicates_as_integers,omitempty"`
	// QualifiedTypeNames reports that the catalog names a column of a
	// schema-qualified user type by its type schema and name.
	QualifiedTypeNames bool `json:"qualified_type_names,omitempty"`
	// AutoIncrementFlag reports that the catalog reports whether a column
	// auto-increments, so a declared auto-increment is compared with it.
	AutoIncrementFlag bool `json:"auto_increment_flag,omitempty"`
	// TimePrecision says how the engine reads a temporal column's fractional-seconds
	// precision. The zero value compares none.
	TimePrecision TimePrecision `json:"time_precision,omitzero"`
	// PrimaryKeyNullable reports that a primary key column may hold NULL
	// unless the table's shape says otherwise, so a key does not imply NOT
	// NULL.
	PrimaryKeyNullable bool `json:"primary_key_nullable,omitempty"`
	// RowidTables reports that a table has a rowid, whose alias and whose
	// STRICT and WITHOUT ROWID shapes decide a key column's nullability as
	// internal/sqlitekey measures it.
	RowidTables bool `json:"rowid_tables,omitempty"`
	// TypeAffinity reports that the engine stores a declared type as written
	// and converts by its affinity, so two spellings with one affinity are
	// one type and a width is no distinction.
	TypeAffinity bool `json:"type_affinity,omitempty"`
	// SingleStringType reports that the engine has one string type, so the
	// unbounded spellings of it are one type and only a width distinguishes.
	SingleStringType bool `json:"single_string_type,omitempty"`
	// MapsDeclaredTypes reports that the renderer maps most declared type
	// names to types of the engine's own, as internal/oracletype does, so a
	// declaration is compared in the renderer's spelling.
	MapsDeclaredTypes bool `json:"maps_declared_types,omitempty"`
	// CatalogTypeNames reports that the catalog names the string and byte
	// types STRING and BYTES, as internal/cockroachtype spells them, and an
	// unsized INT resolves to the width the session's default_int_size gives.
	CatalogTypeNames bool `json:"catalog_type_names,omitempty"`
	// PrimaryKeyStoresColumns reports that the primary key's index stores
	// every column of its table, so a declared payload on it is implied.
	PrimaryKeyStoresColumns bool `json:"primary_key_stores_columns,omitempty"`
	// NullableTypes reports that nullability is part of a column's type, as
	// ClickHouse's Nullable(T) wraps it.
	NullableTypes bool `json:"nullable_types,omitempty"`
	// DefaultGeneratedKind is the storage a generated column declared
	// without one gets, such as "STORED". Empty means it has no default.
	DefaultGeneratedKind string `json:"default_generated_kind,omitempty"`
	// ServerDatabases reports that a comparison of a whole server compares
	// the databases it holds as well as the objects in them: a database only
	// the declaration holds is created, one only the server holds is
	// dropped, and a declared character set or collation that differs is
	// changed. Such a comparison requires the declaration to state its
	// databases.
	ServerDatabases bool `json:"server_databases,omitempty"`
	// NoTableComments reports that the server stores no comment on a table or
	// on a column, as SQLite's CREATE TABLE has no place for one. A declared
	// comment is lost when the table is created and the catalog reports none,
	// so the comparison does not compare it: it would be a difference on every
	// run that no plan can close.
	NoTableComments bool `json:"no_table_comments,omitempty"`
	// RespelledIndexReplaced reports that an index whose catalog name the
	// name rules read as the declared name, but which is spelled otherwise,
	// such as in another case, is dropped and created under the declared
	// spelling rather than kept.
	RespelledIndexReplaced bool `json:"respelled_index_replaced,omitempty"`
}

// TimePrecision states that a comparison reads the precision of a temporal column,
// and what a column that declares none holds. Two columns differ when their
// precisions do, so `timestamp(3)` and `timestamp(6)` are a change and, where six is
// the default, `timestamp` and `timestamp(6)` are not.
type TimePrecision struct {
	// Compared reports that the comparison reads the precision.
	Compared bool `json:"compared,omitempty"`
	// Default is the precision of a column that declares none.
	Default int `json:"default,omitempty"`
}
