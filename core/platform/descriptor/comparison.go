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
	// unique key's columns in another order than the key lists them. PostgreSQL,
	// CockroachDB and SQLite take REFERENCES parent (a, b) over PRIMARY KEY (b, a),
	// while MySQL answers with Error 6125 and MariaDB with errno 150.
	ForeignKeyMatchesKeyInAnyOrder bool `json:"foreign_key_matches_key_in_any_order,omitempty"`
	// ColumnPrimaryKeyIsTheTablesKey reports that a primary key a column declares
	// is compared as the table's key where the table holds another key or none:
	// the planner has a statement for a change of the key, in both directions, and
	// none that a column's primary_key flag can say on its own.
	ColumnPrimaryKeyIsTheTablesKey bool `json:"column_primary_key_is_the_tables_key,omitempty"`
	// GeneratedKeyIsColumnFlag reports that the read carries a primary key the
	// server named itself only as the primary_key flag of its columns, never as
	// a constraint. A diff that takes such a key away names no constraint, and
	// the plan drops the table's key with a statement that names none.
	GeneratedKeyIsColumnFlag bool `json:"generated_key_is_column_flag,omitempty"`
	// InternalIndexPrefix begins the name of an index the engine builds for
	// itself and no statement can name, such as SQLite's autoindexes.
	InternalIndexPrefix string `json:"internal_index_prefix,omitempty"`
	// ExpressionProbes is the catalog the server is asked how it stores a
	// declared expression: "pg-catalog" (CHECK, column, trigger, view and
	// routine spellings, through PostgreSQL's catalog functions),
	// "sys-catalog" (CHECK and computed column expressions, through SQL
	// Server's), or empty for none.
	ExpressionProbes string `json:"expression_probes,omitempty"`
	// ProbeViewsInSchema reports that an expression probe creates its view
	// in a schema rather than as a temporary view.
	ProbeViewsInSchema bool `json:"probe_views_in_schema,omitempty"`
	// TemporaryTablesSetting names the session setting that makes temporary
	// tables available, on a server that refuses them while it is off. A
	// CHECK probe turns it on for its own transaction before it creates its
	// temporary table. Empty where temporary tables need no setting.
	TemporaryTablesSetting string `json:"temporary_tables_setting,omitempty"`
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
	// QuotedNamesKeepCase reports that a double-quoted name keeps the case it
	// is written in while a name written without quotes is folded to lower
	// case, as in PostgreSQL. SQL text compared offline then keeps a quoted
	// name holding an upper-case letter apart from every bare spelling:
	// "A".src and a.src read two schemas. It states the lower fold for SQL
	// text, where [Identifiers.UnquotedFold] states a fold for catalog names,
	// and a descriptor that sets it with the upper fold is refused when the
	// registry is built ([ErrInvalidDescriptor]).
	QuotedNamesKeepCase bool `json:"quoted_names_keep_case,omitempty"`
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
	// AutoIncrementIdentity reports that the target builds every
	// auto-increment column as an identity column and that a read says which
	// generation each holds, so a column's identity is compared with the one
	// the declaration builds: its declared generation, or BY_DEFAULT for an
	// auto-increment flag or a counter type. A serial spelling is that identity
	// rather than a sequence of the column's own, which is how it differs from
	// IdentityColumns.
	AutoIncrementIdentity bool `json:"auto_increment_identity,omitempty"`

	// SequenceStartIsNextValue reports that the start a read reports for a
	// sequence is the next value the sequence issues, rounded up to the end of
	// its cached block, rather than the START WITH it was created with, as
	// Oracle's LAST_NUMBER is. A declared start the counter has reached, at
	// or below the read start for an ascending sequence and at or above it for
	// a descending one, compares equal.
	SequenceStartIsNextValue bool `json:"sequence_start_is_next_value,omitempty"`

	// SerialOwnsSequence reports that a column of a serial type draws from a
	// sequence it owns, so a plan makes a plain column serial with a sequence of
	// its own, a default and an owner.
	SerialOwnsSequence bool `json:"serial_owns_sequence,omitempty"`
	// SerialDefault is the default a column of a serial type holds where it
	// owns no sequence, so a plan makes a plain column serial with that default.
	SerialDefault string `json:"serial_default,omitempty"`
	// PrimaryKeyName is the name the server gives every primary key, such as
	// "PRIMARY". Empty means a primary key keeps the name it is declared
	// with, and one declared with none is named after its table.
	PrimaryKeyName string `json:"primary_key_name,omitempty"`
	// ConstraintComments reports that a constraint carries a comment the
	// catalog reports, so a changed comment is a changed constraint.
	ConstraintComments bool `json:"constraint_comments,omitempty"`
	// ColumnCheckDropByModify reports that a column-level CHECK cannot be
	// dropped as a constraint and is removed by restating its column instead.
	ColumnCheckDropByModify bool `json:"column_check_drop_by_modify,omitempty"`
	// RoutineNamesFold reports that the server folds the case of a routine
	// name it is given, so two spellings that fold together name one routine.
	RoutineNamesFold bool `json:"routine_names_fold,omitempty"`
	// SharedObjectNamespace states the kinds of object that share one
	// namespace in a schema, so an object of one kind cannot take a name an
	// object of another kind holds there. SQL Server keeps tables, views,
	// routines and sequences in sys.objects: CREATE beside an object of
	// another kind of that name fails with error 2714, and CREATE OR ALTER
	// with error 2010. A declaration that gives two of these kinds one name
	// is refused, and so is a plan that creates an object of one kind and
	// drops an object of another the server may read as the same name, since
	// the create would run while the other object exists. The zero value
	// states no such namespace.
	SharedObjectNamespace ObjectNamespace `json:"shared_object_namespace,omitzero"`
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
	// ViewJoinsParenthesized reports that the server stores a view's joins in
	// a spelling of its own: the joins a FROM clause starts with in
	// parentheses, nested from the left, each ON condition in parentheses, a
	// table alias without AS, and a join keyword without INNER, CROSS or
	// OUTER. A declared and a stored body are compared with those spellings
	// read alike.
	ViewJoinsParenthesized bool `json:"view_joins_parenthesized,omitempty"`
	// ViewSQLSecurity reports that a view has a SQL SECURITY mode in the
	// catalog. An omitted declaration uses the server's DEFINER default.
	ViewSQLSecurity bool `json:"view_sql_security,omitempty"`
	// QualifiedTypeNames reports that the catalog names a column of a
	// schema-qualified user type by its type schema and name.
	QualifiedTypeNames bool `json:"qualified_type_names,omitempty"`
	// AutoIncrementFlag reports that the catalog reports whether a column
	// auto-increments, so a declared auto-increment is compared with it.
	AutoIncrementFlag bool `json:"auto_increment_flag,omitempty"`
	// TimePrecision says how the engine reads a temporal column's fractional-seconds
	// precision. The zero value compares none.
	TimePrecision TimePrecision `json:"time_precision,omitzero"`
	// TemporalDefaultFunctions reports that GETDATE() and CURRENT_TIMESTAMP
	// are one default expression, while SYSDATETIME() keeps its higher precision.
	// Function names are compared without regard to case.
	TemporalDefaultFunctions bool `json:"temporal_default_functions,omitempty"`
	// OperatorPrecedence names the operator precedence the engine groups an
	// expression by: "mysql-operators" (internal/precedence's table, which
	// MySQL and MariaDB share), "clickhouse-operators", or empty for none. A
	// declared and a stored expression default are compared as the
	// operations that precedence groups, each spelled one way, so a
	// parenthesis it makes redundant or a synonym the server writes is no
	// difference: MySQL stores (2 * 3 + 1) as ((2 * 3) + 1), ClickHouse
	// stores 2 * 3 + 1 as (2 * 3) + 1, and (1 + 2) * 3 still differs from
	// 1 + 2 * 3. Empty compares the text.
	OperatorPrecedence string `json:"operator_precedence,omitempty"`
	// BareTypes says what a column holds whose type declares no length, precision or
	// scale. The zero value states nothing, and such a type is compared by its name.
	BareTypes BareTypes `json:"bare_types,omitzero"`
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
	// MapsDeclaredTypes reports that the renderer maps most declared type
	// names to types of the engine's own, as internal/oracletype does, so a
	// declaration is compared in the renderer's spelling.
	MapsDeclaredTypes bool `json:"maps_declared_types,omitempty"`
	// MapsPortableTypes reports that the renderer writes a portable type name as a type
	// of the engine's own, as internal/sqlservertype does, and writes a type named for
	// the engine as it stands. A declaration is compared in the renderer's spelling, and
	// a portable name that also names one of the engine's own types matches that type
	// too, since the server builds it from the same word.
	MapsPortableTypes bool `json:"maps_portable_types,omitempty"`
	// BitStoresBoolean reports that BIT holds only false or true, so changing
	// an integer column to BIT can collapse distinct stored values.
	BitStoresBoolean bool `json:"bit_stores_boolean,omitempty"`
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
	// StoredGeneratedKind and VirtualGeneratedKind are the kinds the server
	// creates for a generated column declared STORED or VIRTUAL, where it
	// names them otherwise: ClickHouse creates MATERIALIZED and ALIAS. Empty
	// compares the declared kind as written.
	StoredGeneratedKind  string `json:"stored_generated_kind,omitempty"`
	VirtualGeneratedKind string `json:"virtual_generated_kind,omitempty"`
	// GeneratedColumnsBecomePlain reports that a live generated column whose
	// declaration states no generated kind is compared with the declaration,
	// and the plan makes it an ordinary column, as ClickHouse's REMOVE
	// MATERIALIZED does. Without it such a column is left uncompared.
	GeneratedColumnsBecomePlain bool `json:"generated_columns_become_plain,omitempty"`
	// GeneratedColumnTypesDerived reports that the server derives a
	// generated column's type and nullability from its expression and the
	// column's DDL states neither, as SQL Server does for a computed column.
	// On a column generated on both sides neither is compared: the declared
	// type never reaches the server, and SQL Server refuses ALTER COLUMN on a
	// computed column with Msg 4928.
	GeneratedColumnTypesDerived bool `json:"generated_column_types_derived,omitempty"`
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
	// CollatedKeysRespelled reports that an index key written as a column and
	// a COLLATE, `code COLLATE NOCASE`, is that column under that collation,
	// and that a double-quoted, a bracketed, a backquoted and a bare name are
	// one identifier there, as is each spelling of the collation's name. A
	// document and a catalog then compare such a key in one spelling, so
	// `("code" COLLATE "NOCASE" ASC)` and `([code] COLLATE /*x*/ NOCASE)` are
	// one key, as SQLite reads them. MySQL reads a double-quoted word as a
	// string unless ANSI_QUOTES is set, and SQL Server and ClickHouse key no
	// column under a collation of its own, so the fact is not theirs.
	CollatedKeysRespelled bool `json:"collated_keys_respelled,omitempty"`
}

// TimePrecision states that a comparison reads the precision of a temporal column,
// and what a column that declares none holds. Two columns differ when their
// precisions do, so `timestamp(3)` and `timestamp(6)` are a change and, where six is
// the default, `timestamp` and `timestamp(6)` are not.
type TimePrecision struct {
	// Compared reports that the comparison reads the precision.
	Compared bool `json:"compared,omitempty"`
	// Default is the precision of a column that declares none. It answers whether a
	// change the comparison reports for another reason narrows the column, so an
	// engine that does not compare a precision can still state one.
	Default int `json:"default,omitempty"`
}

// BareTypes states what a column holds whose type declares no size, which decides both
// whether two spellings are one type and which way a change between them goes. `decimal`
// is `decimal(10,0)` on MySQL and `decimal(18,0)` on SQL Server, and it holds any number
// on PostgreSQL, so `decimal(10,2)` declared `decimal` loses the fraction on the first
// two and on the third keeps every value.
//
// What SQL fixes for every engine needs no field: a bare CHAR, NCHAR, BINARY or BIT holds
// one.
type BareTypes struct {
	// Stated reports that the fields below hold the engine's answers.
	Stated bool `json:"stated,omitempty"`
	// DecimalPrecision is the digits of a DECIMAL that declares none, which then holds
	// no fraction. Zero means it holds a number of any size.
	DecimalPrecision int `json:"decimal_precision,omitempty"`
	// VariableLength is the length of a VARCHAR or VARBINARY that declares none. Zero
	// means it has no limit, or that the server refuses the spelling.
	VariableLength int `json:"variable_length,omitempty"`
	// FloatBits and RealBits are the mantissa bits of a FLOAT that declares no precision
	// and of a REAL.
	FloatBits int `json:"float_bits,omitempty"`
	RealBits  int `json:"real_bits,omitempty"`
	// FloatKeepsPrecision reports that FLOAT(p) holds p bits. Elsewhere the server
	// builds the 4-byte type for a p up to 24 and the 8-byte type above it.
	FloatKeepsPrecision bool `json:"float_keeps_precision,omitempty"`
	// IntervalLeading is the digits of an interval's leading field where it declares
	// none, on an engine whose catalog reports them.
	IntervalLeading int `json:"interval_leading,omitempty"`
	// LobTiers reports that TEXT and BLOB are one of four sizes, as MySQL's are, rather
	// than holding a value of any length.
	LobTiers bool `json:"lob_tiers,omitempty"`
	// MoneyScale is the digits after the point a MONEY column keeps, where they are not
	// the four SQL Server's money keeps. PostgreSQL's money keeps the digits its
	// lc_monetary locale names, two in the C and en_US locales.
	MoneyScale int `json:"money_scale,omitempty"`
}

// ObjectNamespace states which kinds of object share one namespace in a
// schema; see [Comparison.SharedObjectNamespace]. A routine is a function or
// a procedure.
type ObjectNamespace struct {
	// Tables reports that tables are in the namespace.
	Tables bool `json:"tables,omitempty"`
	// Views reports that views are in the namespace.
	Views bool `json:"views,omitempty"`
	// MaterializedViews reports that materialized views are in the
	// namespace.
	MaterializedViews bool `json:"materialized_views,omitempty"`
	// Routines reports that functions and procedures are in the namespace,
	// as two kinds: a procedure cannot take the name a function holds.
	Routines bool `json:"routines,omitempty"`
	// Sequences reports that sequences are in the namespace.
	Sequences bool `json:"sequences,omitempty"`
}
