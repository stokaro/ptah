package descriptor

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode"

	"ptah.run/internal/statementform"
)

// Grammar states where the engine's DDL grammar differs, as the shared DDL
// parser, internal/parser, reads it. The parser reads a document of no
// dialect, and one of a dialect no descriptor carries, permissively, taking
// any engine's spelling; with a descriptor it reads the engine's grammar as
// stated here.
//
// The zero value is an engine that has none of the constructs below and
// restricts none of the shared ones: a table body declares no index, a
// constraint takes no ENFORCED, NOT VALID or MATCH clause, every deferral
// clause is read as PostgreSQL reads it, ALTER TABLE has no MODIFY, and a
// routine is read best effort.
//
// A field that ends in Refusal holds the sentence the parser writes after the
// position of a construct it refuses, such as "SQLite takes it only after
// [NOT] DEFERRABLE". The sentence is the engine's own measured answer, which
// is why it is data: it names the release that answers so and the error it
// answers with. Empty means the engine takes the construct.
type Grammar struct {
	// Columns states how a column definition is read.
	Columns ColumnGrammar `json:"columns,omitzero"`
	// Constraints states how a table constraint and a column's REFERENCES
	// are read.
	Constraints ConstraintGrammar `json:"constraints,omitzero"`
	// Keys states how a key or an index declared in a table body is read.
	Keys KeyGrammar `json:"keys,omitzero"`
	// Indexes states how CREATE INDEX, ALTER INDEX and DROP INDEX are read.
	Indexes IndexGrammar `json:"indexes,omitzero"`
	// AlterTable states how the clauses of an ALTER TABLE are read.
	AlterTable AlterTableGrammar `json:"alter_table,omitzero"`
	// Statements states which statements of the shared grammar the engine
	// writes as it does.
	Statements StatementGrammar `json:"statements,omitzero"`
	// Forms states the statements the engine writes in a form of its own.
	Forms StatementForms `json:"forms,omitzero"`
	// Routines states how a routine, a trigger and an anonymous code block
	// are read.
	Routines RoutineGrammar `json:"routines,omitzero"`
}

// ColumnGrammar states how the engine reads a column definition.
type ColumnGrammar struct {
	// TimestampLocalTimeZone reports that TIMESTAMP accepts WITH LOCAL TIME ZONE,
	// including after an explicit fractional precision.
	TimestampLocalTimeZone bool `json:"timestamp_local_time_zone,omitempty"`
	// CurrentTimestampNiladic reports that CURRENT_TIMESTAMP is a keyword
	// without parentheses, while the other current-time names do not share
	// that grammar. SQL Server accepts it as a default expression.
	CurrentTimestampNiladic bool `json:"current_timestamp_niladic,omitempty"`
	// CurrentTimeKeywords reports that CURRENT_TIMESTAMP, CURRENT_DATE,
	// CURRENT_TIME, LOCALTIMESTAMP and LOCALTIME are keywords that take no
	// empty argument list, so a default written with one of them is kept as
	// written. Without it a default of CURRENT_TIMESTAMP, CURRENT_DATE or
	// CURRENT_TIME is read as a call, with `()` after it.
	CurrentTimeKeywords bool `json:"current_time_keywords,omitempty"`
	// NiladicFunctions names the functions, beside the current-time
	// keywords, that a default calls without parentheses, in upper case:
	// Oracle's SYSDATE, SYSTIMESTAMP, USER and UID, among others. A default
	// of one is kept as the expression written. Without its name, the word
	// is read as a literal value, which a renderer writes as a string.
	NiladicFunctions []string `json:"niladic_functions,omitempty"`
	// RelativeTimeLiterals names, in upper case, the date and time input
	// strings the engine reads as a moment relative to when it reads them,
	// such as PostgreSQL's 'now' and 'today'. The engine reads a default
	// when the table is created, so a default of one on a date or time column
	// holds that moment for every row, and a comparison with the declaration
	// changes the column on every apply. The parser refuses one.
	RelativeTimeLiterals []string `json:"relative_time_literals,omitempty"`
	// EmptyCallFunctions names, in upper case, the functions a default may
	// write bare that the engine refuses with an empty argument list, such as
	// Oracle's SYSDATE(). A precision, as in SYSTIMESTAMP(3), is no empty
	// list. A function the engine calls either way, such as Oracle's
	// SESSIONTIMEZONE, is left out.
	EmptyCallFunctions []string `json:"empty_call_functions,omitempty"`
	// SequencePseudocolumns names, in upper case, the pseudocolumns a default
	// reads from a sequence by writing them after its name, as Oracle's
	// NEXTVAL and CURRVAL: `seq.NEXTVAL`, `app.seq.CURRVAL` or
	// `"Seq".NEXTVAL`. A default naming one is kept as the expression
	// written, and compared with the catalog by the names the catalog
	// stores, without a schema that names the table's own. Without its name,
	// the qualified word is read as a literal value, which a renderer writes
	// as a string.
	SequencePseudocolumns []string `json:"sequence_pseudocolumns,omitempty"`
	// NextValueClause names, in upper case and separated by one space, the
	// words a default writes before a sequence's name to read its next value,
	// as SQL Server's NEXT VALUE FOR: `NEXT VALUE FOR seq` or
	// `NEXT VALUE FOR [dbo].[seq]`. A default holding the clause is kept as
	// the expression written, and compared with the catalog by the name it
	// reads, each part without its brackets or quotes. Empty reads no such
	// clause, and a renderer writes NEXT as a literal value.
	NextValueClause string `json:"next_value_clause,omitempty"`
	// EmptyCallRefusal says why a default that writes a function of
	// EmptyCallFunctions with an empty argument list is refused. The parser
	// names the bare spelling before the sentence. The two fields are set
	// together or not at all.
	EmptyCallRefusal string `json:"empty_call_refusal,omitempty"`
	// DefaultOperatorRefusal says why a default that continues with a binary
	// operator outside parentheses, such as DEFAULT 5 - 2, is refused, for an
	// engine that takes an expression default only in parentheses. The parser
	// writes the parenthesized form, DEFAULT (5 - 2), before the sentence.
	// Empty reads the operator into the default.
	DefaultOperatorRefusal string `json:"default_operator_refusal,omitempty"`
	// DefaultPredicates states which comparisons and predicates a default
	// written without parentheses reads, as in DEFAULT a = 1 OR b = 3, for an
	// engine that reads some of them there and answers a syntax error to the
	// rest. The zero value reads every one.
	DefaultPredicates DefaultPredicates `json:"default_predicates,omitzero"`
	// DefaultPipesRefusal says why a default that holds ||, in parentheses or
	// without, is refused, for an engine that reads || by the session's SQL
	// mode: as OR under one mode and as concatenation under another. The
	// parser names the CONCAT form before the sentence. Empty reads || as an
	// operator of the default.
	DefaultPipesRefusal string `json:"default_pipes_refusal,omitempty"`
	// SecondCheckRefusal says why a column definition that writes a second
	// CHECK is refused. Empty reads the second CHECK as the table's.
	SecondCheckRefusal string `json:"second_check_refusal,omitempty"`
	// ReferencesRefusal says why a column-level REFERENCES clause is refused,
	// for an engine where what the clause builds depends on a release the
	// reader cannot know. Empty reads the clause as the column's foreign key.
	ReferencesRefusal string `json:"references_refusal,omitempty"`
	// Constraint is which column constraint a CONSTRAINT keyword may precede,
	// for an engine that takes it before one kind alone. The zero value takes
	// CONSTRAINT before every kind, and only with a name after it.
	Constraint ColumnConstraint `json:"constraint,omitzero"`
	// ModifyRefusesReferences reports that the column definition of an ALTER
	// TABLE ... MODIFY takes no REFERENCES clause, with CONSTRAINT or without.
	ModifyRefusesReferences bool `json:"modify_refuses_references,omitempty"`
	// TypeArgumentsAreTypes reports that a type's arguments may be bare type
	// names, as ClickHouse's Nullable(T) and Array(T) take. Without it, a
	// column whose type arguments all name columns of its table is refused as
	// an index or constraint whose keyword is misspelled.
	TypeArgumentsAreTypes bool `json:"type_arguments_are_types,omitempty"`
	// StoredStringDefaults reports that a string default is read as the one
	// spelling the server stores whichever constant the file wrote, so that
	// `$$x$$` and `E'x'` are the default 'x'. Without it a string default is
	// kept as written.
	StoredStringDefaults bool `json:"stored_string_defaults,omitempty"`
	// StoredStringEscapes reports that the spelling a server stores a string default in
	// escapes with a backslash rather than doubling a quote: ClickHouse reports `'it''s'`
	// as `'it\'s'`. It has no effect without StoredStringDefaults.
	StoredStringEscapes bool `json:"stored_string_escapes,omitempty"`
	// IdentityProperty reports that IDENTITY, with or without a (seed,
	// increment) pair, makes the column an identity column, as T-SQL writes
	// it. A bare IDENTITY is IDENTITY(1, 1). Without it IDENTITY is refused
	// as an unsupported column attribute.
	IdentityProperty bool `json:"identity_property,omitempty"`
	// PersistedComputedColumns reports that a column written `AS (expr)` is
	// computed when it is read, and one written `AS (expr) PERSISTED` is
	// stored, as T-SQL writes them. The column reads VIRTUAL or PERSISTED as
	// its generated kind. Without it a bare `AS (expr)` states no kind, which
	// is the target's default, and PERSISTED is refused as an unsupported
	// column attribute.
	PersistedComputedColumns bool `json:"persisted_computed_columns,omitempty"`
	// QualifiedCollations reports that a column's COLLATE names a collation
	// by a name or by a schema and a name, each an identifier that folds to
	// lower case unless it is quoted. The model keeps the name the server
	// finds: a bare name, or the schema and the name each quoted. Without it
	// the name is kept as written.
	QualifiedCollations bool `json:"qualified_collations,omitempty"`
	// ComputedKinds are the words that give a column a value computed from
	// the expression written after them, in place of a DEFAULT, as
	// ClickHouse's MATERIALIZED, ALIAS and EPHEMERAL do. The column reads the
	// word as its generated kind and the expression as written. A column
	// takes one such clause or one DEFAULT, never both. Without them each
	// word is an unsupported column attribute.
	ComputedKinds []ComputedKind `json:"computed_kinds,omitempty"`
}

// ComputedKind is one word of [ColumnGrammar.ComputedKinds].
type ComputedKind struct {
	// Keyword opens the clause and is the generated kind the column reads,
	// one upper-case word.
	Keyword string `json:"keyword"`
	// ExpressionOptional reports that the word may stand without an
	// expression, as ClickHouse's EPHEMERAL does. The column then reads an
	// empty generated expression.
	ExpressionOptional bool `json:"expression_optional,omitempty"`
}

// DefaultPredicates states which comparisons and predicates an engine reads
// in a default written without parentheses: after the first operand, or
// before it for a NOT. With Refusal empty the engine reads every form, each
// with the operands after it, and the fields that name a form are not set.
// With Refusal set, a form no field names is refused at its operator or word.
type DefaultPredicates struct {
	// Refusal says why a form the other fields leave out is refused, as
	// PostgreSQL answers a syntax error to DEFAULT 1 IS NULL. Empty reads
	// every form.
	Refusal string `json:"refusal,omitempty"`
	// Comparison reads =, <>, !=, <, >, <=, >= and <=>, as in DEFAULT 1 + 1 = 2.
	Comparison bool `json:"comparison,omitempty"`
	// DoubleEquals reads == as a comparison, as in DEFAULT a == 1.
	DoubleEquals bool `json:"double_equals,omitempty"`
	// Shift reads the shifts << and >>, as in DEFAULT 1 << 2.
	Shift bool `json:"shift,omitempty"`
	// DistinctFrom reads IS [NOT] DISTINCT FROM.
	DistinctFrom bool `json:"distinct_from,omitempty"`
	// Is reads IS [NOT] NULL and every other IS test but DISTINCT FROM.
	Is bool `json:"is,omitempty"`
	// Logical reads AND and OR, and a NOT before an operand.
	Logical bool `json:"logical,omitempty"`
	// Between reads [NOT] BETWEEN low AND high.
	Between bool `json:"between,omitempty"`
	// In reads [NOT] IN (list).
	In bool `json:"in,omitempty"`
	// Like reads [NOT] LIKE.
	Like bool `json:"like,omitempty"`
}

// readsSome reports whether the fields name a form the engine reads.
func (p DefaultPredicates) readsSome() bool {
	return p.Comparison || p.DoubleEquals || p.Shift || p.DistinctFrom || p.Is || p.Logical || p.Between || p.In || p.Like
}

// ColumnConstraint is which column constraint a CONSTRAINT keyword may
// precede, on an engine that takes it before one kind alone. On such an
// engine CONSTRAINT may also stand without a name before that kind, and is
// refused before any other word.
type ColumnConstraint struct {
	// Define is the kind CONSTRAINT may precede in the column definition of
	// CREATE TABLE and ALTER TABLE ... ADD COLUMN: [ConstraintKindCheck] or
	// [ConstraintKindReferences]. Empty means the engine does not restrict
	// it, and then Modify is empty too.
	Define string `json:"define,omitempty"`
	// Modify is the kind CONSTRAINT may precede in the column definition of
	// ALTER TABLE ... MODIFY. Empty means it may precede none there.
	Modify string `json:"modify,omitempty"`
}

const (
	// ConstraintKindCheck is the CHECK column constraint.
	ConstraintKindCheck = "CHECK"
	// ConstraintKindReferences is the REFERENCES column constraint.
	ConstraintKindReferences = "REFERENCES"
)

// ConstraintGrammar states how the engine reads a table constraint and the
// clauses after one.
type ConstraintGrammar struct {
	// TableNotNull reports that a NOT NULL constraint may name its column
	// among the table elements of CREATE TABLE.
	TableNotNull bool `json:"table_not_null,omitempty"`
	// Enforcement is where ENFORCED and NOT ENFORCED may follow a
	// constraint.
	Enforcement Enforcement `json:"enforcement,omitzero"`
	// NotValid reports that NOT VALID may follow a CHECK or a foreign key,
	// and that ALTER TABLE ... VALIDATE CONSTRAINT completes it.
	NotValid bool `json:"not_valid,omitempty"`
	// Match states how a MATCH clause after REFERENCES is read.
	Match Match `json:"match,omitzero"`
	// Deferral states how [NOT] DEFERRABLE and INITIALLY DEFERRED |
	// IMMEDIATE are read.
	Deferral Deferral `json:"deferral,omitzero"`
	// SymbolOptional reports that CONSTRAINT may stand without a name before
	// a table's PRIMARY KEY, UNIQUE, FOREIGN KEY or CHECK, which the
	// constraint then reads as unnamed, as the MySQL family takes it.
	SymbolOptional bool `json:"symbol_optional,omitempty"`
	// EnableValidate reports that ALTER TABLE ... ENABLE [VALIDATE]
	// CONSTRAINT name checks every row against a CHECK or foreign key the
	// server holds disabled, or enabled without validating its rows, and
	// enforces it from then on, as Oracle's does. A row that fails the check
	// refuses the statement and leaves the constraint as it was. A plan
	// brings such a constraint to the declared state with the statement,
	// where without it the constraint is dropped and added again.
	EnableValidate bool `json:"enable_validate,omitempty"`
	// CheckAddedFirst reports that a plan replacing a named CHECK whose
	// definition changed adds the new CHECK under a temporary name, drops the
	// old one, and renames the new one to the declared name, for an engine
	// that commits each schema statement apart. Dropped first, a refused ADD
	// leaves the table with neither CHECK. Without it the old CHECK is
	// dropped before the new one is added.
	CheckAddedFirst bool `json:"check_added_first,omitempty"`
	// PrimaryKeyDroppedFirst reports that a plan which adds a primary key to a
	// table whose key it also removes drops the old key first, because the
	// engine holds one key per table and refuses the ADD while the old key
	// stands (ORA-02260 on Oracle). Without it the removal follows the
	// additions, as every other constraint removal does.
	PrimaryKeyDroppedFirst bool `json:"primary_key_dropped_first,omitempty"`
	// OneForeignKeyPerReference reports that a table holds one foreign key
	// for a list of columns and the reference it points at, as Oracle's does:
	// a second one answers ORA-02275 whatever its name. A plan that replaces
	// such a key with one over the same columns and reference drops the old
	// key before it adds the new one. Without it the new key is added first.
	OneForeignKeyPerReference bool `json:"one_foreign_key_per_reference,omitempty"`
}

// Enforcement is where ENFORCED and NOT ENFORCED may follow a constraint.
// The zero value takes them nowhere.
type Enforcement struct {
	// Checks reports that they may follow a CHECK.
	Checks bool `json:"checks,omitempty"`
	// ForeignKeys reports that they may follow a foreign key.
	ForeignKeys bool `json:"foreign_keys,omitempty"`
	// Once reports that a constraint takes one of them. Without it the last
	// one written wins.
	Once bool `json:"once,omitempty"`
}

// Match states how a MATCH clause after REFERENCES is read. The zero value
// takes none.
type Match struct {
	// Clause reports that MATCH SIMPLE, MATCH FULL and MATCH PARTIAL may
	// follow REFERENCES. A key without the clause is MATCH SIMPLE.
	Clause bool `json:"clause,omitempty"`
	// Partial reports that the server builds a MATCH PARTIAL key. Without
	// it MATCH PARTIAL is refused as a type the server does not implement.
	Partial bool `json:"partial,omitempty"`
	// Refusal says why MATCH FULL and MATCH PARTIAL are refused, for a
	// server that accepts them and builds a MATCH SIMPLE key. Empty reads
	// them as the key's type.
	Refusal string `json:"refusal,omitempty"`
}

// Deferral states how [NOT] DEFERRABLE and INITIALLY DEFERRED | IMMEDIATE
// are read. The zero value reads them as PostgreSQL does: after a key or a
// foreign key, in any order.
type Deferral struct {
	// Refusal says why every deferral clause is refused, NOT DEFERRABLE
	// included, for an engine with no deferrable constraints.
	Refusal string `json:"refusal,omitempty"`
	// KeyRefusal says why a deferral clause after a PRIMARY KEY, a UNIQUE or
	// a CHECK is refused, for an engine that takes one after a foreign key
	// alone. The parser writes ", not after" and the kind after it.
	KeyRefusal string `json:"key_refusal,omitempty"`
	// LoneInitiallyRefusal says why INITIALLY is refused where no [NOT]
	// DEFERRABLE comes before it.
	LoneInitiallyRefusal string `json:"lone_initially_refusal,omitempty"`
	// Checks reports that a table's CHECK takes a deferral clause after it
	// and keeps it, as Oracle's does: `CONSTRAINT c CHECK (n > 0) DEFERRABLE
	// INITIALLY DEFERRED` creates a CHECK whose rows are checked at commit.
	// Without it, a clause that defers a CHECK is refused, as PostgreSQL
	// refuses one.
	Checks bool `json:"checks,omitempty"`
}

// KeyGrammar states how the engine reads a key or an index declared in a
// table body.
type KeyGrammar struct {
	// IndexKeywords are the words that open an index inside CREATE TABLE,
	// each one of [IndexKeywordValues]. A table element that opens with
	// another of those words is a column named after it.
	IndexKeywords []string `json:"index_keywords,omitempty"`
	// IndexKeywordsNeedColumns reports that a word of IndexKeywords opens an
	// index only where a column list follows it, at once or after a name:
	// CockroachDB reads `INDEX (b)` as an index and `index text` as a
	// column.
	IndexKeywordsNeedColumns bool `json:"index_keywords_need_columns,omitempty"`
	// Options reports that a key's parts may be followed by the MySQL
	// family's index options: USING BTREE | HASH, COMMENT, a visibility
	// clause and an engine attribute. A PRIMARY KEY may then take USING
	// before its parts too, and ALTER TABLE ... ALTER INDEX changes a
	// visibility.
	Options bool `json:"options,omitempty"`
	// Visibility states which visibility clauses among the options are
	// refused.
	Visibility KeyVisibility `json:"visibility,omitzero"`
	// BareUniqueName reports that a table's UNIQUE may name its index with
	// no KEY or INDEX before the name, as `UNIQUE uq (a)`.
	BareUniqueName bool `json:"bare_unique_name,omitempty"`
	// ForeignKeyIndexName reports that FOREIGN KEY may be followed by a name
	// before its column list, as `FOREIGN KEY ix (a)`.
	ForeignKeyIndexName bool `json:"foreign_key_index_name,omitempty"`
	// ForeignKeyIndexNameNamesKey reports that the name ForeignKeyIndexName
	// reads is the key's own where no CONSTRAINT symbol names the key, so the
	// key and the index it builds both take it. Without it the name is the
	// index's alone, and the key is named as any unnamed key.
	ForeignKeyIndexNameNamesKey bool `json:"foreign_key_index_name_names_key,omitempty"`
	// FunctionalParts reports that a key part may be an expression in
	// parentheses, as `KEY k ((a + 1))`.
	FunctionalParts bool `json:"functional_parts,omitempty"`
	// RefusesNullsDistinct reports that UNIQUE NULLS [NOT] DISTINCT is
	// refused once the clause is read, for an engine that has no such
	// clause. Without it the clause is read, and a render decides it by the
	// target's capabilities.
	RefusesNullsDistinct bool `json:"refuses_nulls_distinct,omitempty"`
}

// IndexGrammar states how the engine reads CREATE INDEX, ALTER INDEX and
// DROP INDEX.
type IndexGrammar struct {
	// TrailingVisibility reports that an index may end with VISIBLE, NOT
	// VISIBLE, INVISIBLE or VISIBILITY and a share, as CockroachDB writes it:
	// in a table body after the key's parts, and in CREATE INDEX after
	// INCLUDE and WHERE.
	TrailingVisibility bool `json:"trailing_visibility,omitempty"`
	// Alter is the ALTER INDEX the engine's grammar has, as the parser reads
	// it: [AlterIndexRename] or [AlterIndexVisibility]. Empty means the
	// parser reads none.
	Alter string `json:"alter,omitempty"`
	// DropNamesTable reports that a qualified name in a DROP INDEX without ON
	// is table.index rather than schema.index, as SQL Server's backward
	// compatible spelling has it. The parser refuses such a name.
	DropNamesTable bool `json:"drop_names_table,omitempty"`
	// Storing reports that CREATE INDEX spells its INCLUDE columns STORING too,
	// as CockroachDB does.
	Storing bool `json:"storing,omitempty"`
	// Nonclustered reports that NONCLUSTERED may stand before INDEX, as T-SQL
	// writes the kind of index a plain CREATE INDEX makes.
	Nonclustered bool `json:"nonclustered,omitempty"`
}

const (
	// AlterIndexRename is PostgreSQL's ALTER INDEX [IF EXISTS] name RENAME
	// TO new_name, and ALTER INDEX name ATTACH PARTITION partition_index.
	AlterIndexRename = "rename"
	// AlterIndexVisibility is CockroachDB's ALTER INDEX table@index VISIBLE |
	// NOT VISIBLE | INVISIBLE.
	AlterIndexVisibility = "visibility"
)

// AlterIndexGrammars returns the values an [IndexGrammar.Alter] may be.
func AlterIndexGrammars() []string {
	return []string{AlterIndexRename, AlterIndexVisibility}
}

// AlterTableGrammar states how the engine reads the clauses of an ALTER
// TABLE.
type AlterTableGrammar struct {
	// AddIndex reports that ADD KEY, ADD INDEX, ADD SPATIAL and ADD FULLTEXT
	// add an index.
	AddIndex bool `json:"add_index,omitempty"`
	// RenameIndex reports that RENAME INDEX and RENAME KEY rename an index.
	RenameIndex bool `json:"rename_index,omitempty"`
	// Modify is the MODIFY clause the engine has: [ModifyDefinition], or
	// [ModifyOther] for one of another meaning, which the parser does not
	// read. Empty means the engine has none.
	Modify string `json:"modify,omitempty"`
	// AlterColumnDefinition reports that ALTER COLUMN, with COLUMN required,
	// states the column's whole new definition, as SQL Server writes it,
	// rather than one action.
	AlterColumnDefinition bool `json:"alter_column_definition,omitempty"`
	// ChecksCapabilities reports that a clause only some releases take is
	// refused where the target lacks it, by the capability key the renderer
	// asks before it writes the clause: DROP CHECK by DropCheckClause, IF
	// EXISTS on an index by DropIndexIfExists, on a constraint by
	// DropConstraintIfExists, and on a column, which no key names, by
	// ColumnGuards. Without it every one of them is read.
	ChecksCapabilities bool `json:"checks_capabilities,omitempty"`
	// ColumnGuards reports that DROP COLUMN takes IF EXISTS and ADD COLUMN
	// takes IF NOT EXISTS. The parser asks it only where ChecksCapabilities is set.
	ColumnGuards bool `json:"column_guards,omitempty"`
	// KeyUsingIndex reports that ADD [CONSTRAINT name] UNIQUE | PRIMARY KEY
	// USING INDEX index makes an existing unique index the key, its columns
	// the index's, as PostgreSQL writes it. Without it the clause is refused.
	KeyUsingIndex bool `json:"key_using_index,omitempty"`
	// AddNotNull reports that ADD [CONSTRAINT name] NOT NULL column makes a column
	// NOT NULL, under that name, as PostgreSQL 18 writes it. Without it the clause
	// is refused.
	AddNotNull bool `json:"add_not_null,omitempty"`
}

const (
	// ModifyDefinition is the MySQL family's MODIFY [COLUMN], which states a
	// column's whole new definition.
	ModifyDefinition = "definition"
	// ModifyOther is a MODIFY that means something else, such as Oracle's,
	// which changes only the attributes it states.
	ModifyOther = "other"
)

// ModifyGrammars returns the values an [AlterTableGrammar.Modify] may be.
func ModifyGrammars() []string {
	return []string{ModifyDefinition, ModifyOther}
}

// StatementGrammar states which statements of the shared grammar the engine
// writes as it does.
type StatementGrammar struct {
	// UseSelectsDatabase reports that USE selects the database an unqualified
	// table name later in the document is in.
	UseSelectsDatabase bool `json:"use_selects_database,omitempty"`
	// Extensions reports that CREATE EXTENSION creates an object of the
	// engine's own, which the extension owner reads, so a read that selected
	// no such owner says it did not. Without it the statement is refused as
	// one the engine's server refuses.
	Extensions bool `json:"extensions,omitempty"`
	// UserTypes reports that CREATE DOMAIN and CREATE TYPE ... AS (...)
	// create objects of the engine's own, which a type owner reads, so a
	// read that selected no such owner says it did not. Without it the
	// statements are refused as ones the engine's server refuses.
	UserTypes bool `json:"user_types,omitempty"`
	// GrantTargetLists reports that GRANT may name several objects, and
	// grants each privilege on each of them.
	GrantTargetLists bool `json:"grant_target_lists,omitempty"`
	// RoleIfNotExists reports that CREATE ROLE takes IF NOT EXISTS.
	RoleIfNotExists bool `json:"role_if_not_exists,omitempty"`
	// DatabaseWildcards reports that GRANT and REVOKE name every table of a
	// database as `db.*`, as MySQL does, which grants on the database: the
	// target is read as the object kind SCHEMA named db.
	DatabaseWildcards bool `json:"database_wildcards,omitempty"`
	// RoutineGrantsByName reports that GRANT and REVOKE name a function or a
	// procedure after FUNCTION or PROCEDURE by its name alone, as MySQL does:
	// the server holds one routine of each kind under a name, and an argument
	// list or the word ROUTINE there is a syntax error. Without it a routine
	// target needs its argument types, as PostgreSQL resolves an overload by
	// them.
	RoutineGrantsByName bool `json:"routine_grants_by_name,omitempty"`
}

// StatementForms states the statements the parser reads as the engine writes them,
// beside the DDL it reads on every engine. The zero value states none, and each is then
// refused.
type StatementForms struct {
	// Drops states how DROP reads each kind of object it takes beyond a table
	// and an index, which every engine's grammar has. A [DropKindTable] entry
	// states whether DROP TABLE takes a list and the clauses it adds. A kind
	// other than TABLE that is not listed is refused.
	Drops []Drop `json:"drops,omitempty"`
	// Operations are the statements the engine runs that change no schema
	// object and that the parser reads to their end, each written as the
	// engine's manual writes it, one form per opening keyword, such as
	// `SAVEPOINT <name>`. A word in capitals is that keyword, `<name>` a
	// possibly qualified name, `<names>` a comma-separated list of them, `[a|b]`
	// at most one of its alternatives, `{a|b}` exactly one, and `(a b),...` a
	// comma-separated list of what the parentheses hold. Each alternative opens
	// with a keyword of its own, so a form is read one token at a time. A
	// statement no form opens with is refused.
	Operations []string `json:"operations,omitempty"`
	// UnreadStatements are the opening keywords of further statements that
	// write rows and change no schema object, beside the ones every engine has,
	// such as MySQL's REPLACE. The parser steps over each to its semicolon, as
	// it does an INSERT, so a statement here says nothing about its own syntax.
	UnreadStatements []string `json:"unread_statements,omitempty"`
	// RenameTable reports that RENAME TABLE a TO b, with further pairs after a
	// comma, renames a table, as ALTER TABLE a RENAME TO b does.
	RenameTable bool `json:"rename_table,omitempty"`
	// OwnerTo states the kinds of object whose owner `ALTER <kind> <name>
	// OWNER TO <role>` changes, each one of [OwnerKinds]. A kind that is not
	// listed is refused. The parser reads the statement to its end, and
	// contributes a node for a kind of ManagedOwners alone.
	OwnerTo []string `json:"owner_to,omitempty"`
	// MultiWordPrivileges are the privilege names of more than one word that
	// GRANT and REVOKE take, as the engine's catalog reports them: upper-case
	// words one space apart, such as MySQL's SHOW VIEW. A privilege is read
	// as the longest of them its words spell, and one no entry spells is read
	// as its first word alone. Every name of more than one word in
	// [Dialect.AllPrivileges] is one of them.
	MultiWordPrivileges []string `json:"multi_word_privileges,omitempty"`
	// GrantClasses are the object kinds GRANT and REVOKE may name as a class
	// before `::`, in upper case, as T-SQL names SCHEMA in `ON SCHEMA::app`.
	// Another kind is not followed by `::`.
	GrantClasses []string `json:"grant_classes,omitempty"`
	// Deny reports that DENY withholds privileges from a grantee, as T-SQL's
	// does, whatever grant, schema permission or role membership gives them
	// otherwise. The statement names its privileges and its target as GRANT
	// does and its grantees after TO, and a SQL schema file declares a denied
	// privilege with it. Without it DENY is refused.
	Deny bool `json:"deny,omitempty"`
	// ManagedOwners states the kinds of OwnerTo whose owner a desired schema
	// declares and Ptah reconciles: the engine changes the owner with that
	// statement, a read reports it, and an object the engine creates belongs
	// to the role that runs the CREATE. A schema file's statement of such a
	// kind declares the owner of the object it names. A kind of OwnerTo that
	// is not listed here is read and plans nothing, and a declaration of an
	// owner for it in any source is refused.
	ManagedOwners []string `json:"managed_owners,omitempty"`
}

// Drop states how DROP reads one kind of object.
type Drop struct {
	// Kind is the kind of object: one of [DropKinds].
	Kind string `json:"kind"`
	// IfExists reports that the kind takes IF EXISTS.
	IfExists bool `json:"if_exists,omitempty"`
	// List reports that one statement may name several objects.
	List bool `json:"list,omitempty"`
	// Behavior reports that the kind takes CASCADE or RESTRICT after its names.
	Behavior bool `json:"behavior,omitempty"`
	// OnTable reports that the name is the one of a trigger on a table, so
	// `ON table` follows it, and that the statement names one trigger.
	OnTable bool `json:"on_table,omitempty"`
	// Arguments reports that a routine's name may take its argument list.
	Arguments bool `json:"arguments,omitempty"`
	// Sync reports that the statement may end in SYNC.
	Sync bool `json:"sync,omitempty"`
}

// The kinds of object a [Drop] states.
const (
	// DropKindTable is DROP TABLE, which the parser reads on every engine. An
	// entry for it states whether a list of tables is accepted and whether
	// the statement takes [Drop.Sync].
	DropKindTable = "TABLE"
	// DropKindView is DROP VIEW.
	DropKindView = "VIEW"
	// DropKindMaterializedView is DROP MATERIALIZED VIEW.
	DropKindMaterializedView = "MATERIALIZED VIEW"
	// DropKindTrigger is DROP TRIGGER.
	DropKindTrigger = "TRIGGER"
	// DropKindFunction is DROP FUNCTION.
	DropKindFunction = "FUNCTION"
	// DropKindProcedure is DROP PROCEDURE.
	DropKindProcedure = "PROCEDURE"
	// DropKindProc is T-SQL's short spelling of DROP PROCEDURE.
	DropKindProc = "PROC"
	// DropKindSequence is DROP SEQUENCE.
	DropKindSequence = "SEQUENCE"
	// DropKindType is DROP TYPE.
	DropKindType = "TYPE"
)

// DropKinds returns the values a [Drop.Kind] may be.
func DropKinds() []string {
	return []string{
		DropKindTable, DropKindView, DropKindMaterializedView, DropKindTrigger,
		DropKindFunction, DropKindProcedure, DropKindProc, DropKindSequence, DropKindType,
	}
}

// The kinds of object an [StatementForms.OwnerTo] entry may name.
const (
	// OwnerKindTable is ALTER TABLE ... OWNER TO.
	OwnerKindTable = "TABLE"
	// OwnerKindSchema is ALTER SCHEMA ... OWNER TO.
	OwnerKindSchema = "SCHEMA"
	// OwnerKindType is ALTER TYPE ... OWNER TO.
	OwnerKindType = "TYPE"
	// OwnerKindDomain is ALTER DOMAIN ... OWNER TO.
	OwnerKindDomain = "DOMAIN"
	// OwnerKindFunction is ALTER FUNCTION ... OWNER TO, which may carry an
	// argument list.
	OwnerKindFunction = "FUNCTION"
	// OwnerKindProcedure is ALTER PROCEDURE ... OWNER TO, which may carry an
	// argument list.
	OwnerKindProcedure = "PROCEDURE"
	// OwnerKindSequence is ALTER SEQUENCE ... OWNER TO.
	OwnerKindSequence = "SEQUENCE"
	// OwnerKindView is ALTER VIEW ... OWNER TO.
	OwnerKindView = "VIEW"
	// OwnerKindMaterializedView is ALTER MATERIALIZED VIEW ... OWNER TO.
	OwnerKindMaterializedView = "MATERIALIZED VIEW"
	// OwnerKindPublication is ALTER PUBLICATION ... OWNER TO, of a logical
	// replication publication, which has no schema.
	OwnerKindPublication = "PUBLICATION"
)

// OwnerKinds returns the values an entry of [StatementForms.OwnerTo] may be.
func OwnerKinds() []string {
	return []string{
		OwnerKindTable, OwnerKindSchema, OwnerKindType, OwnerKindDomain, OwnerKindFunction,
		OwnerKindProcedure, OwnerKindSequence, OwnerKindView, OwnerKindMaterializedView, OwnerKindPublication,
	}
}

// RoutineGrammar states how the engine reads a routine, a trigger and an
// anonymous code block.
type RoutineGrammar struct {
	// Body is how CREATE FUNCTION and CREATE PROCEDURE write a routine:
	// [RoutineQuotedBody], [RoutineCompoundStatement] or [RoutineTransactSQL].
	// Empty reads a routine best effort, as a statement whose end the parser
	// finds.
	Body string `json:"body,omitempty"`
	// TriggerConditions reports that a trigger takes a WHEN condition in
	// parentheses and a REFERENCING clause naming its transition tables.
	TriggerConditions bool `json:"trigger_conditions,omitempty"`
	// TriggerStatementBody reports that a trigger's body may be one statement
	// rather than a BEGIN ... END block.
	TriggerStatementBody bool `json:"trigger_statement_body,omitempty"`
	// Trigger is how CREATE TRIGGER is written: [TriggerTransactSQL], or empty
	// for the order the other engines share, `CREATE TRIGGER name {BEFORE |
	// AFTER | INSTEAD OF} events ON table`. TriggerConditions and
	// TriggerStatementBody refine that order, so a registry refuses either of
	// them beside [TriggerTransactSQL].
	Trigger string `json:"trigger,omitempty"`
	// DefaultDoBlockLanguage is the language of a DO block that writes no
	// LANGUAGE clause, in lower case, such as plpgsql. The parser gives such a
	// block this language. Empty leaves its language unknown. Only the
	// [RoutineQuotedBody] grammar reads a DO block, so a registry refuses the
	// field with any other.
	DefaultDoBlockLanguage string `json:"default_do_block_language,omitempty"`
}

const (
	// RoutineQuotedBody is PostgreSQL's grammar: the body is a string
	// constant, usually dollar-quoted, in the language a LANGUAGE clause
	// names, read with the string rules of the head of the dialect's family.
	// A DO block is read as an anonymous routine of the same grammar.
	RoutineQuotedBody = "quoted-body"
	// RoutineCompoundStatement is the MySQL family's grammar: the body is one
	// statement, often a BEGIN ... END block, after a DEFINER and the
	// routine's characteristics.
	RoutineCompoundStatement = "compound-statement"
	// RoutineTransactSQL is SQL Server's grammar: CREATE [OR ALTER] FUNCTION
	// and PROC[EDURE], with the body after AS.
	RoutineTransactSQL = "transact-sql"
)

// RoutineGrammars returns the values a [RoutineGrammar.Body] may be.
func RoutineGrammars() []string {
	return []string{RoutineQuotedBody, RoutineCompoundStatement, RoutineTransactSQL}
}

// TriggerTransactSQL is SQL Server's CREATE TRIGGER: `CREATE [OR ALTER]
// TRIGGER [schema.]name ON table [WITH options] {FOR | AFTER | INSTEAD OF}
// events AS body`. The table comes before the timing, FOR is AFTER, the
// events are separated by commas, a trigger fires once per statement, and the
// body runs to the end of its batch.
const TriggerTransactSQL = "transact-sql"

// TriggerGrammars returns the values a [RoutineGrammar.Trigger] may be.
func TriggerGrammars() []string {
	return []string{TriggerTransactSQL}
}

// KeyVisibility states which visibility clauses among a key's options the
// engine refuses.
type KeyVisibility struct {
	// Refused are the clauses refused, each one of
	// [KeyVisibilityClauses].
	Refused []string `json:"refused,omitempty"`
	// Refusal says why. It is set exactly when Refused is.
	Refusal string `json:"refusal,omitempty"`
}

// IndexKeywordValues returns the words a [KeyGrammar.IndexKeywords] entry may
// be.
func IndexKeywordValues() []string {
	return []string{"KEY", "INDEX", "FULLTEXT", "SPATIAL"}
}

// KeyVisibilityClauses returns the clauses a [KeyVisibility.Refused] entry
// may be.
func KeyVisibilityClauses() []string {
	return []string{"VISIBLE", "INVISIBLE", "IGNORED", "NOT IGNORED"}
}

// validateGrammar checks the enumerated values of the grammar fields, and
// refuses a combination no parser could read: a refusal that leaves another
// field of its group unreachable, or a field that refines a clause the
// engine does not take.
func (d Dialect) validateGrammar() error {
	for _, check := range []func(Grammar) error{
		validateColumnGrammar, validateConstraintGrammar, validateKeyGrammar, validateGrammarNames, validateRoutineGrammar,
		validateStatementForms, validateMultiWordPrivileges,
	} {
		if err := check(d.Grammar); err != nil {
			return fmt.Errorf("%w: dialect %q: grammar: %w", ErrInvalidDescriptor, d.Name, err)
		}
	}
	return nil
}

// validateColumnGrammar checks the niladic function names, the computed
// kinds, the relative time literals and the column constraint kinds.
func validateColumnGrammar(grammar Grammar) error {
	if err := validateFunctionNames("niladic function", grammar.Columns.NiladicFunctions); err != nil {
		return err
	}
	if err := validateFunctionNames("relative time literal", grammar.Columns.RelativeTimeLiterals); err != nil {
		return err
	}
	if err := validateFunctionNames("empty call function", grammar.Columns.EmptyCallFunctions); err != nil {
		return err
	}
	if err := validateFunctionNames("sequence pseudocolumn", grammar.Columns.SequencePseudocolumns); err != nil {
		return err
	}
	if err := validateClause("next value clause", grammar.Columns.NextValueClause); err != nil {
		return err
	}
	computed := make([]string, len(grammar.Columns.ComputedKinds))
	for i, kind := range grammar.Columns.ComputedKinds {
		computed[i] = kind.Keyword
	}
	if err := validateFunctionNames("computed kind", computed); err != nil {
		return err
	}
	constraint := grammar.Columns.Constraint
	columnKinds := []string{ConstraintKindCheck, ConstraintKindReferences}
	switch {
	case constraint.Define != "" && !slices.Contains(columnKinds, constraint.Define):
		return fmt.Errorf("column constraint kind %q is not one of %q", constraint.Define, columnKinds)
	case constraint.Modify != "" && !slices.Contains(columnKinds, constraint.Modify):
		return fmt.Errorf("column constraint kind %q in MODIFY is not one of %q", constraint.Modify, columnKinds)
	case constraint.Modify != "" && constraint.Define == "":
		return errors.New("a MODIFY column constraint kind needs a column constraint kind")
	case (grammar.Columns.EmptyCallRefusal == "") != (len(grammar.Columns.EmptyCallFunctions) == 0):
		return errors.New("empty call functions and an empty call refusal are set together")
	case grammar.Columns.DefaultPredicates.readsSome() && grammar.Columns.DefaultPredicates.Refusal == "":
		return errors.New("default predicates that name the forms read need a refusal of the others")
	default:
		return nil
	}
}

// validateConstraintGrammar checks the MATCH and deferral fields.
func validateConstraintGrammar(grammar Grammar) error {
	match := grammar.Constraints.Match
	deferral := grammar.Constraints.Deferral
	switch {
	case (match.Partial || match.Refusal != "") && !match.Clause:
		return errors.New("a MATCH type or refusal needs the MATCH clause")
	case match.Partial && match.Refusal != "":
		return errors.New("MATCH PARTIAL cannot be both built and refused")
	case deferral.Refusal != "" && (deferral.KeyRefusal != "" || deferral.LoneInitiallyRefusal != ""):
		return errors.New("a deferral refusal leaves no deferral clause to refuse after a key or before DEFERRABLE")
	case deferral.Checks && (deferral.Refusal != "" || deferral.KeyRefusal != ""):
		return errors.New("a deferral clause after a CHECK cannot be both kept and refused")
	default:
		return nil
	}
}

// validateKeyGrammar checks the index keywords and the refused visibility
// clauses.
func validateKeyGrammar(grammar Grammar) error {
	keys := grammar.Keys
	if err := validateWords("index keyword", keys.IndexKeywords, IndexKeywordValues()); err != nil {
		return err
	}
	if keys.IndexKeywordsNeedColumns && len(keys.IndexKeywords) == 0 {
		return errors.New("index keywords that need columns name no index keyword")
	}
	if err := validateWords("visibility clause", keys.Visibility.Refused, KeyVisibilityClauses()); err != nil {
		return err
	}
	if (len(keys.Visibility.Refused) == 0) != (keys.Visibility.Refusal == "") {
		return errors.New("a refused visibility clause and its refusal are set together")
	}
	if keys.ForeignKeyIndexNameNamesKey && !keys.ForeignKeyIndexName {
		return errors.New("a foreign key index name that names the key is stated, and no foreign key index name is read")
	}
	return nil
}

// validateGrammarNames checks the fields that name one of a declared set of
// grammars.
func validateGrammarNames(grammar Grammar) error {
	for _, field := range []struct {
		label, value string
		allowed      []string
	}{
		{"ALTER INDEX", grammar.Indexes.Alter, AlterIndexGrammars()},
		{"MODIFY", grammar.AlterTable.Modify, ModifyGrammars()},
		{"routine body", grammar.Routines.Body, RoutineGrammars()},
		{"trigger", grammar.Routines.Trigger, TriggerGrammars()},
	} {
		if field.value != "" && !slices.Contains(field.allowed, field.value) {
			return fmt.Errorf("%s grammar %q is not one of %q", field.label, field.value, field.allowed)
		}
	}
	return nil
}

// validateRoutineGrammar checks the default language of a DO block: it is
// stated only with the grammar that reads a DO block, and in the form the
// parser gives a block, one lower-case word. It also refuses a refinement of
// the shared trigger order beside a trigger grammar that does not read it.
func validateRoutineGrammar(grammar Grammar) error {
	if trigger := grammar.Routines.Trigger; trigger != "" {
		switch {
		case grammar.Routines.TriggerConditions:
			return fmt.Errorf("trigger conditions are stated, and trigger grammar %q reads no WHEN or REFERENCING", trigger)
		case grammar.Routines.TriggerStatementBody:
			return fmt.Errorf("a one-statement trigger body is stated, and trigger grammar %q reads its own body", trigger)
		}
	}
	language := grammar.Routines.DefaultDoBlockLanguage
	switch {
	case language == "":
		return nil
	case grammar.Routines.Body != RoutineQuotedBody:
		return fmt.Errorf("default DO-block language %q is stated, and routine grammar %q reads no DO block",
			language, grammar.Routines.Body)
	case language != strings.ToLower(language) || strings.ContainsFunc(language, unicode.IsSpace):
		return fmt.Errorf("default DO-block language %q is not one lower-case word", language)
	default:
		return nil
	}
}

// validateStatementForms checks the kinds a DROP states, each once, and each
// form of a statement, one per opening keyword.
func validateStatementForms(grammar Grammar) error {
	statements := grammar.Forms
	kinds := make([]string, 0, len(statements.Drops))
	for _, drop := range statements.Drops {
		kinds = append(kinds, drop.Kind)
	}
	if err := validateWords("DROP kind", kinds, DropKinds()); err != nil {
		return err
	}
	if err := validateWords("OWNER TO kind", statements.OwnerTo, OwnerKinds()); err != nil {
		return err
	}
	if err := validateWords("managed owner kind", statements.ManagedOwners, statements.OwnerTo); err != nil {
		return err
	}
	openings := make([]string, 0, len(statements.Operations))
	for _, form := range statements.Operations {
		terms, err := statementform.Parse(form)
		if err != nil {
			return err
		}
		if slices.Contains(openings, terms[0].Word) {
			return fmt.Errorf("two statement forms open with %s", terms[0].Word)
		}
		openings = append(openings, terms[0].Word)
	}
	return nil
}

// validateFunctionNames refuses a function name of names that is not one
// upper-case word, which the parser could never match, and one written twice.
// label names the list in the error.
func validateFunctionNames(label string, names []string) error {
	for i, name := range names {
		if name == "" || name != strings.ToUpper(name) || strings.IndexFunc(name, func(r rune) bool {
			return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_'
		}) >= 0 {
			return fmt.Errorf("%s %q is not one upper-case word", label, name)
		}
		if slices.Contains(names[:i], name) {
			return fmt.Errorf("%s %q is written twice", label, name)
		}
	}
	return nil
}

// validateClause refuses a clause that is not upper-case words separated by
// one space. The empty clause is accepted.
func validateClause(label, clause string) error {
	words := strings.Fields(clause)
	if strings.Join(words, " ") != clause {
		return fmt.Errorf("%s %q is not words separated by one space", label, clause)
	}
	for _, word := range words {
		if err := validateFunctionNames(label+" word", []string{word}); err != nil {
			return err
		}
	}
	return nil
}

// validateWords refuses a word of words that is not one of allowed, and one
// written twice.
func validateWords(label string, words, allowed []string) error {
	for i, word := range words {
		if !slices.Contains(allowed, word) {
			return fmt.Errorf("%s %q is not one of %q", label, word, allowed)
		}
		if slices.Contains(words[:i], word) {
			return fmt.Errorf("%s %q is written twice", label, word)
		}
	}
	return nil
}
