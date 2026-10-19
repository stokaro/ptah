package descriptor

import (
	"errors"
	"fmt"
	"slices"
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
	// Routines states how a routine, a trigger and an anonymous code block
	// are read.
	Routines RoutineGrammar `json:"routines,omitzero"`
}

// ColumnGrammar states how the engine reads a column definition.
type ColumnGrammar struct {
	// CurrentTimeKeywords reports that CURRENT_TIMESTAMP, CURRENT_DATE,
	// CURRENT_TIME, LOCALTIMESTAMP and LOCALTIME are keywords that take no
	// empty argument list, so a default written with one of them is kept as
	// written. Without it a default of CURRENT_TIMESTAMP, CURRENT_DATE or
	// CURRENT_TIME is read as a call, with `()` after it.
	CurrentTimeKeywords bool `json:"current_time_keywords,omitempty"`
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
}

const (
	// AlterIndexRename is PostgreSQL's ALTER INDEX [IF EXISTS] name RENAME
	// TO new_name.
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
		validateColumnGrammar, validateConstraintGrammar, validateKeyGrammar, validateGrammarNames,
	} {
		if err := check(d.Grammar); err != nil {
			return fmt.Errorf("%w: dialect %q: grammar: %w", ErrInvalidDescriptor, d.Name, err)
		}
	}
	return nil
}

// validateColumnGrammar checks the column constraint kinds.
func validateColumnGrammar(grammar Grammar) error {
	constraint := grammar.Columns.Constraint
	columnKinds := []string{ConstraintKindCheck, ConstraintKindReferences}
	switch {
	case constraint.Define != "" && !slices.Contains(columnKinds, constraint.Define):
		return fmt.Errorf("column constraint kind %q is not one of %q", constraint.Define, columnKinds)
	case constraint.Modify != "" && !slices.Contains(columnKinds, constraint.Modify):
		return fmt.Errorf("column constraint kind %q in MODIFY is not one of %q", constraint.Modify, columnKinds)
	case constraint.Modify != "" && constraint.Define == "":
		return errors.New("a MODIFY column constraint kind needs a column constraint kind")
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
	} {
		if field.value != "" && !slices.Contains(field.allowed, field.value) {
			return fmt.Errorf("%s grammar %q is not one of %q", field.label, field.value, field.allowed)
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
