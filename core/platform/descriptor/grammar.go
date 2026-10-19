package descriptor

import (
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
// constraint takes no ENFORCED, NOT VALID or MATCH clause, and every deferral
// clause is read as PostgreSQL reads it.
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
	invalid := func(format string, args ...any) error {
		return fmt.Errorf("%w: dialect %q: grammar: %s", ErrInvalidDescriptor, d.Name, fmt.Sprintf(format, args...))
	}
	grammar := d.Grammar
	constraint := grammar.Columns.Constraint
	columnKinds := []string{ConstraintKindCheck, ConstraintKindReferences}
	switch {
	case constraint.Define != "" && !slices.Contains(columnKinds, constraint.Define):
		return invalid("column constraint kind %q is not one of %q", constraint.Define, columnKinds)
	case constraint.Modify != "" && !slices.Contains(columnKinds, constraint.Modify):
		return invalid("column constraint kind %q in MODIFY is not one of %q", constraint.Modify, columnKinds)
	case constraint.Modify != "" && constraint.Define == "":
		return invalid("a MODIFY column constraint kind needs a column constraint kind")
	}
	match := grammar.Constraints.Match
	switch {
	case (match.Partial || match.Refusal != "") && !match.Clause:
		return invalid("a MATCH type or refusal needs the MATCH clause")
	case match.Partial && match.Refusal != "":
		return invalid("MATCH PARTIAL cannot be both built and refused")
	}
	if deferral := grammar.Constraints.Deferral; deferral.Refusal != "" &&
		(deferral.KeyRefusal != "" || deferral.LoneInitiallyRefusal != "") {
		return invalid("a deferral refusal leaves no deferral clause to refuse after a key or before DEFERRABLE")
	}
	keys := grammar.Keys
	if err := validateWords("index keyword", keys.IndexKeywords, IndexKeywordValues()); err != nil {
		return invalid("%v", err)
	}
	if keys.IndexKeywordsNeedColumns && len(keys.IndexKeywords) == 0 {
		return invalid("index keywords that need columns name no index keyword")
	}
	if err := validateWords("visibility clause", keys.Visibility.Refused, KeyVisibilityClauses()); err != nil {
		return invalid("%v", err)
	}
	if (len(keys.Visibility.Refused) == 0) != (keys.Visibility.Refusal == "") {
		return invalid("a refused visibility clause and its refusal are set together")
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
