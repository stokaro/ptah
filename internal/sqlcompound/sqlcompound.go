// Package sqlcompound answers one question: does this semicolon end the
// statement, or is it a line of a routine body?
//
// It exists because more than one scanner has to answer it. [core/sqlutil]
// splits SQL two ways -- into executable statements and into the source text
// each statement was written as -- and [migration/lint] splits it a third way,
// over its own tokens, because it also has to carry byte offsets, line numbers
// and the suppression directives that precede a statement. Three scanners with
// three copies of the rule is three chances to disagree, and they did: the
// linter cut `CREATE PROCEDURE p() BEGIN DELETE FROM audit; DROP TABLE users;
// END` into fragments, so the routine reached the change model as nothing at
// all and its body reached the rules as statements the migration performs
// (stokaro/ptah#2069).
//
// The state is fed words, not tokens, so a scanner with its own token type can
// use it without adopting another one. What counts as a word is the caller's
// decision, and it matters: a quoted `"BEGIN"` is an identifier and not the
// keyword, so a caller that keeps its quotes in the text passes it through
// safely, and one that strips them must not.
//
// YQL needs punctuation as well, because a lambda's body sits between braces,
// so a scanner that splits YQL also feeds [State.Symbol] every operator token.
// For every other grammar Symbol does nothing.
//
// Which grammar a dialect's bodies are written in is the dialect
// descriptor's Scripts.Bodies, so no engine is named here.
package sqlcompound

import (
	"strings"

	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/descriptortable"
)

// State tracks one scanner's position relative to a compound routine body.
//
// The zero value is a state for no dialect, which recognizes the dialect-blind
// forms -- `CREATE FUNCTION … BEGIN … END` -- and none of the ones that need a
// grammar to read: T-SQL's `AS`-opened body, PL/SQL's `IS`/`AS`, the PL/SQL
// terminator that belongs to the block rather than to the client, and YQL's
// actions and lambdas.
type State struct {
	bodies           descriptor.Bodies
	createPrefix     createStatementPrefix
	createObject     string
	inCompoundCreate bool
	// routineBodyOpener records that the body was opened by a keyword rather
	// than by BEGIN -- T-SQL's AS, PL/SQL's IS or AS -- so a semicolon inside
	// it is kept until the closing END even before any BEGIN is seen.
	routineBodyOpener bool
	// plsqlBody narrows that to the PL/SQL case, where the semicolon after the
	// closing END belongs to the block rather than to the client.
	plsqlBody bool
	// terminatorBelongsToStatement is set for the one semicolon that closed a
	// PL/SQL routine, so a flush can write it back before the reset.
	terminatorBelongsToStatement bool
	compoundDepth                int
	caseDepth                    int
	pendingEndKeyword            bool
	pendingCaseEndKeyword        bool
	// pendingBegin holds a T-SQL BEGIN until the word after it says whether
	// it opens a block; see [State.transactSQLWord].
	pendingBegin bool
	// blocksClosed records that a T-SQL END followed by a word closed the
	// last open block, so the next semicolon ends the routine as it would
	// have ended after "END;".
	blocksClosed bool
	// yql is the YQL position, which shares nothing with the routine bodies
	// above.
	yql yqlBodies
}

// yqlBodies tracks the YQL constructs whose body holds statements:
//
//   - DEFINE ACTION and DEFINE SUBQUERY, closed by END DEFINE;
//   - DO BEGIN, the inline action that EVALUATE FOR, EVALUATE IF, a plain DO
//     and CREATE VIEW ... AS DO all write, closed by END DO;
//   - a lambda body, ($x) -> { ...; RETURN ...; }, between braces.
//
// The grammar is SQLv1Antlr4.g.in: define_action_or_subquery_stmt,
// inline_action and lambda. Each keyword pair has to be adjacent words:
// DEFINE alone opens MATCH_RECOGNIZE's pattern definitions, and BEGIN and DO
// are ordinary names elsewhere. END alone closes a CASE, and a CASE may end
// right before the DO of an IF, as in EVALUATE IF CASE ... END DO BEGIN, so
// CASE expressions are counted and their END pairs with nothing. Measured on
// YDB 26.2.1.14, CASE and END are also names after AS and after a dot
// (SELECT 1 AS case; SELECT t.case FROM ...), where they count for nothing.
type yqlBodies struct {
	blocks   int
	braces   int
	cases    int
	previous string
	// named records that the last token was AS or a dot, after which a word
	// is a name.
	named bool
}

func (b *yqlBodies) word(word string) {
	previous, named := b.previous, b.named
	b.previous, b.named = word, word == "AS"
	if named && (word == "CASE" || word == "END") {
		b.previous = ""
		return
	}
	switch {
	case word == "CASE":
		b.cases++
	case word == "END" && b.cases > 0:
		b.cases--
		b.previous = ""
	case previous == "DEFINE" && (word == "ACTION" || word == "SUBQUERY"):
		b.blocks++
	case previous == "DO" && word == "BEGIN":
		b.blocks++
	case previous == "END" && (word == "DEFINE" || word == "DO") && b.blocks > 0:
		b.blocks--
	}
}

// separate records a semicolon, which ends a statement inside a body, so the
// words on either side of it pair with nothing.
func (b *yqlBodies) separate() {
	b.previous = ""
	b.named = false
}

// symbol records punctuation. It separates the words on either side of it as
// a semicolon does: measured on YDB 26.2.1.14, SELECT do, begin FROM ... is
// two columns, and SELECT end, define FROM ... inside an action closes
// nothing.
func (b *yqlBodies) symbol(value string) {
	b.previous = ""
	b.named = value == "."
	switch value {
	case "{":
		b.braces++
	case "}":
		if b.braces > 0 {
			b.braces--
		}
	}
}

func (b *yqlBodies) inside() bool {
	return b.blocks > 0 || b.braces > 0
}

// New returns the state for a dialect, which reads the bodies of the grammar
// the dialect's descriptor names in Scripts.Bodies. A caller may pass
// whatever spelling it was given; an unknown dialect reads the dialect-blind
// forms, as the zero value does.
func New(dialect string) State {
	return State{bodies: bodiesByDialect.Get(dialect)}
}

// bodiesByDialect holds every dialect's Scripts.Bodies. A splitter builds a
// state for every script it reads, so the grammars are read once per
// registry rather than copied out of a descriptor per call.
var bodiesByDialect = descriptortable.New(func(described descriptor.Dialect) descriptor.Bodies {
	return described.Scripts.Bodies
})

// Reset returns to the start of a statement, keeping the grammar.
func (s *State) Reset() {
	bodies := s.bodies
	*s = State{bodies: bodies}
}

// Word feeds one identifier-like word, in the spelling it was written.
func (s *State) Word(value string) {
	value = strings.ToUpper(value)
	if s.bodies == descriptor.BodiesYQL {
		s.yql.word(value)
		return
	}
	if !s.inCompoundCreate {
		s.observeCreatePrefix(value)
		return
	}
	if s.transactSQL() {
		s.transactSQLWord(value)
		return
	}

	switch value {
	case "CASE":
		if s.pendingEndKeyword || s.pendingCaseEndKeyword {
			s.pendingEndKeyword = false
			s.pendingCaseEndKeyword = false
			return
		}
		s.caseDepth++
	case "BEGIN":
		s.compoundDepth++
		s.pendingEndKeyword = false
		s.pendingCaseEndKeyword = false
	case "END":
		if s.caseDepth > 0 {
			s.caseDepth--
			s.pendingEndKeyword = false
			s.pendingCaseEndKeyword = true
			return
		}
		s.pendingEndKeyword = true
		s.pendingCaseEndKeyword = false
	default:
		if s.pendingEndKeyword && isEndContinuationKeyword(value) {
			s.pendingEndKeyword = false
		}
		s.pendingCaseEndKeyword = false
	}
}

// transactSQLWord follows the blocks of a T-SQL routine body, which differ from
// the other dialects' in what a BEGIN and an END mean.
//
// BEGIN TRAN, BEGIN TRANSACTION, BEGIN DISTRIBUTED TRANSACTION, BEGIN DIALOG
// and BEGIN CONVERSATION TIMER open no block, and END CONVERSATION closes
// none, so a BEGIN and an END each wait for the word after them. An END that
// closes a block closes it there: T-SQL ends no block with a word of its own
// as `END IF` does elsewhere, and writes `END TRY BEGIN CATCH` with no
// semicolon between the two. When that END closed the last open block, the
// next semicolon ends the routine, as the one in "END;" does. The answers are
// the ones the parser and the SQL Server owner use (stokaro/ptah#4461).
func (s *State) transactSQLWord(value string) {
	if s.pendingBegin {
		s.pendingBegin = false
		if SQLServerBeginOpensBlock(value) {
			s.compoundDepth++
			s.blocksClosed = false
		}
	}
	if s.pendingEndKeyword {
		s.pendingEndKeyword = false
		if SQLServerEndClosesBlock(value) && s.compoundDepth > 0 {
			s.compoundDepth--
			s.blocksClosed = s.compoundDepth == 0
		}
	}
	switch value {
	case "CASE":
		s.caseDepth++
	case "BEGIN":
		s.pendingBegin = true
	case "END":
		if s.caseDepth > 0 {
			s.caseDepth--
			return
		}
		s.pendingEndKeyword = true
	}
}

// Symbol feeds one punctuation token, such as `{` or `)`. Only YQL reads
// punctuation, for the braces around a lambda's body; for every other grammar
// it does nothing.
func (s *State) Symbol(value string) {
	if s.bodies == descriptor.BodiesYQL {
		s.yql.symbol(value)
	}
}

type createStatementPrefix int

const (
	createPrefixNone createStatementPrefix = iota
	createPrefixCreate
	createPrefixCreateObject
	createPrefixCreateObjectBeforeBody
)

func (s *State) observeCreatePrefix(value string) {
	switch s.createPrefix {
	case createPrefixNone:
		if value == "CREATE" {
			s.createPrefix = createPrefixCreate
		}
		return

	case createPrefixCreate:
		if s.isCompoundCreateObject(value) {
			s.createPrefix = createPrefixCreateObject
			s.createObject = value
			return
		}
		if value == "OR" || value == "ALTER" || value == "REPLACE" || value == "DEFINER" {
			return
		}
		s.createPrefix = createPrefixNone
		return

	case createPrefixCreateObject:
		s.createPrefix = createPrefixCreateObjectBeforeBody
		return

	case createPrefixCreateObjectBeforeBody:
		if s.transactSQLRoutineObject() && value == "AS" {
			s.inCompoundCreate = true
			s.routineBodyOpener = true
			s.pendingEndKeyword = false
			return
		}
		// PL/SQL opens a routine body with IS or AS, and what follows may be a
		// declaration section rather than BEGIN. Waiting for BEGIN split
		// `FUNCTION f RETURN NUMBER IS x NUMBER := 0; BEGIN ... END;` at the
		// semicolon after the declaration, and the four fragments that came out
		// are four statements the server refuses.
		if s.plsqlRoutineObject() && (value == "IS" || value == "AS") {
			s.inCompoundCreate = true
			s.routineBodyOpener = true
			s.plsqlBody = true
			s.pendingEndKeyword = false
			return
		}
		if value == "BEGIN" {
			s.inCompoundCreate = true
			s.compoundDepth = 1
			s.pendingEndKeyword = false
			// A trigger reaches the body this way rather than through the arm
			// above: its header carries no IS. The block still ends with the
			// semicolon that belongs to it, and a trigger handed over without
			// one is created INVALID -- measured, and invisible from
			// USER_TRIGGERS, which reports it ENABLED all the same.
			s.plsqlBody = s.plsql()
		}
		return
	}
}

func (s State) isCompoundCreateObject(value string) bool {
	switch value {
	case "FUNCTION", "PROCEDURE", "TRIGGER":
		return true
	case "PROC":
		return s.transactSQL()
	default:
		return false
	}
}

func (s State) transactSQLRoutineObject() bool {
	return s.transactSQL() &&
		(s.createObject == "FUNCTION" || s.createObject == "PROC" || s.createObject == "PROCEDURE" || s.createObject == "TRIGGER")
}

// plsqlRoutineObject reports whether the CREATE being scanned is one whose
// body PL/SQL opens with IS or AS.
//
// A TRIGGER is not in the set, and it is the one exclusion worth stating. Its
// body is opened by BEGIN, which is why Oracle triggers split correctly before
// this arm existed -- and its HEADER carries a standalone AS of its own, in
// `REFERENCING OLD AS o NEW AS n`, which opens nothing. A trigger whose body is
// a CALL has neither BEGIN nor END, so treating that AS as the opener would
// keep every semicolon after it and swallow the rest of the file into one
// statement.
func (s State) plsqlRoutineObject() bool {
	return s.plsql() &&
		(s.createObject == "FUNCTION" || s.createObject == "PROCEDURE")
}

func (s State) transactSQL() bool {
	return s.bodies == descriptor.BodiesTransactSQL
}

func (s State) plsql() bool {
	return s.bodies == descriptor.BodiesPLSQL
}

func isEndContinuationKeyword(value string) bool {
	switch value {
	case "IF", "LOOP", "REPEAT", "WHILE", "CASE":
		return true
	default:
		return false
	}
}

// TerminatorBelongsToStatement reports that the semicolon just declined by
// [State.KeepSemicolonInsideStatement] is part of what was written rather than
// the client's terminator. It is only ever true for a PL/SQL block, whose own
// syntax requires the semicolon -- which is why Oracle's client terminates such
// a block with a `/` on the next line instead.
//
// Handing an Oracle server `... END` without it is not an error the driver
// reports: measured on 23.26.2.0.0 through go-ora, CREATE OR REPLACE FUNCTION
// returned no error, USER_OBJECTS reported the function INVALID with PLS-00103
// in USER_ERRORS, and USER_PROCEDURES did not list it at all -- so an apply
// reported success and left a routine nothing could call.
func (s State) TerminatorBelongsToStatement() bool {
	return s.terminatorBelongsToStatement
}

// KeepSemicolonInsideStatement reports whether the semicolon now being read is
// body text rather than a statement terminator. It advances the state, so it is
// called once per semicolon.
func (s *State) KeepSemicolonInsideStatement() bool {
	if s.bodies == descriptor.BodiesYQL {
		// A semicolon separates the words on either side of it as well, so
		// END; DO is not END DO.
		s.yql.separate()
		return s.yql.inside()
	}
	if !s.inCompoundCreate {
		return false
	}
	if s.blocksClosed {
		s.inCompoundCreate = false
		return false
	}
	if s.routineBodyOpener && !s.pendingEndKeyword {
		return true
	}
	if !s.pendingEndKeyword {
		return true
	}
	if s.compoundDepth > 0 {
		s.compoundDepth--
	}
	s.pendingEndKeyword = false
	if s.compoundDepth == 0 {
		s.inCompoundCreate = false
		s.terminatorBelongsToStatement = s.plsqlBody
		return false
	}
	return true
}

// SQLServerBeginOpensBlock reports whether a T-SQL BEGIN followed by the word
// next opens a block that an END closes. BEGIN TRAN, BEGIN TRANSACTION, BEGIN
// DISTRIBUTED TRANSACTION, BEGIN DIALOG and BEGIN CONVERSATION TIMER are
// statements of their own and open nothing.
//
// Three readers ask it and must agree: the parser, which finds where a CREATE
// PROCEDURE or CREATE FUNCTION ends; [State], which decides the same for the
// migration splitters; and the SQL Server owner, which splits the body inside
// it. A second list in any of them would let it read an END as closing a block
// the others never opened.
func SQLServerBeginOpensBlock(next string) bool {
	switch strings.ToUpper(next) {
	case "CONVERSATION", "DIALOG", "DISTRIBUTED", "TRAN", "TRANSACTION":
		return false
	default:
		return true
	}
}

// SQLServerEndClosesBlock reports whether a T-SQL END followed by the word
// next closes a block. END CONVERSATION is a statement of its own and closes
// nothing. The readers of [SQLServerBeginOpensBlock] ask it for the same
// reason.
func SQLServerEndClosesBlock(next string) bool {
	return !strings.EqualFold(next, "CONVERSATION")
}
