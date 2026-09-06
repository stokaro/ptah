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
package sqlcompound

import (
	"strings"

	"ptah.run/core/platform"
)

// State tracks one scanner's position relative to a compound routine body.
//
// The zero value is a state for no dialect, which recognizes the dialect-blind
// forms -- `CREATE FUNCTION … BEGIN … END` -- and none of the ones that need a
// dialect to read: T-SQL's `AS`-opened body, PL/SQL's `IS`/`AS`, and the
// PL/SQL terminator that belongs to the block rather than to the client.
type State struct {
	dialect          string
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
}

// New returns the state for a dialect. The dialect is normalized here, so a
// caller may pass whatever spelling it was given.
func New(dialect string) State {
	return State{dialect: platform.NormalizeDialect(dialect)}
}

// Reset returns to the start of a statement, keeping the dialect.
func (s *State) Reset() {
	dialect := s.dialect
	*s = State{dialect: dialect}
}

// Word feeds one identifier-like word, in the spelling it was written.
func (s *State) Word(value string) {
	value = strings.ToUpper(value)
	if !s.inCompoundCreate {
		s.observeCreatePrefix(value)
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
		if s.isSQLServerRoutineObject() && value == "AS" {
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
		if s.isOracleRoutineObject() && (value == "IS" || value == "AS") {
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
			s.plsqlBody = s.isOracle()
		}
		return
	}
}

func (s State) isCompoundCreateObject(value string) bool {
	switch value {
	case "FUNCTION", "PROCEDURE", "TRIGGER":
		return true
	case "PROC":
		return s.isSQLServer()
	default:
		return false
	}
}

func (s State) isSQLServerRoutineObject() bool {
	return s.isSQLServer() &&
		(s.createObject == "FUNCTION" || s.createObject == "PROC" || s.createObject == "PROCEDURE" || s.createObject == "TRIGGER")
}

// isOracleRoutineObject reports whether the CREATE being scanned is one whose
// body PL/SQL opens with IS or AS.
//
// A TRIGGER is not in the set, and it is the one exclusion worth stating. Its
// body is opened by BEGIN, which is why Oracle triggers split correctly before
// this arm existed -- and its HEADER carries a standalone AS of its own, in
// `REFERENCING OLD AS o NEW AS n`, which opens nothing. A trigger whose body is
// a CALL has neither BEGIN nor END, so treating that AS as the opener would
// keep every semicolon after it and swallow the rest of the file into one
// statement.
func (s State) isOracleRoutineObject() bool {
	return s.isOracle() &&
		(s.createObject == "FUNCTION" || s.createObject == "PROCEDURE")
}

func (s State) isSQLServer() bool {
	return s.dialect == platform.SQLServer
}

func (s State) isOracle() bool {
	return s.dialect == platform.Oracle
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
	if !s.inCompoundCreate {
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
