package sqlserverbatch

import (
	"slices"
)

// scan follows one simple statement while [parser.simple] reads it, holding
// what decides whether a statement word continues it rather than starting the
// next statement.
//
// Each continuation is a place the grammar of the statement takes the word,
// so the server cannot read a new statement there: an UPDATE has one SET
// clause and needs it, an INSERT needs one source, the action of ALTER TABLE
// stands right after the table's name. A word that the statement may or may
// not take starts a new statement. Splitting where the server does not makes
// the guard judge two parts of one statement; joining where the server splits
// lets the second statement run unjudged, so every rule here is one the
// grammar forces.
type scan struct {
	p     *parser
	kind  Kind
	start int
	// lead is the word that decides the rules: the statement's first, or for
	// a statement that opens with common table expressions, the word of the
	// statement they serve.
	lead string
	// expressions is true while the common table expressions of a WITH are
	// read, before the statement they serve.
	expressions bool
	// merge is true for a MERGE, which runs to the semicolon it requires.
	merge bool
	// source is true once an INSERT has its rows: VALUES, DEFAULT VALUES, a
	// SELECT or an EXEC.
	source bool
	// assigned is true once an UPDATE has its SET clause.
	assigned bool
	// references is true once the statement names the table a foreign key
	// references, after which ON DELETE and ON UPDATE are its actions.
	references bool
	// action is the index where the action of an ALTER stands, and actions
	// the statement words it may be; -1 when the ALTER names no such place.
	action  int
	actions []string
	// columnAction is the index after ALTER COLUMN and the column's name,
	// where DROP is the column's action, or -1.
	columnAction int
	// execute is the index of the EXEC an INSERT stores the rows of, or -1.
	execute int
	// top is the index after the argument of TOP, with PERCENT and WITH
	// TIES, where the select list or the target starts; -1 before a TOP.
	top int
	// ordered is true once the statement has an ORDER BY, after which OFFSET
	// and FETCH take an expression.
	ordered bool
}

func (p *parser) newScan(index int, kind Kind) *scan {
	s := &scan{p: p, kind: kind, start: index, action: -1, columnAction: -1, execute: -1, top: -1}
	if kind == Simple {
		s.lead = p.keyword(index)
		s.expressions = s.lead == "WITH"
		s.serve(index)
	}
	return s
}

// serve takes the statement word at index as the one whose rules apply.
func (s *scan) serve(index int) {
	s.lead = s.p.keyword(index)
	switch s.lead {
	case "MERGE":
		s.merge = true
	case "UPDATE":
		if s.p.is(index+1, "STATISTICS") {
			s.lead = "UPDATE STATISTICS"
		}
	case "ALTER":
		s.action, s.actions = s.p.alterAction(index)
	}
}

// track follows a word at the statement's top level that the rules read
// later.
func (s *scan) track(index int) {
	p := s.p
	switch p.keyword(index) {
	case "VALUES":
		s.source = true
	case "REFERENCES":
		s.references = true
	case "ORDER":
		s.ordered = s.ordered || p.is(index+1, "BY")
	case "TOP":
		top := index + 2
		if p.op(index+1, "(") {
			top = p.closingParenthesis(index+1) + 1
		}
		if p.is(top, "PERCENT") {
			top++
		}
		if p.is(top, "WITH", "TIES") {
			top += 2
		}
		s.top = top
	}
}

// endsBefore reports whether the statement ends before the token at index,
// which stands at its top level. A MERGE ends at its semicolon alone, and a
// statement word in it that a MERGE cannot hold is refused: the server fails
// the batch with Msg 10713, and the reader does not guess where the MERGE
// would have ended.
func (s *scan) endsBefore(index int) (bool, error) {
	p := s.p
	if p.closesBlock(index) || p.isElse(index) || p.isLabel(index) {
		return true, nil
	}
	if s.merge {
		if !p.op(index, "(") && p.startsStatement(index) && !p.cannotEnd(index-1) &&
			!slices.Contains(mergeWords, p.keyword(index)) {
			return false, p.unreadable(index, "a MERGE that does not end with a semicolon")
		}
		return false, nil
	}
	if s.continues(index) {
		return false, nil
	}
	return p.startsStatement(index) && !p.cannotEnd(index-1), nil
}

// mergeWords are the statement words a MERGE holds: the actions of its WHEN
// clauses and the SET of an UPDATE action.
var mergeWords = []string{"DELETE", "INSERT", "SET", "UPDATE"}

// continues reports whether the statement word at index belongs to the
// statement rather than starting another.
func (s *scan) continues(index int) bool {
	word := s.p.keyword(index)
	return s.takes(index, word) || s.clause(index, word)
}

// takes reports whether the statement word at index is one the statement's
// lead takes at this point: the statement common table expressions serve, the
// source of an INSERT, the SET of an UPDATE, the action of an ALTER.
func (s *scan) takes(index int, word string) bool {
	p := s.p
	switch {
	case s.expressions:
		if p.op(index-1, ")") && slices.Contains(servedWords, word) {
			s.expressions = false
			s.serve(index)
			return true
		}
	case s.lead == "INSERT" && !s.source && (isQueryWord(word) || p.parenthesizedQuery(index)):
		s.source = true
		if isExecute(word) {
			s.execute = index
		}
		return true
	case s.lead == "UPDATE" && !s.assigned && word == "SET":
		s.assigned = true
		return true
	case index == s.action && slices.Contains(s.actions, word):
		if word == "ALTER" && p.is(index+1, "COLUMN") {
			s.columnAction = p.afterName(index + 2)
		}
		return true
	case index == s.columnAction && word == "DROP":
		return true
	}
	return false
}

// clause reports whether the statement word at index starts a clause of the
// statement rather than a statement: IF EXISTS of a DROP, the actions of a
// foreign key, FETCH after OFFSET, and a parenthesis that RETURN, VALUES, TOP,
// OFFSET or FETCH takes.
func (s *scan) clause(index int, word string) bool {
	p := s.p
	switch {
	case word == "IF":
		// DROP ... IF EXISTS. The IF statement's EXISTS takes a subquery.
		return p.is(index+1, "EXISTS") && !p.op(index+2, "(")
	case word == "DELETE" || word == "UPDATE":
		// ON DELETE and ON UPDATE of a foreign key.
		return s.references && p.keyword(index-1) == "ON"
	case word == "SET":
		return s.referentialSet(index)
	case word == "FETCH":
		return s.offsetFetch(index)
	case p.op(index, "("):
		return s.takesParenthesis(index)
	default:
		return false
	}
}

// isQueryWord reports a word that gives an INSERT its rows: SELECT, or EXEC
// and EXECUTE, whose rows the INSERT stores.
func isQueryWord(word string) bool {
	return word == "SELECT" || isExecute(word)
}

// isExecute reports EXEC or EXECUTE.
func isExecute(word string) bool {
	return word == "EXEC" || word == "EXECUTE"
}

// referentialSet reports the SET of ON DELETE SET NULL or ON UPDATE SET
// DEFAULT at index.
func (s *scan) referentialSet(index int) bool {
	p := s.p
	previous := p.keyword(index - 1)
	return s.references && (previous == "DELETE" || previous == "UPDATE") && p.keyword(index-2) == "ON" &&
		(p.is(index+1, "NULL") || p.is(index+1, "DEFAULT"))
}

// offsetFetch reports the FETCH of OFFSET ... ROWS FETCH NEXT ... ROWS ONLY at
// index. A cursor's FETCH NEXT names FROM.
func (s *scan) offsetFetch(index int) bool {
	p := s.p
	previous := p.keyword(index - 1)
	return (previous == "ROW" || previous == "ROWS") &&
		(p.is(index+1, "NEXT") || p.is(index+1, "FIRST")) && !p.is(index+2, "FROM")
}

// takesParenthesis reports a parenthesis at index that the statement takes:
// RETURN's value, the rows of VALUES, the first column after TOP, and the
// expression OFFSET and FETCH take.
func (s *scan) takesParenthesis(index int) bool {
	previous := s.p.keyword(index - 1)
	return s.lead == "RETURN" && index == s.start+1 ||
		s.lead == "INSERT" && previous == "VALUES" && s.p.keyword(index-2) != "DEFAULT" ||
		index == s.top ||
		s.ordered && s.orderedExpression(index, previous)
}

// orderedExpression reports the parenthesis after OFFSET, or after FETCH NEXT
// or FETCH FIRST, of a statement with an ORDER BY. OFFSET is not reserved and
// is a column alias anywhere else: SELECT 1 AS offset (SELECT ...) is two
// statements.
func (s *scan) orderedExpression(index int, previous string) bool {
	p := s.p
	return previous == "OFFSET" && p.keyword(index-2) != "BY" && !p.op(index-2, ",") ||
		(previous == "NEXT" || previous == "FIRST") && p.keyword(index-2) == "FETCH"
}

// servedWords are the statements common table expressions serve.
var servedWords = []string{"DELETE", "INSERT", "MERGE", "SELECT", "UPDATE"}

// finish returns the statement read from its start to the token at last.
func (s *scan) finish(last int) Statement {
	statement := s.p.span(s.kind, s.start, last)
	if s.execute < 0 {
		return statement
	}
	execute := s.p.span(Simple, s.execute, last)
	statement.Head = s.p.text[statement.Start:s.p.tokens[s.execute-1].End]
	statement.Statements = []Statement{execute}
	return statement
}

// alterAction returns the index where the action of the ALTER at index stands
// and the statement words the action may be, for the objects whose action is
// such a word: ALTER TABLE t DROP ..., ALTER INDEX i ON t DISABLE, ALTER
// DATABASE d SET .... It returns -1 for any other ALTER, whose statement words
// start statements as anywhere else.
func (p *parser) alterAction(index int) (int, []string) {
	object := index + 1
	switch {
	case p.is(object, "TABLE"):
		return p.afterName(object + 1), []string{"ALTER", "DISABLE", "DROP", "ENABLE", "SET"}
	case p.is(object, "INDEX"):
		on := p.afterName(object + 1)
		if !p.is(on, "ON") {
			return -1, nil
		}
		return p.afterName(on + 1), []string{"DISABLE", "SET"}
	case p.is(object, "FULLTEXT", "INDEX", "ON"):
		return p.afterName(object + 3), []string{"ALTER", "DISABLE", "DROP", "ENABLE", "SET"}
	case p.is(object, "DATABASE", "SCOPED", "CONFIGURATION"):
		action := object + 3
		if p.is(action, "FOR", "SECONDARY") {
			action += 2
		}
		return action, []string{"SET"}
	case p.is(object, "DATABASE"):
		return p.afterName(object + 1), []string{"SET"}
	case p.is(object, "ROLE"):
		return p.afterName(object + 1), []string{"DROP"}
	case p.is(object, "SERVER", "ROLE"):
		return p.afterName(object + 2), []string{"DROP"}
	case p.is(object, "LOGIN"):
		return p.afterName(object + 1), []string{"DISABLE", "ENABLE"}
	case p.is(object, "SECURITY", "POLICY"):
		return p.afterName(object + 2), []string{"ALTER", "DROP"}
	case p.is(object, "PARTITION", "FUNCTION"):
		parameters := p.afterName(object + 2)
		if p.op(parameters, "(") && p.op(parameters+1, ")") {
			return parameters + 2, []string{"MERGE"}
		}
	}
	return -1, nil
}
