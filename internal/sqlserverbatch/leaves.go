package sqlserverbatch

import "strings"

// Leaves returns the text of each statement the server runs from statements,
// in the order the text writes them, without the semicolon that ends it. The
// statements a block, an IF or a WHILE runs stand in for it, since BEGIN, END,
// a condition and an ELSE run nothing, and CREATE SCHEMA stands beside the
// elements it creates. An EXEC of a string literal stands for the statements
// the literal holds ([ExecText]), or for the literal's text where the reader
// cannot find its statements. A procedure, function, trigger or view is one
// leaf: the server stores its body rather than runs it.
//
// Every judgment of what T-SQL text does reads it through this one walk: the
// safety rating in ptah.run/migration/safety, the refusal of transaction
// control in internal/txcontrol, and the resume of a migration no
// transaction wraps in ptah.run/migration/migrator, which [RunsSeveral] reads
// too. The migrator sends a whole block as one statement, so a judge that
// reads only the words a statement starts with sees BEGIN and misses the
// COMMIT inside it, and two walks agree when the second is written and stop
// agreeing when the first learns a new kind. The rating would then judge a
// statement the refusal lets through, or the other way round.
func Leaves(statements []Statement) []string {
	var leaves []string
	walk(statements, false, func(leaf string, _ bool) {
		leaves = append(leaves, leaf)
	})
	return leaves
}

// RunsSeveral reports whether statements can commit part of their work and
// then fail, when no transaction wraps them: they run more than one leaf
// ([Leaves]) that changes something, a WHILE that runs a leaf any number of
// times, or an EXEC whose statements the text does not show, which a
// procedure's and computed text's are. A leaf that changes nothing is
// [ChangesNothing]'s. A migration that failed in such a statement cannot run
// it again safely: the server committed each leaf before the one that failed.
func RunsSeveral(statements []Statement) bool {
	changes := 0
	several := false
	walk(statements, false, func(leaf string, repeated bool) {
		switch {
		case ChangesNothing(leaf):
		case repeated, runsUnread(leaf):
			several = true
		default:
			changes++
		}
	})
	return several || changes > 1
}

// ChangesNothing reports a T-SQL statement whose effect ends with its batch:
// a DECLARE or SET of a variable, or a PRINT.
func ChangesNothing(statement string) bool {
	p, err := newParser(statement)
	if err != nil || len(p.tokens) == 0 {
		return false
	}
	switch p.keyword(0) {
	case "PRINT":
		return true
	case "DECLARE", "SET":
		return len(p.tokens) > 1 && strings.HasPrefix(p.tokens[1].Value, "@") && !strings.HasPrefix(p.tokens[1].Value, "@@")
	default:
		return false
	}
}

// runsUnread reports a leaf that runs statements its text does not show: an
// EXEC that [ExecText] does not read, or a procedure named with no EXEC.
func runsUnread(leaf string) bool {
	statements, err := Read(leaf)
	if err != nil || len(statements) != 1 {
		return true
	}
	if statements[0].Kind == Call {
		return true
	}
	p, err := newParser(leaf)
	if err != nil {
		return true
	}
	word := p.keyword(0)
	return word == "EXEC" || word == "EXECUTE"
}

// walk calls visit with each leaf of statements, as [Leaves] defines them,
// and whether a WHILE runs it.
func walk(statements []Statement, repeated bool, visit func(leaf string, repeated bool)) {
	for _, statement := range statements {
		switch statement.Kind {
		case Block, If:
			walk(statement.Statements, repeated, visit)
		case While:
			walk(statement.Statements, true, visit)
		case Schema:
			visit(strings.TrimSuffix(statement.Head, ";"), repeated)
			walk(statement.Statements, repeated, visit)
		case Simple:
			leaf := strings.TrimSuffix(statement.SQL, ";")
			text, ok := ExecText(leaf)
			if !ok {
				visit(leaf, repeated)
				continue
			}
			inner, err := Read(text)
			if err != nil {
				visit(strings.TrimSpace(text), repeated)
				continue
			}
			walk(inner, repeated, visit)
		default:
			visit(strings.TrimSuffix(statement.SQL, ";"), repeated)
		}
	}
}
