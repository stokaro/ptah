// Package triggerdef reads the parts of a trigger's definition that a column
// of the model or the catalog holds as text: the event list, and a PostgreSQL
// trigger's WHEN condition.
//
// The event list has one grammar and one order wherever it is read. A
// declaration may name the events in any order, and PostgreSQL reports them in
// its own, so canonicalizing and comparing both go through [Canonical].
//
// The WHEN condition is stored as a node tree in pg_trigger.tgqual, and
// pg_get_expr cannot print it: measured on PostgreSQL 18.6,
// `pg_get_expr(tgqual, tgrelid)` answers `expression contains variables of
// more than one relation`, because the condition reads both OLD and NEW. So the
// schema reader and the comparison probe both take it from the definition,
// through this one function, and compare the server's spelling with itself.
package triggerdef

import "strings"

// When returns the condition of a pg_get_triggerdef definition's WHEN clause,
// without the clause's own parentheses, or "" when the definition has none.
//
// It reads the clause where pg_get_triggerdef writes it: after FOR EACH ROW or
// FOR EACH STATEMENT, before EXECUTE. The parentheses are matched outside
// string literals and quoted identifiers, so a condition comparing with ')' is
// read whole.
func When(definition string) string {
	for _, level := range []string{" FOR EACH ROW ", " FOR EACH STATEMENT "} {
		_, rest, found := strings.Cut(definition, level)
		if !found {
			continue
		}
		condition, ok := strings.CutPrefix(rest, "WHEN (")
		if !ok {
			return ""
		}
		end := closingParenthesis(condition)
		if end < 0 {
			return ""
		}
		return strings.TrimSpace(condition[:end])
	}
	return ""
}

// closingParenthesis is the index of the parenthesis that closes one already
// open before text begins, or -1.
func closingParenthesis(text string) int {
	depth := 1
	var quote byte
	for i := 0; i < len(text); i++ {
		c := text[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"':
			quote = c
		case c == '(':
			depth++
		case c == ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}
