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
// through this one function, and compare the server's spelling with itself. A
// server without pg_get_triggerdef reports no condition at all; see
// capability.CatalogTriggerDefinitions.
package triggerdef

import "strings"

// When returns the condition of a pg_get_triggerdef definition's WHEN clause,
// without the parentheses around the whole of it, or "" when the definition
// has none.
//
// It reads the clause where pg_get_triggerdef writes it: after FOR EACH ROW or
// FOR EACH STATEMENT, up to the EXECUTE that follows it outside any
// parenthesis. PostgreSQL and YugabyteDB wrap the condition in parentheses,
// `WHEN ((new.a = 1)) EXECUTE`; CockroachDB 26.2 and 26.3 do not, `WHEN
// (new).a > 0:::INT8 EXECUTE`, measured 2026-09-26. So the outer pair is
// removed only where it encloses the whole condition, and `(new).a > 0` keeps
// its own. Parentheses are matched outside string literals and quoted
// identifiers, so a condition comparing with ')' is read whole.
func When(definition string) string {
	for _, level := range []string{" FOR EACH ROW ", " FOR EACH STATEMENT "} {
		_, rest, found := strings.Cut(definition, level)
		if !found {
			continue
		}
		clause, ok := strings.CutPrefix(rest, "WHEN ")
		if !ok {
			return ""
		}
		end := clauseEnd(clause)
		if end < 0 {
			return ""
		}
		condition := strings.TrimSpace(clause[:end])
		if inner, wrapped := strings.CutPrefix(condition, "("); wrapped && closingParenthesis(inner) == len(inner)-1 {
			return strings.TrimSpace(inner[:len(inner)-1])
		}
		return condition
	}
	return ""
}

// clauseEnd is the index of the " EXECUTE " that ends a WHEN clause, the
// first one outside every parenthesis and quote, or -1 when there is none or
// a parenthesis closes one the clause never opened.
func clauseEnd(clause string) int {
	depth := 0
	var quote byte
	for i := 0; i < len(clause); i++ {
		c := clause[i]
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
			if depth < 0 {
				return -1
			}
		case depth == 0 && strings.HasPrefix(clause[i:], " EXECUTE "):
			return i
		}
	}
	return -1
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
