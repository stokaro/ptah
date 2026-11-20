package compare

import (
	"slices"
	"strings"
)

// canonicalizeCheckSetMembership renders a membership test in one spelling,
// whichever of the two an engine stored.
//
// SQL Server does not keep `col IN ('a','b')`. It stores the disjunction it
// compiles the list into -- `([col]='b' OR [col]='a')` -- and the order is its
// own. Compared as text against the declaration, the two never match, so every
// `schema apply` planned the same DROP and ADD of the same constraint: a change
// that is applied, never converges and never fails (stokaro/ptah#1716).
//
// Declining to compare, the way the PostgreSQL `ANY (ARRAY[...])` rewrite is
// handled, would be worse here rather than simpler. Value REMOVAL currently
// works on this engine precisely because the comparison keeps reporting a
// difference; silencing it would trade the churn for a change that is never
// applied at all.
//
// So both sides are canonicalized instead: same values, same answer; different
// values, still different.
func canonicalizeCheckSetMembership(expr string) string {
	column, values, ok := parseCheckInList(expr)
	if !ok {
		column, values, ok = parseCheckEqualityDisjunction(expr)
	}
	if !ok {
		return expr
	}
	slices.Sort(values)
	// The space is not cosmetic: without it a column named `color` and the
	// operator would render `colorin(...)`, which reads as a column named
	// `colorin`. The canonical form is only ever compared against itself, so a
	// separator that cannot occur in the normalized input keeps it unambiguous.
	return canonicalCheckColumn(column) + " in (" + strings.Join(values, ",") + ")"
}

// parseCheckInList reads `col in ('a','b')` from an already-normalized
// expression, which carries no whitespace.
//
// The operator is found without requiring a boundary before it, because after
// normalization there is none: `status IN (...)` becomes `statusin(...)`, and
// the column's last letter sits against the operator's first. The cost is one
// ambiguous shape -- a function call whose name ends in `in` and whose
// arguments are all string literals, `myin('a','b')`, reads as membership on a
// column `my`. Both sides of a comparison go through this same function, so
// such a call still compares equal to itself; what it cannot do is be told
// apart from `my IN ('a','b')`, which is a shape no schema in this repository
// produces.
func parseCheckInList(expr string) (column string, values []string, ok bool) {
	open := indexOfCheckInOperator(expr)
	if open < 0 || !strings.HasSuffix(expr, ")") {
		return "", nil, false
	}
	column = expr[:open]
	if column == "" {
		return "", nil, false
	}
	values, ok = splitCheckLiteralList(expr[open+len("in(") : len(expr)-1])
	if !ok || len(values) == 0 {
		return "", nil, false
	}
	return column, values, true
}

// parseCheckEqualityDisjunction reads `col='a'or col='b'`, requiring every
// branch to test the same column against a literal.
//
// A branch that is anything else -- a different column, a comparison other
// than equality, a nested expression -- makes the whole expression something
// this canonical form cannot represent, and it is left alone.
func parseCheckEqualityDisjunction(expr string) (column string, values []string, ok bool) {
	branches := splitOutsideCheckString(expr, "or")
	if len(branches) < 2 {
		return "", nil, false
	}
	values = make([]string, 0, len(branches))
	for _, branch := range branches {
		equals := indexOutsideCheckString(branch, "=")
		if equals <= 0 {
			return "", nil, false
		}
		name, literal := branch[:equals], branch[equals+1:]
		if !isCheckQuotedLiteral(literal) {
			return "", nil, false
		}
		if column == "" {
			column = name
		} else if canonicalCheckColumn(name) != canonicalCheckColumn(column) {
			return "", nil, false
		}
		values = append(values, literal)
	}
	return column, values, true
}

// canonicalCheckColumn strips the quoting an engine chose for the column name,
// so a declaration written without it compares equal to a catalog that added
// it.
func canonicalCheckColumn(column string) string {
	for _, pair := range [][2]string{{"[", "]"}, {`"`, `"`}, {"`", "`"}} {
		if strings.HasPrefix(column, pair[0]) && strings.HasSuffix(column, pair[1]) && len(column) > 1 {
			return column[1 : len(column)-1]
		}
	}
	return column
}

// splitCheckLiteralList splits a comma-separated list of quoted literals,
// refusing anything else so an expression carrying a function call or a
// subquery is left alone.
func splitCheckLiteralList(body string) ([]string, bool) {
	parts := splitOutsideCheckString(body, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		if !isCheckQuotedLiteral(part) {
			return nil, false
		}
		values = append(values, part)
	}
	return values, true
}

func isCheckQuotedLiteral(value string) bool {
	if len(value) < 2 || value[0] != '\'' || value[len(value)-1] != '\'' {
		return false
	}
	return indexOutsideCheckString(value[1:len(value)-1], "'") < 0 ||
		!strings.Contains(strings.ReplaceAll(value[1:len(value)-1], "''", ""), "'")
}

// splitOutsideCheckString splits on a separator that appears outside string
// literals and outside parentheses.
func splitOutsideCheckString(expr, separator string) []string {
	parts := make([]string, 0, 4)
	start := 0
	for i := 0; i < len(expr); {
		if next := skipCheckLiteral(expr, i); next != i {
			i = next
			continue
		}
		if expr[i] == '(' {
			i = skipCheckParens(expr, i)
			continue
		}
		if strings.HasPrefix(expr[i:], separator) && checkSeparatorStandsAlone(expr, i, separator) {
			parts = append(parts, expr[start:i])
			i += len(separator)
			start = i
			continue
		}
		i++
	}
	return append(parts, expr[start:])
}

// indexOfCheckInOperator finds the IN operator's opening parenthesis outside
// string literals, or -1.
func indexOfCheckInOperator(expr string) int {
	for i := 0; i < len(expr); {
		if next := skipCheckLiteral(expr, i); next != i {
			i = next
			continue
		}
		if strings.HasPrefix(expr[i:], "in(") {
			return i
		}
		i++
	}
	return -1
}

// indexOutsideCheckString finds a separator outside string literals and
// parentheses, or -1.
func indexOutsideCheckString(expr, separator string) int {
	for i := 0; i < len(expr); {
		if next := skipCheckLiteral(expr, i); next != i {
			i = next
			continue
		}
		if expr[i] == '(' {
			i = skipCheckParens(expr, i)
			continue
		}
		if strings.HasPrefix(expr[i:], separator) && checkSeparatorStandsAlone(expr, i, separator) {
			return i
		}
		i++
	}
	return -1
}

// checkSeparatorStandsAlone keeps a word separator from matching inside an
// identifier: the `or` in `color` is not the OR operator.
//
// Normalization has already removed the whitespace, so there is no boundary to
// read -- `color='red' OR color='blue'` arrives as
// `color='red'orcolor='blue'`, where the real operator is followed immediately
// by a letter and the false one is preceded by one. What the shape this parser
// accepts does guarantee is that the operator follows the END of a value: a
// quoted literal, or a closing parenthesis. That is the boundary used.
func checkSeparatorStandsAlone(expr string, at int, separator string) bool {
	if !isCheckIdentChar(rune(separator[0])) {
		return true
	}
	if at == 0 {
		return false
	}
	previous := expr[at-1]
	return previous == '\'' || previous == ')'
}

func skipCheckLiteral(expr string, i int) int {
	if expr[i] != '\'' {
		return i
	}
	i++
	for i < len(expr) {
		if expr[i] != '\'' {
			i++
			continue
		}
		if i+1 < len(expr) && expr[i+1] == '\'' {
			i += 2
			continue
		}
		return i + 1
	}
	return i
}

func skipCheckParens(expr string, i int) int {
	depth := 0
	for ; i < len(expr); i++ {
		if next := skipCheckLiteral(expr, i); next != i {
			i = next - 1
			continue
		}
		switch expr[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i + 1
			}
		}
	}
	return i
}
