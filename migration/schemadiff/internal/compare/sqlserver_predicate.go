package compare

import "strings"

// normalizeSQLServerPredicateSpelling reduces SQL Server filtered-index
// predicate spelling differences that carry no semantic weight before the
// generic check-expression normalization runs.
//
// SQL Server persists sys.indexes.filter_definition in a canonical spelling:
// identifiers come back bracket-quoted and numeric literals parenthesized, so
// a user-authored predicate such as "status = 1" is stored as
// "([status]=(1))". Comparing raw spellings verbatim would classify every
// such filtered index as a perpetual drop/create replacement. This pass:
//
//   - drops square-bracket identifier quoting outside string literals, and
//   - unwraps parentheses that wrap a bare numeric literal.
//
// String literal content, including doubled single-quote escapes, is
// preserved verbatim. Spellings SQL Server rewrites more aggressively (for example
// implicit N'...' prefixes or CAST insertions) are intentionally not
// reconstructed here; they compare as changed and are documented as requiring
// the catalog's canonical spelling in the annotation.
func normalizeSQLServerPredicateSpelling(expr string) string {
	previous := expr
	for {
		next := stripSQLServerPredicateSpelling(previous)
		if next == previous {
			return next
		}
		previous = next
	}
}

func stripSQLServerPredicateSpelling(expr string) string {
	var builder strings.Builder
	builder.Grow(len(expr))
	inString := false
	for pos := 0; pos < len(expr); pos++ {
		ch := expr[pos]
		if ch == '\'' {
			if inString && pos+1 < len(expr) && expr[pos+1] == '\'' {
				builder.WriteString("''")
				pos++
				continue
			}
			inString = !inString
			builder.WriteByte(ch)
			continue
		}
		if inString {
			builder.WriteByte(ch)
			continue
		}
		if ch == '[' || ch == ']' {
			continue
		}
		if ch == '(' {
			if literal, closing, ok := parenthesizedNumericLiteral(expr, pos); ok {
				builder.WriteString(literal)
				pos = closing
				continue
			}
		}
		builder.WriteByte(ch)
	}
	return builder.String()
}

// parenthesizedNumericLiteral reports whether the parenthesis opening at
// expr[open] wraps nothing but a bare numeric literal. It returns the literal
// text and the position of the closing parenthesis.
func parenthesizedNumericLiteral(expr string, open int) (literal string, closing int, ok bool) {
	pos := skipPredicateSpaces(expr, open+1)
	literalStart := pos
	if pos < len(expr) && (expr[pos] == '-' || expr[pos] == '+') {
		pos++
	}
	digits, dots := 0, 0
	for pos < len(expr) {
		ch := expr[pos]
		if ch >= '0' && ch <= '9' {
			digits++
			pos++
			continue
		}
		if ch == '.' {
			dots++
			pos++
			continue
		}
		break
	}
	literalEnd := pos
	pos = skipPredicateSpaces(expr, pos)
	if digits == 0 || dots > 1 || pos >= len(expr) || expr[pos] != ')' {
		return "", 0, false
	}
	return expr[literalStart:literalEnd], pos, true
}

func skipPredicateSpaces(expr string, pos int) int {
	for pos < len(expr) && (expr[pos] == ' ' || expr[pos] == '\t' || expr[pos] == '\n' || expr[pos] == '\r') {
		pos++
	}
	return pos
}
