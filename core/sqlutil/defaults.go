package sqlutil

import "strings"

// DefaultLooksLikeExpression reports whether a DEFAULT a catalog reported is an
// EXPRESSION rather than a literal value.
//
// A catalog hands back one string for both. The distinction matters because the
// two are declared differently -- `default` takes a value and `default_expr`
// takes SQL -- and a description that put an expression in the value slot would
// render it quoted, turning `now()` into the literal text "now()".
//
// The rule is the absence of quoting: a value the server wrapped in single or
// double quotes is a literal, and anything else is SQL it will evaluate. That
// is deliberately coarse, and it is the same rule a column default and a domain
// default are both routed by -- which is why it lives here rather than beside
// either of them (stokaro/ptah#2315).
//
// Double quotes wrap a literal only where they wrap the whole value, a doubled
// quote inside standing for one. `"APP"."SEQ"."NEXTVAL"`, the sequence
// reference Oracle stores, opens and closes with a quote and is three quoted
// names; read as a literal, it renders as the string
// '"APP"."SEQ"."NEXTVAL"'.
func DefaultLooksLikeExpression(defaultSQL string) bool {
	value := strings.TrimSpace(defaultSQL)
	if value == "" {
		return false
	}
	if wholeDoubleQuoted(value) {
		return false
	}
	if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
		return false
	}
	return true
}

// wholeDoubleQuoted reports whether value is one double-quoted token: it
// opens with a quote, and its first quote that is not doubled is its last
// character.
func wholeDoubleQuoted(value string) bool {
	if len(value) < 2 || value[0] != '"' {
		return false
	}
	for index := 1; index < len(value); index++ {
		if value[index] != '"' {
			continue
		}
		if index+1 < len(value) && value[index+1] == '"' {
			index++
			continue
		}
		return index == len(value)-1
	}
	return false
}
