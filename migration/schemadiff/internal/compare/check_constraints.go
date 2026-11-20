package compare

import (
	"strings"
	"unicode"
)

func checkExpressionHasUnsupportedRewrite(desired, database string) bool {
	db := strings.ToLower(database)
	return checkExpressionContainsInOperator(desired) && strings.Contains(db, "any") && strings.Contains(db, "array[")
}

func checkExpressionContainsInOperator(expr string) bool {
	inString := false
	for i := 0; i < len(expr)-1; i++ {
		ch := expr[i]
		if ch == '\'' {
			if inString && i+1 < len(expr) && expr[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
			continue
		}
		if inString || (ch != 'i' && ch != 'I') || (expr[i+1] != 'n' && expr[i+1] != 'N') {
			continue
		}
		prevIdent := i > 0 && isCheckIdentChar(rune(expr[i-1]))
		nextIdent := i+2 < len(expr) && isCheckIdentChar(rune(expr[i+2]))
		if !prevIdent && !nextIdent {
			return true
		}
	}
	return false
}

func isCheckIdentChar(ch rune) bool {
	return unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_'
}
