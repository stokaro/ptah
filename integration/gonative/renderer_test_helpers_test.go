package gonative_test

import "regexp"

var simpleRenderedIdentifierQuoteRE = regexp.MustCompile("[`\"]([a-z_][a-z0-9_]*)[`\"]")

func legacyRenderedSQL(sql string) string {
	return simpleRenderedIdentifierQuoteRE.ReplaceAllString(sql, "$1")
}
