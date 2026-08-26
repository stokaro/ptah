package goschema_test

import (
	"regexp"
	"strings"
)

// The pipeline tests in this package render SQL and compare it, and the
// renderer quotes identifiers. Stripping the quotes keeps those assertions
// about field ORDER rather than about a dialect's quoting rule.
//
// The same helper exists in core/schemamodel's tests, and deliberately: a test
// helper shared across a package boundary would have to be exported from a
// non-test file, which puts it in the public surface for no caller
// (stokaro/ptah#2246 section 2.1).
var simpleRenderedIdentifierQuoteRE = regexp.MustCompile("[`\"]([a-z_][a-z0-9_]*)[`\"]")

func legacyRenderedSQL(sql string) string {
	return simpleRenderedIdentifierQuoteRE.ReplaceAllString(sql, "$1")
}

// executableSQL drops the comment lines a plan carries, so a test comparing
// statements is not comparing prose.
func executableSQL(sql string) string {
	kept := make([]string, 0, strings.Count(sql, "\n")+1)
	for line := range strings.SplitSeq(sql, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, "\n")
}
