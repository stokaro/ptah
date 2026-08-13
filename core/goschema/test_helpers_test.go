package goschema_test

import (
	"regexp"
	"strings"
)

var simpleRenderedIdentifierQuoteRE = regexp.MustCompile("[`\"]([a-z_][a-z0-9_]*)[`\"]")

func legacyRenderedSQL(sql string) string {
	return simpleRenderedIdentifierQuoteRE.ReplaceAllString(sql, "$1")
}

// executableSQL drops every SQL line comment, leaving only what a server would
// run.
//
// A target that cannot host an object names it in a comment that repeats the
// object's own DDL keywords -- `-- CREATE FUNCTION test_func not supported in
// MySQL` -- so a Contains check over the raw output cannot tell an emitted
// CREATE FUNCTION from a diagnostic about one. Asserting on this instead
// separates "the statement is not there" from "nothing is there", which is the
// difference between a named skip and a silent omission (stokaro/ptah#929
// item 5).
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
