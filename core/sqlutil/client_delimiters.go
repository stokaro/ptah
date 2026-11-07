package sqlutil

import (
	"ptah.run/internal/clientdelimiter"
)

// NormalizeClientDelimiters rewrites MySQL DELIMITER and Atlas delimiter
// directives into regular semicolon-terminated SQL: the directive lines are
// removed, and each statement terminated by the custom delimiter comes back
// terminated by a plain semicolon, so the splitters in this package see
// standard SQL. A custom delimiter that follows a semicolon — with nothing but
// whitespace between — is dropped rather than doubled, which is how `END;$$`
// becomes `END;`.
//
// A directive must be alone on its line. Both spellings are matched
// case-insensitively: `DELIMITER $$` (the MySQL client's form) and
// `-- atlas:delimiter $$`. The delimiter token may be wrapped in single or
// double quotes, which are stripped, and the escapes \n, \r and \t are
// expanded, so `-- atlas:delimiter \n\n` selects a blank line as the
// terminator. `DELIMITER ;` restores normal splitting.
//
// The two spellings differ in one respect a script can see: a delimiter
// announced by the Atlas form is honored even when the delimiter itself reads
// as a comment (`-- end` alone on a line, say), while the MySQL form never
// matches its delimiter inside comment text. String literals, quoted
// identifiers, comments and dollar-quoted bodies are otherwise passed through
// unchanged, so a delimiter spelled inside one of them is not rewritten.
func NormalizeClientDelimiters(input string) string {
	normalized, _ := clientdelimiter.Normalize(input)
	return normalized
}
