// Package sqlmode reads a MySQL-family session's sql_mode, the
// comma-separated list of modes `SELECT @@SESSION.sql_mode` answers. It is the
// one place that list is parsed: the connector reads it to mark a session
// whose string literals read differently, and the migrator reads it to refuse
// a transaction mode its statement boundaries cannot follow, and the two have
// to agree on what names a mode.
package sqlmode

import "strings"

// NoBackslashEscapes is the mode under which a backslash in a string literal
// is the character rather than the start of an escape.
const NoBackslashEscapes = "NO_BACKSLASH_ESCAPES"

// Has reports whether sqlMode names mode. A name is matched whole, after
// trimming spaces, and in any case: the server writes the list in upper case
// and accepts a mode in any case when one is set.
func Has(sqlMode, mode string) bool {
	for name := range strings.SplitSeq(sqlMode, ",") {
		if strings.EqualFold(strings.TrimSpace(name), mode) {
			return true
		}
	}
	return false
}
