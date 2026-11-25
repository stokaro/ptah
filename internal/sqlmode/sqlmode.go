// Package sqlmode reads a MySQL-family session's sql_mode, the
// comma-separated list of modes `SELECT @@SESSION.sql_mode` answers. It is the
// one place that list is parsed: the connector reads it to mark a session
// whose string literals read differently and to make a loose session strict,
// and the migrator reads it to refuse a transaction mode its statement
// boundaries cannot follow, and they have to agree on what names a mode.
package sqlmode

import (
	"slices"
	"strings"
)

// NoBackslashEscapes is the mode under which a backslash in a string literal
// is the character rather than the start of an escape.
const NoBackslashEscapes = "NO_BACKSLASH_ESCAPES"

// AnsiQuotes is the mode under which "..." is a quoted name, in which a
// backslash is a character, rather than a string.
const AnsiQuotes = "ANSI_QUOTES"

// MSSQL is MariaDB's mode under which [...] is a quoted name. The server
// writes it in the list whenever a session sets it, alone or within the
// combination of the same name.
const MSSQL = "MSSQL"

// StrictTransTables and StrictAllTables are the strict modes. A session that
// names either fails an ALTER TABLE that would store a value a column cannot
// hold, on a transactional table and on a MyISAM one alike, since the
// statement builds the new table before it swaps it in. A session that names
// neither lets the statement store the nearest value the column can hold, and
// warn: an ENUM member nothing declares becomes the empty string, and a
// string longer than its column is cut.
const (
	StrictTransTables = "STRICT_TRANS_TABLES"
	StrictAllTables   = "STRICT_ALL_TABLES"
)

// strictDependent are the modes whose effect a strict mode changes. Without
// one, ERROR_FOR_DIVISION_BY_ZERO stores NULL for a division by zero, and
// NO_ZERO_DATE stores a zero date, each with a warning; NO_ZERO_IN_DATE stores
// a date with a zero month or day as a zero date. With one, each of them fails
// the statement, so a copy of a table whose rows the session stored this way
// fails though it would change none of them.
var strictDependent = []string{"ERROR_FOR_DIVISION_BY_ZERO", "NO_ZERO_DATE", "NO_ZERO_IN_DATE"}

// Strict reports whether sqlMode names a strict mode.
func Strict(sqlMode string) bool {
	return Has(sqlMode, StrictTransTables) || Has(sqlMode, StrictAllTables)
}

// WithStrict returns the mode under which a session that sqlMode leaves
// loose keeps the values its tables store: sqlMode with STRICT_ALL_TABLES
// added and the modes whose effect a strict mode changes taken out, so a
// statement fails where it would change a stored value and stores what the
// loose session would store everywhere else. Every other mode stays, in its
// order, since modes such as ANSI_QUOTES decide how the server reads the
// statements. It returns sqlMode itself when sqlMode is [Strict].
func WithStrict(sqlMode string) string {
	if Strict(sqlMode) {
		return sqlMode
	}
	var kept []string
	for name := range strings.SplitSeq(sqlMode, ",") {
		name = strings.TrimSpace(name)
		if name == "" || slices.ContainsFunc(strictDependent, func(mode string) bool { return strings.EqualFold(name, mode) }) {
			continue
		}
		kept = append(kept, name)
	}
	return strings.Join(append(kept, StrictAllTables), ",")
}

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
