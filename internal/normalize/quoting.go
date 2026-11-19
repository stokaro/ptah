package normalize

import "strings"

// Quoting is how a dialect's quoted text ends. The zero value is the SQL
// standard: a single quote opens a string, a double quote opens a name, a
// doubled quote inside either is the quote itself, and a backslash is an
// ordinary character.
//
// Reading a backslash as an escape where the dialect does not hides a change.
// Under PostgreSQL's standard_conforming_strings `'p\'` is a whole string, and
// a scanner that reads `\'` as an escaped quote takes the next string for text
// outside quotes, where whitespace and case are folded.
type Quoting struct {
	// Strings are the bytes that open a string: `'`, or `'"` where a
	// double-quoted token is a string rather than a name. Empty is `'`.
	Strings string
	// BackslashEscapes reports that a backslash escapes the next byte in
	// every string, as MySQL and ClickHouse read one.
	BackslashEscapes bool
	// IdentifierBackslashEscapes reports that a backslash escapes the next
	// byte in a quoted name too, as ClickHouse reads one.
	IdentifierBackslashEscapes bool
	// EscapeStrings reports that E'...' is a string in which a backslash
	// escapes the next byte whatever BackslashEscapes says, as PostgreSQL
	// reads one.
	EscapeStrings bool
}

// OpensString reports that value[at] is a quote that opens a string, not a
// quoted name.
func (q Quoting) OpensString(value string, at int) bool {
	strs := q.Strings
	if strs == "" {
		strs = "'"
	}
	return strings.IndexByte(strs, value[at]) >= 0
}

// QuotedEnd returns the index of the quote that closes the string or quoted
// name opening at value[start], or the last index of value when nothing
// closes it. A doubled quote is the quote itself.
func (q Quoting) QuotedEnd(value string, start int) int {
	if end, closed := q.ClosingQuote(value, start); closed {
		return end
	}
	return len(value) - 1
}

// ClosingQuote returns the index of the quote that closes the string or quoted
// name opening at value[start], and false when nothing closes it. A doubled
// quote is the quote itself.
func (q Quoting) ClosingQuote(value string, start int) (end int, closed bool) {
	quote := value[start]
	backslashes := q.BackslashEscapesIn(value, start)
	for i := start + 1; i < len(value); i++ {
		switch {
		case backslashes && value[i] == '\\' && i+1 < len(value):
			i++
		case value[i] != quote:
		case i+1 < len(value) && value[i+1] == quote:
			i++
		default:
			return i, true
		}
	}
	return 0, false
}

// BackslashEscapesIn reports whether a backslash escapes the next byte inside
// the quoted text opening at value[start].
func (q Quoting) BackslashEscapesIn(value string, start int) bool {
	if !q.OpensString(value, start) {
		return q.IdentifierBackslashEscapes
	}
	return q.BackslashEscapes || q.EscapeStrings && value[start] == '\'' && opensEscapeString(value, start)
}

// opensEscapeString reports that the quote at value[start] follows an E that
// is a string prefix, not the last letter of a name.
func opensEscapeString(value string, start int) bool {
	return start > 0 && (value[start-1] == 'E' || value[start-1] == 'e') &&
		(start == 1 || !isIdentifierByte(value[start-2]))
}
