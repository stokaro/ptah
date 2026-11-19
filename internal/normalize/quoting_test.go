package normalize_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/normalize"
)

// TestQuoting_QuotedEnd finds the quote that closes the quoted text opening at
// the first byte, under each dialect's rules for a backslash.
func TestQuoting_QuotedEnd(t *testing.T) {
	standard := normalize.Quoting{}
	postgres := normalize.Quoting{EscapeStrings: true}
	mysql := normalize.Quoting{Strings: `'"`, BackslashEscapes: true}
	clickhouse := normalize.Quoting{BackslashEscapes: true, IdentifierBackslashEscapes: true}
	tests := []struct {
		name    string
		value   string
		start   int
		quoting normalize.Quoting
		want    int
	}{
		{name: "a plain string", value: `'ab' x`, quoting: standard, want: 3},
		{name: "a doubled quote", value: `'a''b' x`, quoting: standard, want: 5},
		{name: "a backslash in a standard string", value: `'p\' || 'q'`, quoting: standard, want: 3},
		{name: "a backslash in a PostgreSQL string", value: `'p\' || 'q'`, quoting: postgres, want: 3},
		{name: "a backslash in a PostgreSQL escape string", value: `E'p\'' x`, start: 1, quoting: postgres, want: 5},
		{name: "a lower-case escape string prefix", value: `e'p\'' x`, start: 1, quoting: postgres, want: 5},
		{name: "an E ending a name is no prefix", value: `typE'p\' x'`, start: 4, quoting: postgres, want: 7},
		{name: "an escape string where the dialect has none", value: `E'p\' x'`, start: 1, quoting: standard, want: 4},
		{name: "a backslash in a MySQL string", value: `'p\'' x`, quoting: mysql, want: 4},
		{name: "a backslash in a MySQL double-quoted string", value: `"p\"" x`, quoting: mysql, want: 4},
		{name: "a backslash in a quoted name where names take none", value: "`p\\` x", quoting: mysql, want: 3},
		{name: "a backslash in a ClickHouse quoted name", value: `"p\"" x`, quoting: clickhouse, want: 4},
		{name: "an unclosed string", value: `'abc`, quoting: standard, want: 3},
		{name: "a trailing backslash", value: `'abc\`, quoting: mysql, want: 4},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.quoting.QuotedEnd(test.value, test.start), qt.Equals, test.want)
		})
	}
}

// TestQuoting_OpensString tells a quote that opens a string from one that
// opens a name.
func TestQuoting_OpensString(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		quoting normalize.Quoting
		want    bool
	}{
		{name: "a single quote by default", value: `'a'`, want: true},
		{name: "a double quote by default", value: `"a"`, want: false},
		{name: "a double quote where it opens a string", value: `"a"`, quoting: normalize.Quoting{Strings: `'"`}, want: true},
		{name: "a backtick", value: "`a`", quoting: normalize.Quoting{Strings: `'"`}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.quoting.OpensString(test.value, 0), qt.Equals, test.want)
		})
	}
}
