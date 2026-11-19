package normalize_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/normalize"
)

// A string keeps being one string under the spellings catalogs report it in.
func TestIsStringLiteral_HappyPath(t *testing.T) {
	rows := []struct{ name, value string }{
		{"plain", "'1 + 2'"},
		{"a doubled quote", "'it''s'"},
		{"a quote after a backslash", `'it\'s'`},
		{"a PostgreSQL string ending in a backslash", `'C:\'::text`},
		{"a PostgreSQL cast", "'1 + 2'::character varying"},
		{"a cast with a size", "'a'::character varying(30)"},
		{"two casts", "'a'::text::\"Mood\""},
		{"SQL Server's parentheses", "(('a'))"},
		{"a national string", "N'a'"},
		{"a character set introducer", "_utf8mb4'a'"},
		{"surrounding space", "  'a'  "},
		{"empty", "''"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(normalize.IsStringLiteral(row.value), qt.IsTrue)
		})
	}
}

// An expression is no string, even where it holds strings or starts and ends
// with a quote.
func TestIsStringLiteral_FailurePath(t *testing.T) {
	rows := []struct{ name, value string }{
		{"an operator expression", "(1 + 2)"},
		{"a call", "now()"},
		{"a call on a string", "upper('a')"},
		{"two strings and an operator", "'a' || 'b'"},
		{"a cast string concatenated", "'a'::text || 'b'::text"},
		{"a number", "0"},
		{"a word", "CURRENT_TIMESTAMP"},
		{"an unclosed quote", "'a"},
		{"an empty value", ""},
		{"a string compared", "'a' = 'b'"},
		{"a digit before the quote", "1'a'"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(normalize.IsStringLiteral(row.value), qt.IsFalse)
		})
	}
}
