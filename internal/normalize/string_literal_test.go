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
		{"a qualified quoted type", "'a'::public.\"Mood\""},
		{"a type in several words", "'2020-01-01'::timestamp with time zone"},
		{"a precision inside a type in several words", "'2020-01-01'::timestamp(3) with time zone"},
		{"an array of a sized type", "'{}'::character varying(30)[]"},
		{"a national string in lower case", "n'a'"},
		{"a PostgreSQL escape string", "E'a\\nb'"},
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
		{"an Oracle sequence reference", "seq.NEXTVAL"},
		{"the reference Oracle stores", `"APP"."SEQ"."NEXTVAL"`},
		{"a SQL Server sequence clause", "NEXT VALUE FOR s"},
		{"the clause SQL Server stores", "(NEXT VALUE FOR [s])"},
		{"a cast followed by IS NOT NULL", "'a'::text IS NOT NULL"},
		{"a cast followed by AND", "'a'::text AND b"},
		{"a cast followed by OR", "'a'::text OR b"},
		{"a bit literal", "B'101'"},
		{"a bit literal in lower case", "b'1'"},
		{"a hexadecimal literal", "X'41'"},
		{"a hexadecimal literal in lower case", "x'41'"},
		{"a word before the quote", "upper'a'"},
		{"an underscore alone before the quote", "_'a'"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(normalize.IsStringLiteral(row.value), qt.IsFalse)
		})
	}
}
