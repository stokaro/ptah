package defaultlit_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/defaultlit"
)

var relativeTimeLiterals = []string{"NOW", "TODAY", "TOMORROW", "YESTERDAY"}

// A string the engine reads as a moment is the literal however it is cast and
// enclosed, on any date or time column.
func TestRelativeTimeLiteral_Matches(t *testing.T) {
	rows := []struct{ name, columnType, value string }{
		{name: "quoted", columnType: "timestamptz", value: "'now'"},
		{name: "mixed case and spaces", columnType: "date", value: "' Today '"},
		{name: "a cast", columnType: "timestamp", value: "'now'::timestamp"},
		{name: "parentheses", columnType: "timestamptz", value: "('now')"},
		{name: "two pairs of parentheses", columnType: "time", value: "(('tomorrow'))"},
		{name: "a cast inside parentheses", columnType: "timestamp with time zone", value: "('now'::timestamptz)"},
		{name: "a cast after parentheses", columnType: "timestamptz", value: "('yesterday')::timestamptz"},
		{name: "a datetime", columnType: "datetime", value: "'now'"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(defaultlit.RelativeTimeLiteral(row.columnType, row.value, relativeTimeLiterals), qt.IsTrue)
		})
	}
}

// A fixed moment, a function, a string beside other SQL, the word on a column
// that holds no moment, and an engine that names no literal are not one.
func TestRelativeTimeLiteral_DoesNotMatch(t *testing.T) {
	rows := []struct {
		name, columnType, value string
		literals                []string
	}{
		{name: "a fixed moment", columnType: "timestamptz", value: "'epoch'", literals: relativeTimeLiterals},
		{name: "a function", columnType: "timestamptz", value: "now()", literals: relativeTimeLiterals},
		{name: "a bare word", columnType: "timestamptz", value: "now", literals: relativeTimeLiterals},
		{name: "a text column", columnType: "text", value: "'now'", literals: relativeTimeLiterals},
		{name: "an expression around the string", columnType: "timestamptz", value: "('now')::timestamptz + interval '1 day'", literals: relativeTimeLiterals},
		{name: "two strings", columnType: "timestamptz", value: "('now') || ('now')", literals: relativeTimeLiterals},
		{name: "a quote inside", columnType: "timestamptz", value: "'now''s'", literals: relativeTimeLiterals},
		{name: "no literals", columnType: "timestamptz", value: "'now'"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(defaultlit.RelativeTimeLiteral(row.columnType, row.value, row.literals), qt.IsFalse)
		})
	}
}
