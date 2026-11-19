package defaultlit_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/defaultlit"
)

var relativeTimeLiterals = []string{"NOW", "TODAY", "TOMORROW", "YESTERDAY"}

// A string the engine reads as a moment is the literal however it is cast and
// enclosed.
func TestRelativeTimeLiteral_Matches(t *testing.T) {
	rows := []struct{ name, value string }{
		{name: "quoted", value: "'now'"},
		{name: "mixed case and spaces", value: "' Today '"},
		{name: "a cast", value: "'now'::timestamp"},
		{name: "parentheses", value: "('now')"},
		{name: "two pairs of parentheses", value: "(('tomorrow'))"},
		{name: "a cast inside parentheses", value: "('now'::timestamptz)"},
		{name: "a cast after parentheses", value: "('yesterday')::timestamptz"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(defaultlit.RelativeTimeLiteral(row.value, relativeTimeLiterals), qt.IsTrue)
		})
	}
}

// A fixed moment, a function, a string beside other SQL, and an engine that
// names no literal are not one.
func TestRelativeTimeLiteral_DoesNotMatch(t *testing.T) {
	rows := []struct {
		name, value string
		literals    []string
	}{
		{name: "a fixed moment", value: "'epoch'", literals: relativeTimeLiterals},
		{name: "a function", value: "now()", literals: relativeTimeLiterals},
		{name: "a bare word", value: "now", literals: relativeTimeLiterals},
		{name: "an expression around the string", value: "('now')::timestamptz + interval '1 day'", literals: relativeTimeLiterals},
		{name: "two strings", value: "('now') || ('now')", literals: relativeTimeLiterals},
		{name: "a quote inside", value: "'now''s'", literals: relativeTimeLiterals},
		{name: "no literals", value: "'now'"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(defaultlit.RelativeTimeLiteral(row.value, row.literals), qt.IsFalse)
		})
	}
}

// Each spelling of a built-in date or time type is one, with or without a
// precision, a pg_catalog qualifier and upper case.
func TestDateTimeType_Matches(t *testing.T) {
	for _, spelling := range []string{
		"date", "TIMESTAMPTZ", "timestamp", "timestamp(3) with time zone", "TIMESTAMP WITHOUT TIME ZONE",
		"time (6)", "timetz", "time with time zone", "pg_catalog.date", " timestamptz(0) ",
	} {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(defaultlit.DateTimeType(spelling), qt.IsTrue)
		})
	}
}

// A name that only contains a date or time word, a type of another engine,
// an array, an interval and a created type are not one.
func TestDateTimeType_DoesNotMatch(t *testing.T) {
	for _, spelling := range []string{
		"update_kind", "timeslot", "uptime_state", "text", "datetime", "timestamptz[]", "interval",
		"app.moment", "timestamp(3", "",
	} {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(defaultlit.DateTimeType(spelling), qt.IsFalse)
		})
	}
}
