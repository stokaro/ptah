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
		`"timestamp"`, `"pg_catalog"."timestamptz"`, `pg_catalog."date"`, `"timestamptz"(3)`, ` "time" `,
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
		`"Timestamp"`, `"TIMESTAMPTZ"`, `"timestamp with time zone"`, `"app"."timestamp"`, `"timestamp"[]`, `"timestamp`,
	} {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(defaultlit.DateTimeType(spelling), qt.IsFalse)
		})
	}
}

// An array type names its element type, however its dimensions are written.
func TestArrayElementType_Matches(t *testing.T) {
	rows := []struct{ spelling, want string }{
		{spelling: "timestamptz[]", want: "timestamptz"},
		{spelling: "timestamp(3) with time zone[]", want: "timestamp(3) with time zone"},
		{spelling: "date[3][]", want: "date"},
		{spelling: "date [ 3 ]", want: "date"},
		{spelling: "timestamp ARRAY", want: "timestamp"},
		{spelling: "timestamp array[4]", want: "timestamp"},
		{spelling: `"timestamp"[]`, want: `"timestamp"`},
		{spelling: "app.moment[]", want: "app.moment"},
	}
	for _, row := range rows {
		t.Run(row.spelling, func(t *testing.T) {
			c := qt.New(t)
			got, ok := defaultlit.ArrayElementType(row.spelling)
			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, row.want)
		})
	}
}

// A scalar type, a name that ends in array, a bracket in a quoted name and a
// bracket that holds no dimension are not an array.
func TestArrayElementType_DoesNotMatch(t *testing.T) {
	for _, spelling := range []string{"timestamptz", "myarray", `"a[]"`, "[]", "timestamp[x]", "ARRAY", ""} {
		t.Run(spelling, func(t *testing.T) {
			c := qt.New(t)
			got, ok := defaultlit.ArrayElementType(spelling)
			c.Assert(ok, qt.IsFalse)
			c.Assert(got, qt.Equals, "")
		})
	}
}

// An array default holds the literal however the array is written: a quoted
// array literal, quoted elements, nesting, a dimension decoration, an ARRAY
// constructor, and casts and parentheses around each.
func TestRelativeTimeArrayLiteral_Matches(t *testing.T) {
	rows := []struct{ name, value string }{
		{name: "an array literal", value: "'{now}'"},
		{name: "a second element", value: "'{epoch, Today }'"},
		{name: "a quoted element", value: `'{"epoch","tomorrow"}'`},
		{name: "nested", value: "'{{epoch},{now}}'"},
		{name: "a dimension decoration", value: "'[1:1]={now}'"},
		{name: "a cast", value: "'{now}'::timestamptz[]"},
		{name: "parentheses and a cast", value: "('{now}')::timestamp(3)[]"},
		{name: "an ARRAY constructor", value: "ARRAY['today']"},
		{name: "an ARRAY constructor with a cast", value: "ARRAY['epoch', 'today'::date]::date[]"},
		{name: "a nested ARRAY constructor", value: "array[array['now']]"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(defaultlit.RelativeTimeArrayLiteral(row.value, relativeTimeLiterals), qt.IsTrue)
		})
	}
}

// Fixed moments, a function in a constructor, the word inside a longer
// element, an escaped quote, a scalar string and an engine that names no
// literal are not one.
func TestRelativeTimeArrayLiteral_DoesNotMatch(t *testing.T) {
	rows := []struct {
		name, value string
		literals    []string
	}{
		{name: "fixed moments", value: "'{epoch,\"2026-01-01\"}'", literals: relativeTimeLiterals},
		{name: "a function in a constructor", value: "ARRAY[now()]", literals: relativeTimeLiterals},
		{name: "the word inside an element", value: "'{nowhere}'", literals: relativeTimeLiterals},
		{name: "a scalar string", value: "'now'", literals: relativeTimeLiterals},
		{name: "an empty array", value: "'{}'", literals: relativeTimeLiterals},
		{name: "an expression around the array", value: "'{now}'::timestamptz[] || '{epoch}'", literals: relativeTimeLiterals},
		{name: "no literals", value: "'{now}'"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(defaultlit.RelativeTimeArrayLiteral(row.value, row.literals), qt.IsFalse)
		})
	}
}

// The predicate pairs each default shape with its column type: a string on a
// date or time type, an array on an array of one.
func TestRelativeTimeDefault(t *testing.T) {
	rows := []struct {
		name, columnType, value string
		want                    bool
	}{
		{name: "a string on a timestamp", columnType: "timestamp", value: "'now'", want: true},
		{name: "a string on a quoted timestamp", columnType: `"pg_catalog"."timestamptz"`, value: "'now'", want: true},
		{name: "an array on a timestamp array", columnType: "timestamptz[]", value: "'{now}'", want: true},
		{name: "a constructor on a date array", columnType: "date ARRAY", value: "ARRAY['today']::date[]", want: true},
		{name: "an array on a scalar", columnType: "timestamp", value: "'{now}'"},
		{name: "a string on an array", columnType: "timestamp[]", value: "'now'"},
		{name: "an array on a text array", columnType: "text[]", value: "'{now}'"},
		{name: "a string on a created type", columnType: `"Timestamp"`, value: "'now'"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(defaultlit.RelativeTimeDefault(row.columnType, row.value, relativeTimeLiterals), qt.Equals, row.want)
		})
	}
}
