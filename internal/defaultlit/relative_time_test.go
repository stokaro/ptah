package defaultlit_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/defaultlit"
)

var relativeTimeLiterals = []string{"NOW", "TODAY", "TOMORROW", "YESTERDAY"}

// A string the engine reads as a moment is the literal however it is cast and
// enclosed, as long as the first cast is not to a string type.
func TestRelativeTimeLiteral_Matches(t *testing.T) {
	rows := []struct{ name, value string }{
		{name: "quoted", value: "'now'"},
		{name: "mixed case and spaces", value: "' Today '"},
		{name: "a cast", value: "'now'::timestamp"},
		{name: "parentheses", value: "('now')"},
		{name: "two pairs of parentheses", value: "(('tomorrow'))"},
		{name: "a cast inside parentheses", value: "('now'::timestamptz)"},
		{name: "a cast after parentheses", value: "('yesterday')::timestamptz"},
		{name: "a timestamp cast to a date", value: "'now'::timestamp::date"},
		{name: "a cast to a created type", value: "'now'::app.moment"},
		{name: "a CAST to a date", value: "CAST('now' AS date)"},
		{name: "a CAST of a CAST", value: "cast(CAST('now' AS timestamp) as date)"},
		{name: "a typed literal", value: "date 'today'"},
		{name: "a typed literal of a spelled type", value: "TIMESTAMP WITH TIME ZONE 'now'"},
		{name: "an escape string", value: "E'now'"},
		{name: "an escape string with escapes", value: `e'no\x77\n'`},
		{name: "an escape string with an octal escape", value: `E'\156ow'`},
		{name: "a Unicode escape string", value: `U&'\006Eow'`},
		{name: "a dollar-quoted string", value: "$$now$$"},
		{name: "a tagged dollar-quoted string", value: "$a$today$a$"},
		// PostgreSQL 19 reads a tag outside ASCII, and stores DEFAULT $é$now$é$
		// on a timestamptz as the moment the table was created, as it does
		// DEFAULT 'now'.
		{name: "a dollar-quoted string whose tag is outside ASCII", value: "$\u00e9$now$\u00e9$"},
		{name: "a function-style cast", value: "date('today')"},
		{name: "a collation inside a cast", value: `('now' COLLATE "C")::date`},
		{name: "a time beside the word", value: "'tomorrow 10:00'"},
		{name: "punctuation beside the word", value: "'now,'"},
		{name: "braces around the word", value: "'{now}'"},
		{name: "arithmetic on the word", value: "'tomorrow'::date + 1"},
		{name: "an expression around the string", value: "('now')::timestamptz + interval '1 day'"},
		{name: "a function argument", value: "coalesce(NULL, 'now'::timestamp)"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(defaultlit.RelativeTimeLiteral(row.value, relativeTimeLiterals), qt.IsTrue)
		})
	}
}

// A fixed moment, a function, a string beside other SQL, a string cast to a
// string type before a date or time type, and an engine that names no literal
// are not one. PostgreSQL 18 keeps `('now'::text)::date` as written and
// converts the text for each row.
func TestRelativeTimeLiteral_DoesNotMatch(t *testing.T) {
	rows := []struct {
		name, value string
		literals    []string
	}{
		{name: "a fixed moment", value: "'epoch'", literals: relativeTimeLiterals},
		{name: "a function", value: "now()", literals: relativeTimeLiterals},
		{name: "a bare word", value: "now", literals: relativeTimeLiterals},
		{name: "a word inside a longer word", value: "'nowhere'", literals: relativeTimeLiterals},
		{name: "a text cast inside an expression", value: "'now'::text::timestamp + interval '1 day'", literals: relativeTimeLiterals},
		{name: "a function-style text cast", value: "text('now')::date", literals: relativeTimeLiterals},
		{name: "a typed text literal inside an expression", value: "text 'now' || ''", literals: relativeTimeLiterals},
		{name: "a dollar-quoted text cast", value: "$$now$$::text::date", literals: relativeTimeLiterals},
		// $1abc$ is the parameter $1 and a name on PostgreSQL, not a tag.
		{name: "a tag that starts with a digit", value: "$1abc$now$1abc$", literals: relativeTimeLiterals},
		{name: "text first", value: "('now'::text)::date", literals: relativeTimeLiterals},
		{name: "varchar first", value: "'now'::character varying(10)::timestamptz", literals: relativeTimeLiterals},
		{name: "quoted text first", value: `'now'::"pg_catalog"."text"::timestamp`, literals: relativeTimeLiterals},
		{name: "text alone", value: "'now'::TEXT", literals: relativeTimeLiterals},
		{name: "a chain from text", value: "'now'::text::date", literals: relativeTimeLiterals},
		{name: "a CAST to text first", value: "CAST('now' AS text)::date", literals: relativeTimeLiterals},
		{name: "a CAST of a CAST to text", value: "CAST(CAST('now' AS varchar(5)) AS date)", literals: relativeTimeLiterals},
		{name: "a collation before the cast", value: `('now'::text COLLATE "C")::date`, literals: relativeTimeLiterals},
		{name: "a typed text literal", value: "text 'now'", literals: relativeTimeLiterals},
		{name: "a national string", value: "N'now'", literals: relativeTimeLiterals},
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
// constructor whose elements are cast or typed by a cast around it, and casts
// and parentheses around each.
func TestRelativeTimeArrayLiteral_Matches(t *testing.T) {
	rows := []struct{ name, value string }{
		{name: "an array literal", value: "'{now}'"},
		{name: "a second element", value: "'{epoch, Today }'"},
		{name: "a quoted element", value: `'{"epoch","tomorrow"}'`},
		{name: "nested", value: "'{{epoch},{now}}'"},
		{name: "a dimension decoration", value: "'[1:1]={now}'"},
		{name: "a cast", value: "'{now}'::timestamptz[]"},
		{name: "parentheses and a cast", value: "('{now}')::timestamp(3)[]"},
		{name: "an ARRAY constructor with a cast element", value: "ARRAY['today'::date]"},
		{name: "an ARRAY constructor with a cast", value: "ARRAY['epoch', 'today']::date[]"},
		{name: "an array literal with a cast", value: "'{now}'::date[]"},
		{name: "a nested ARRAY constructor with a cast", value: "array[array['now']]::timestamp[]"},
		{name: "a nested ARRAY constructor with its own cast", value: "array[array['now']::timestamp[]]"},
		{name: "a word beside a function", value: "ARRAY['today', CURRENT_DATE]"},
		{name: "a word before a cast element", value: "ARRAY['now', '2020-01-01'::date]"},
		{name: "a word after a cast element", value: "ARRAY['2020-01-01'::date, 'now']"},
		{name: "a word beside a cast NULL", value: "ARRAY[NULL::date, 'now']"},
		{name: "a collated element under a cast", value: `ARRAY['now' COLLATE "C"]::date[]`},
		{name: "a typed literal element", value: "ARRAY[date 'today']"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(defaultlit.RelativeTimeArrayLiteral(row.value, relativeTimeLiterals), qt.IsTrue)
		})
	}
}

// Fixed moments, a function in a constructor, the word inside a longer
// element, an escaped quote, a scalar string, an uncast constructor, an array
// or element cast to a string type first, and an engine that names no literal
// are not one. PostgreSQL 18 reads an uncast ARRAY['today'] as a text array
// and refuses it on a date array column with its own error.
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
		{name: "an uncast ARRAY constructor", value: "ARRAY['today']", literals: relativeTimeLiterals},
		{name: "an uncast nested ARRAY constructor", value: "array[array['now']]", literals: relativeTimeLiterals},
		{name: "an array literal cast to text first", value: "'{now}'::text[]::date[]", literals: relativeTimeLiterals},
		{name: "a constructor cast to text first", value: "ARRAY['today']::text[]::date[]", literals: relativeTimeLiterals},
		{name: "an element cast to text", value: "ARRAY['today'::text]::date[]", literals: relativeTimeLiterals},
		{name: "a word beside NULL", value: "ARRAY['today', NULL]", literals: relativeTimeLiterals},
		{name: "a word beside a text element", value: "ARRAY['today', 'epoch'::text]", literals: relativeTimeLiterals},
		{name: "a word beside a NULL cast to text", value: "ARRAY['today', NULL::text]", literals: relativeTimeLiterals},
		{name: "uncast nested constructors", value: "ARRAY[ARRAY['now'], ARRAY['today']]", literals: relativeTimeLiterals},
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
		{name: "an array literal on a scalar", columnType: "timestamp", value: "'{now}'", want: true},
		{name: "a string on an array", columnType: "timestamp[]", value: "'now'"},
		{name: "an array on a text array", columnType: "text[]", value: "'{now}'"},
		{name: "a string on a created type", columnType: `"Timestamp"`, value: "'now'"},
		{name: "text cast to a date on a date", columnType: "date", value: "('now'::text)::date"},
		{name: "an uncast constructor on a date array", columnType: "date[]", value: "ARRAY['today']"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(defaultlit.RelativeTimeDefault(row.columnType, row.value, relativeTimeLiterals), qt.Equals, row.want)
		})
	}
}
