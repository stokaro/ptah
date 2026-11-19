package normalize_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/normalize"
)

// TestPostgreSQLTypeCastingIssue32 tests the specific issue described in GitHub issue #32
// where PostgreSQL type casting in default values causes redundant migration generation
func TestPostgreSQLTypeCastingIssue32(t *testing.T) {
	// Test cases from the actual issue #32
	testCases := []struct {
		name         string
		dbDefault    string // What PostgreSQL returns from information_schema.columns.column_default
		goDefault    string // What the Go model annotation specifies
		expectedNorm string // What both should normalize to
		description  string
	}{
		{
			name:         "text default user",
			dbDefault:    "'user'::text",
			goDefault:    "user",
			expectedNorm: "user",
			description:  "PostgreSQL stores 'user'::text but Go model expects user",
		},
		{
			name:         "text default active",
			dbDefault:    "'active'::text",
			goDefault:    "active",
			expectedNorm: "active",
			description:  "PostgreSQL stores 'active'::text but Go model expects active",
		},
		{
			name:         "bigint default zero",
			dbDefault:    "'0'::bigint",
			goDefault:    "0",
			expectedNorm: "0",
			description:  "PostgreSQL stores '0'::bigint but Go model expects 0",
		},
		{
			name:         "bigint default number",
			dbDefault:    "'123'::bigint",
			goDefault:    "123",
			expectedNorm: "123",
			description:  "PostgreSQL stores '123'::bigint but Go model expects 123",
		},
		{
			name:         "boolean default true",
			dbDefault:    "'true'::boolean",
			goDefault:    "true",
			expectedNorm: "true",
			description:  "PostgreSQL stores 'true'::boolean but Go model expects true",
		},
		{
			name:         "boolean default false",
			dbDefault:    "'false'::boolean",
			goDefault:    "false",
			expectedNorm: "false",
			description:  "PostgreSQL stores 'false'::boolean but Go model expects false",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			// Normalize the database default value (with type casting)
			dbNormalized := normalize.DefaultValue(tc.dbDefault, "")

			// Normalize the Go model default value (without type casting)
			goNormalized := normalize.DefaultValue(tc.goDefault, "")

			// Both should normalize to the same value
			c.Assert(dbNormalized, qt.Equals, tc.expectedNorm,
				qt.Commentf("Database default '%s' should normalize to '%s', got '%s'",
					tc.dbDefault, tc.expectedNorm, dbNormalized))

			c.Assert(goNormalized, qt.Equals, tc.expectedNorm,
				qt.Commentf("Go default '%s' should normalize to '%s', got '%s'",
					tc.goDefault, tc.expectedNorm, goNormalized))

			// Most importantly, they should be equal to each other
			c.Assert(dbNormalized, qt.Equals, goNormalized,
				qt.Commentf("Database default '%s' and Go default '%s' should normalize to the same value. DB: '%s', Go: '%s'",
					tc.dbDefault, tc.goDefault, dbNormalized, goNormalized))
		})
	}
}

// TestPostgreSQLTypeCastingEdgeCases tests additional edge cases for PostgreSQL type casting
func TestPostgreSQLTypeCastingEdgeCases(t *testing.T) {
	testCases := []struct {
		name        string
		input       string
		expected    string
		description string
	}{
		{
			name:        "schema qualified type",
			input:       "'value'::public.custom_type",
			expected:    "value",
			description: "Should handle schema-qualified types",
		},
		{
			name:        "complex value with colons",
			input:       "'value::with::colons'::text",
			expected:    "value::with::colons",
			description: "Should only remove the last ::type part",
		},
		{
			name:        "numeric with precision",
			input:       "'123.456'::numeric(10,2)",
			expected:    "'123.456'::decimal(10,2)",
			description: "A precision rounds the literal, so the cast is part of the value",
		},
		{
			name:        "timestamp with timezone",
			input:       "'2023-01-01 00:00:00'::timestamp with time zone",
			expected:    "2023-01-01 00:00:00",
			description: "Should handle complex type names",
		},
		{
			name:        "no type casting",
			input:       "'simple_value'",
			expected:    "simple_value",
			description: "Should handle values without type casting",
		},
		{
			name:        "unquoted type cast",
			input:       "0::bigint",
			expected:    "0",
			description: "Should handle unquoted values with type casting",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			result := normalize.DefaultValue(tc.input, "")
			c.Assert(result, qt.Equals, tc.expected,
				qt.Commentf("Input '%s' should normalize to '%s', got '%s'. %s",
					tc.input, tc.expected, result, tc.description))
		})
	}
}

// TestDefaultValue_FoldsOnlyAValuePreservingCast pins which casts a default
// loses before it is compared. PostgreSQL types the literal it stores, and
// every default is converted to the column's type as a row is stored, so a
// cast to the column's own type is folded, and so is a cast to text or
// character varying on a column that stores characters. A cast that
// can change the value stays, and the
// default is compared as the expression it is: each kept row is a default a
// declaration can change by adding the cast, as `n numeric DEFAULT 1.5`
// against `DEFAULT (1.5::integer)`, which PostgreSQL 18 stores as
// `(1.5)::integer`. The kept spelling is the one both catalogs reduce to.
func TestDefaultValue_FoldsOnlyAValuePreservingCast(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		typeName string
		want     string
	}{
		{name: "a varchar literal", input: "'abc'::character varying", typeName: "varchar", want: "abc"},
		{name: "a negative integer", input: "'-1'::integer", typeName: "integer", want: "-1"},
		{name: "an array literal", input: "'{}'::text[]", typeName: "text[]", want: "{}"},
		{name: "a timestamp literal", input: "'2020-01-01 10:30:00'::timestamp without time zone", typeName: "timestamp", want: "2020-01-01 10:30:00"},
		{name: "a literal of a qualified enum", input: "'a'::app.status", typeName: "status", want: "a"},
		{name: "a quoted type name", input: "'101'::\"bit\"", typeName: "bit", want: "101"},
		{name: "NULL with a type", input: "NULL::character varying", typeName: "varchar", want: ""},
		{name: "NULL with a length", input: "NULL::character varying(3)", typeName: "varchar", want: ""},
		{name: "an integer in parentheses", input: "(0)::bigint", typeName: "integer", want: "0"},
		{name: "a decimal to numeric", input: "1.5::numeric", typeName: "decimal", want: "1.5"},
		{name: "any value to text", input: "lower(name)::text", typeName: "text", want: "lower(name)"},
		{name: "a call in parentheses to text", input: "(now())::text", typeName: "text", want: "now()"},
		{name: "a literal to text on a char column", input: "'ab'::text", typeName: "character(5)", want: "ab"},
		{name: "a literal to text on a regclass column", input: "'users'::text", typeName: "regclass", want: "'users'::text"},
		{name: "a literal to text on a numeric column", input: "'1.5'::text", typeName: "numeric", want: "'1.5'::text"},
		{name: "a decimal to integer rounds", input: "(1.5)::integer", typeName: "decimal", want: "1.5::integer"},
		{name: "CockroachDB spells the rounding cast", input: "1.5::INT8", typeName: "decimal", want: "1.5::integer"},
		{name: "a length truncates", input: "'abcd'::character varying(3)", typeName: "varchar", want: "'abcd'::varchar(3)"},
		{name: "CockroachDB spells the truncating cast", input: "'abcd'::VARCHAR(3)", typeName: "varchar", want: "'abcd'::varchar(3)"},
		{name: "a date drops the time", input: "(now())::date", typeName: "timestamp", want: "now()::date"},
		{name: "CockroachDB spells the date cast", input: "now()::DATE", typeName: "timestamp", want: "now()::date"},
		{name: "a date literal on a timestamp column", input: "'2020-01-01 10:30'::date", typeName: "timestamp", want: "'2020-01-01 10:30'::date"},
		{name: "a decimal to a float can round", input: "0.1::real", typeName: "decimal", want: "0.1::real"},
		{name: "parentheses that group stay", input: "(1 + 2.5)::integer", typeName: "numeric", want: "(1 + 2.5)::integer"},
		{name: "an array constructor to the column's type", input: "ARRAY[]::text[]", typeName: "text[]", want: "ARRAY[]"},
		{name: "a decimal to the column's own integer type", input: "(1.5)::integer", typeName: "integer", want: "1.5"},
		{name: "a length on the column's own type truncates", input: "'abcd'::varchar(3)", typeName: "varchar(10)", want: "'abcd'::varchar(3)"},
		{name: "a cast with no column type known stays", input: "ARRAY[]::text[]", typeName: "", want: "ARRAY[]::text[]"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(normalize.DefaultValue(test.input, test.typeName), qt.Equals, test.want)
		})
	}
}
