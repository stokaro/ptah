package normalize_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/schemadiff/internal/normalize"
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
		name         string
		input        string
		expected     string
		description  string
	}{
		{
			name:         "schema qualified type",
			input:        "'value'::public.custom_type",
			expected:     "value",
			description:  "Should handle schema-qualified types",
		},
		{
			name:         "complex value with colons",
			input:        "'value::with::colons'::text",
			expected:     "value::with::colons",
			description:  "Should only remove the last ::type part",
		},
		{
			name:         "numeric with precision",
			input:        "'123.45'::numeric(10,2)",
			expected:     "123.45",
			description:  "Should handle numeric types with precision",
		},
		{
			name:         "timestamp with timezone",
			input:        "'2023-01-01 00:00:00'::timestamp with time zone",
			expected:     "2023-01-01 00:00:00",
			description:  "Should handle complex type names",
		},
		{
			name:         "no type casting",
			input:        "'simple_value'",
			expected:     "simple_value",
			description:  "Should handle values without type casting",
		},
		{
			name:         "unquoted type cast",
			input:        "0::bigint",
			expected:     "0",
			description:  "Should handle unquoted values with type casting",
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
