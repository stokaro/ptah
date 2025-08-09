package dbschema

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestRemovePostgresPoolParams(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "URL with both pool params",
			input:    "postgres://user:pass@localhost:5432/db?pool_max_conns=10&pool_min_conns=2&other=value",
			expected: "postgres://user:pass@localhost:5432/db?other=value",
		},
		{
			name:     "URL with only max_conns",
			input:    "postgres://user:pass@localhost:5432/db?pool_max_conns=10&other=value",
			expected: "postgres://user:pass@localhost:5432/db?other=value",
		},
		{
			name:     "URL with only min_conns",
			input:    "postgres://user:pass@localhost:5432/db?pool_min_conns=2&other=value",
			expected: "postgres://user:pass@localhost:5432/db?other=value",
		},
		{
			name:     "URL without pool params",
			input:    "postgres://user:pass@localhost:5432/db?other=value",
			expected: "postgres://user:pass@localhost:5432/db?other=value",
		},
		{
			name:     "URL with no query params",
			input:    "postgres://user:pass@localhost:5432/db",
			expected: "postgres://user:pass@localhost:5432/db",
		},
		{
			name:     "URL with pool params and multiple other params",
			input:    "postgres://user:pass@localhost:5432/db?sslmode=disable&pool_max_conns=20&timeout=30&pool_min_conns=5&application_name=myapp",
			expected: "postgres://user:pass@localhost:5432/db?application_name=myapp&sslmode=disable&timeout=30",
		},
		{
			name:     "URL with pool params at different positions",
			input:    "postgres://user:pass@localhost:5432/db?first=1&pool_max_conns=10&middle=2&pool_min_conns=3&last=4",
			expected: "postgres://user:pass@localhost:5432/db?first=1&last=4&middle=2",
		},
		{
			name:     "URL with only pool params (should result in no query string)",
			input:    "postgres://user:pass@localhost:5432/db?pool_max_conns=10&pool_min_conns=2",
			expected: "postgres://user:pass@localhost:5432/db",
		},
		{
			name:     "Invalid URL fallback",
			input:    "not-a-url",
			expected: "not-a-url",
		},
		{
			name:     "URL with special characters in pool params",
			input:    "postgres://user:pass@localhost:5432/db?pool_max_conns=10&other=special%20value&pool_min_conns=2",
			expected: "postgres://user:pass@localhost:5432/db?other=special+value",
		},
		{
			name:     "Empty URL",
			input:    "",
			expected: "",
		},
		{
			name:     "URL with case variations (should not match)",
			input:    "postgres://user:pass@localhost:5432/db?POOL_MAX_CONNS=10&Pool_Min_Conns=2&other=value",
			expected: "postgres://user:pass@localhost:5432/db?POOL_MAX_CONNS=10&Pool_Min_Conns=2&other=value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			result := removePostgresPoolParams(tt.input)
			c.Assert(result, qt.Equals, tt.expected, qt.Commentf("removePostgresPoolParams(%q) = %q, want %q", tt.input, result, tt.expected))
		})
	}
}

func TestRemovePostgresPoolParams_ParameterOrdering(t *testing.T) {
	c := qt.New(t)

	// Test that the function produces consistent results regardless of input parameter order
	input1 := "postgres://user:pass@localhost:5432/db?pool_max_conns=10&other=value&pool_min_conns=2"
	input2 := "postgres://user:pass@localhost:5432/db?pool_min_conns=2&pool_max_conns=10&other=value"
	input3 := "postgres://user:pass@localhost:5432/db?other=value&pool_max_conns=10&pool_min_conns=2"

	result1 := removePostgresPoolParams(input1)
	result2 := removePostgresPoolParams(input2)
	result3 := removePostgresPoolParams(input3)
	
	// All should result in the same cleaned URL
	expected := "postgres://user:pass@localhost:5432/db?other=value"
	c.Assert(result1, qt.Equals, expected)
	c.Assert(result2, qt.Equals, expected)
	c.Assert(result3, qt.Equals, expected)
	
	// All results should be identical
	c.Assert(result1, qt.Equals, result2)
	c.Assert(result2, qt.Equals, result3)
}

func TestRemovePostgresPoolParams_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "URL with fragment",
			input:    "postgres://user:pass@localhost:5432/db?pool_max_conns=10#fragment",
			expected: "postgres://user:pass@localhost:5432/db#fragment",
		},
		{
			name:     "URL with port and path",
			input:    "postgres://user:pass@localhost:5432/path/to/db?pool_max_conns=10&pool_min_conns=2",
			expected: "postgres://user:pass@localhost:5432/path/to/db",
		},
		{
			name:     "URL with encoded characters",
			input:    "postgres://user:pass%40word@localhost:5432/db?pool_max_conns=10&other=value%20with%20spaces",
			expected: "postgres://user:pass%40word@localhost:5432/db?other=value+with+spaces",
		},
		{
			name:     "URL with duplicate non-pool params (should preserve all)",
			input:    "postgres://user:pass@localhost:5432/db?other=value1&pool_max_conns=10&other=value2",
			expected: "postgres://user:pass@localhost:5432/db?other=value1&other=value2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			result := removePostgresPoolParams(tt.input)
			c.Assert(result, qt.Equals, tt.expected, qt.Commentf("removePostgresPoolParams(%q) = %q, want %q", tt.input, result, tt.expected))
		})
	}
}
