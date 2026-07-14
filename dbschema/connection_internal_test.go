package dbschema

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/dbschema/types"
)

func TestConvertClickHouseURL(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "passthrough canonical URL",
			input:    "clickhouse://default:secret@localhost:9000/analytics",
			expected: "clickhouse://default:secret@localhost:9000/analytics",
		},
		{
			name:     "preserves query parameters",
			input:    "clickhouse://default:secret@localhost:9000/analytics?secure=true&dial_timeout=10s",
			expected: "clickhouse://default:secret@localhost:9000/analytics?secure=true&dial_timeout=10s",
		},
		{
			name:     "rewrites uppercase scheme",
			input:    "CLICKHOUSE://default@localhost:9000/db",
			expected: "clickhouse://default@localhost:9000/db",
		},
		{
			name:     "returns input on malformed URL",
			input:    "::not-a-url::",
			expected: "::not-a-url::",
		},
		{
			name:     "preserves secure=true on native port",
			input:    "clickhouse://default@localhost:9000/analytics?secure=true",
			expected: "clickhouse://default@localhost:9000/analytics?secure=true",
		},
		{
			name:     "native TLS port 9440 round-trips",
			input:    "clickhouse://default@localhost:9440/analytics",
			expected: "clickhouse://default@localhost:9440/analytics",
		},
		{
			name:     "HTTP-SSL port 8443 with secure flag round-trips",
			input:    "clickhouse://default@localhost:8443/db?secure=true",
			expected: "clickhouse://default@localhost:8443/db?secure=true",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			got := convertClickHouseURL(tc.input)
			// url.Parse() doesn't always preserve raw query ordering. For the
			// expected URLs that carry multiple parameters we assert by
			// fragments; single-param and no-param URLs round-trip exactly.
			if strings.Count(tc.expected, "&") > 0 {
				prefix, _, _ := strings.Cut(tc.expected, "?")
				c.Assert(got, qt.Contains, prefix)
				for kv := range strings.SplitSeq(tc.expected[strings.Index(tc.expected, "?")+1:], "&") {
					c.Assert(got, qt.Contains, kv)
				}
				return
			}
			c.Assert(got, qt.Equals, tc.expected)
		})
	}
}

func TestConvertPostgresWireURL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "postgres passthrough with pool params removed",
			input:    "postgres://user:pass@localhost:5432/app?pool_max_conns=10&sslmode=disable",
			expected: "postgres://user:pass@localhost:5432/app?sslmode=disable",
		},
		{
			name:     "cockroachdb scheme rewrites to postgres for pgx",
			input:    "cockroachdb://root@localhost:26257/defaultdb?sslmode=disable",
			expected: "postgres://root@localhost:26257/defaultdb?sslmode=disable",
		},
		{
			name:     "yugabytedb scheme rewrites to postgres for pgx",
			input:    "yugabytedb://yugabyte@localhost:5433/yugabyte",
			expected: "postgres://yugabyte@localhost:5433/yugabyte",
		},
		{
			name:     "spanner scheme rewrites to postgres for pgx",
			input:    "spanner://user@localhost:5432/db",
			expected: "postgres://user@localhost:5432/db",
		},
		{
			name:     "malformed URL falls back to cleaned input",
			input:    "::not-a-url::",
			expected: "::not-a-url::",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(convertPostgresWireURL(tt.input), qt.Equals, tt.expected)
		})
	}
}

func TestDetectPostgresWireDialect(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		version  string
		expected string
	}{
		{
			name:     "plain postgres",
			declared: "postgres",
			version:  "PostgreSQL 16.3 (Debian 16.3-1.pgdg120+1)",
			expected: "postgres",
		},
		{
			name:     "cockroach detected from postgres URL",
			declared: "postgres",
			version:  "CockroachDB CCL v23.2.5 (x86_64-pc-linux-gnu)",
			expected: "cockroachdb",
		},
		{
			name:     "yugabyte detected from postgres URL",
			declared: "postgres",
			version:  "PostgreSQL 11.2-YB-2.25.1.0-b0 on x86_64-pc-linux-gnu, compiled by clang, YugabyteDB",
			expected: "yugabytedb",
		},
		{
			name:     "spanner detected from postgres URL",
			declared: "postgres",
			version:  "Cloud Spanner PostgreSQL interface",
			expected: "spanner",
		},
		{
			name:     "explicit cockroach survives generic banner",
			declared: "cockroachdb",
			version:  "PostgreSQL-compatible server",
			expected: "cockroachdb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(detectPostgresWireDialect(tt.declared, tt.version), qt.Equals, tt.expected)
		})
	}
}

func TestDatabaseConnectionInfoClonesCapabilities(t *testing.T) {
	c := qt.New(t)

	conn := &DatabaseConnection{
		info: types.DBInfo{
			Dialect:      "cockroachdb",
			Capabilities: capability.CockroachDB23(),
		},
	}

	info := conn.Info()
	info.Capabilities[capability.RowLevelSecurity] = true

	c.Assert(conn.Info().Capabilities.Has(capability.RowLevelSecurity), qt.IsFalse)
}

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
