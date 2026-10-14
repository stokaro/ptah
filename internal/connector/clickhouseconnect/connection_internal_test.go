package clickhouseconnect

// White-box testing required: the URL clickhouse-go receives is built by an unexported converter, and no exported result reports it without a live server.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestConvertClickHouseURL(t *testing.T) {
	tests := []struct {
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
			name:     "native TLS port 9440 round-trips",
			input:    "clickhouse://default@localhost:9440/analytics",
			expected: "clickhouse://default@localhost:9440/analytics",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(convertClickHouseURL(tt.input), qt.Equals, tt.expected)
		})
	}
}

func TestConvertClickHouseURL_PreservesQueryParameters(t *testing.T) {
	c := qt.New(t)

	got := convertClickHouseURL("clickhouse://default:secret@localhost:9000/analytics?secure=true&dial_timeout=10s")

	c.Assert(got, qt.Contains, "clickhouse://default:secret@localhost:9000/analytics?")
	for kv := range strings.SplitSeq("secure=true&dial_timeout=10s", "&") {
		c.Assert(got, qt.Contains, kv)
	}
}

func TestConvertClickHouseURL_ExactQueryRoundTrips(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "preserves secure=true on native port",
			input:    "clickhouse://default@localhost:9000/analytics?secure=true",
			expected: "clickhouse://default@localhost:9000/analytics?secure=true",
		},
		{
			name:     "HTTP-SSL port 8443 with secure flag round-trips",
			input:    "clickhouse://default@localhost:8443/db?secure=true",
			expected: "clickhouse://default@localhost:8443/db?secure=true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(convertClickHouseURL(tt.input), qt.Equals, tt.expected)
		})
	}
}
