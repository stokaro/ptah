package mssqlconnect

// White-box testing required: the URL go-mssqldb receives is built by an unexported converter, and no exported result reports it without a live server.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestConvertSQLServerURL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "canonical sqlserver URL passes through",
			input:    "sqlserver://sa:pass@localhost:1433?database=ptah&encrypt=disable",
			expected: "sqlserver://sa:pass@localhost:1433?database=ptah&encrypt=disable",
		},
		{
			name:     "mssql alias rewrites to preferred driver scheme",
			input:    "mssql://sa:pass@localhost:1433?database=ptah&encrypt=disable",
			expected: "sqlserver://sa:pass@localhost:1433?database=ptah&encrypt=disable",
		},
		{
			name:     "drops ptah-only schema parameter before driver sees URL",
			input:    "mssql://sa:pass@localhost:1433?database=ptah&schema=custom&encrypt=disable",
			expected: "sqlserver://sa:pass@localhost:1433?database=ptah&encrypt=disable",
		},
		{
			name:     "malformed URL falls back",
			input:    "::not-a-url::",
			expected: "::not-a-url::",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(convertSQLServerURL(tt.input), qt.Equals, tt.expected)
		})
	}
}
