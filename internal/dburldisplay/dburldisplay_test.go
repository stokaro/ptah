package dburldisplay_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dburldisplay"
)

func TestFormat(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "PostgreSQL URL with password",
			input:    "postgres://user:secret123@localhost:5432/mydb",
			expected: "postgres://user:***@localhost:5432/mydb",
		},
		{
			name:     "PostgreSQL URL with query password",
			input:    "postgres://user:secret123@localhost:5432/mydb?sslmode=disable&sslpassword=querysecret",
			expected: "postgres://user:***@localhost:5432/mydb?sslmode=disable&sslpassword=redacted",
		},
		{
			name:     "PostgreSQL URL with query password and no user password",
			input:    "postgres://user@localhost:5432/mydb?password=querysecret",
			expected: "postgres://user@localhost:5432/mydb?password=redacted",
		},
		{
			name:     "PostgreSQL URL preserves fragment while redacting query",
			input:    "postgres://user:secret123@localhost:5432/mydb?password=querysecret#frag",
			expected: "postgres://user:***@localhost:5432/mydb?password=redacted#frag",
		},
		{
			name:     "PostgreSQL URL without password",
			input:    "postgres://user@localhost:5432/mydb",
			expected: "postgres://user@localhost:5432/mydb",
		},
		{
			name:     "Invalid URL",
			input:    "not-a-url",
			expected: "not-a-url",
		},
		{
			name:     "MySQL URL with password",
			input:    "mysql://root:password@localhost:3306/testdb",
			expected: "mysql://root:***@localhost:3306/testdb",
		},
		{
			name:     "MySQL tcp URL with query secret",
			input:    "mysql://root:password@tcp(localhost:3306)/testdb?parseTime=true&sslpassword=querysecret",
			expected: "mysql://root:***@tcp(localhost:3306)/testdb?parseTime=true&sslpassword=redacted",
		},
		{
			name:     "MySQL tcp URL does not redact embedded query URL credentials",
			input:    "mysql://root@tcp(localhost:3306)/testdb?callback=https%3A%2F%2Fx%3Ay%40example.test&password=querysecret",
			expected: "mysql://root@tcp(localhost:3306)/testdb?callback=https%3A%2F%2Fx%3Ay%40example.test&password=redacted",
		},
		{
			name:     "SQL Server URL with password and query secret",
			input:    "sqlserver://sa:VerySecret@localhost:1433?database=ptah&password=querysecret&encrypt=disable",
			expected: "sqlserver://sa:***@localhost:1433?database=ptah&encrypt=disable&password=redacted",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			result := dburldisplay.Format(tt.input)
			c.Assert(result, qt.Equals, tt.expected)
		})
	}
}
