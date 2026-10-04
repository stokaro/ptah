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
			// net/url reads the rest of the password as a fragment and refuses
			// the port it is left with.
			name:     "PostgreSQL URL with a # in its password",
			input:    "postgres://user:p#ss@localhost:5432/mydb",
			expected: "postgres://user:***@localhost:5432/mydb",
		},
		{
			name:     "PostgreSQL URL with a % that starts no escape in its password",
			input:    "postgres://user:p%zz@localhost:5432/mydb",
			expected: "postgres://user:***@localhost:5432/mydb",
		},
		{
			// Parsed, this is host user, port 5 and a path holding the rest
			// of the password; the @ in the path is what gives it away.
			name:     "PostgreSQL URL with a / in its password",
			input:    "postgres://user:5/ss@localhost/mydb",
			expected: "postgres://user:***@localhost/mydb",
		},
		{
			name:     "PostgreSQL URL with a ? in its password and a secret in its query",
			input:    "postgres://user:p?ss@localhost/mydb?sslmode=disable&sslpassword=querysecret",
			expected: "postgres://user:***@localhost/mydb?sslmode=disable&sslpassword=redacted",
		},
		{
			// Parsed, this is user p, host ss and a fragment holding the
			// rest; the first @ is part of the password.
			name:     "PostgreSQL URL with an @ and a # in its password",
			input:    "postgres://user:p@ss#word@localhost/mydb",
			expected: "postgres://user:***@localhost/mydb",
		},
		{
			// Parsed, this is host user, port 5 and a query whose key holds
			// the rest of the password.
			name:     "PostgreSQL URL with a ? after a digit in its password",
			input:    "postgres://user:5?ss@localhost/mydb",
			expected: "postgres://user:***@localhost/mydb",
		},
		{
			// An @ in the query is a value, not the end of credentials.
			name:     "PostgreSQL URL with an @ in a query value",
			input:    "postgres://localhost:5432/mydb?application_name=ops@example",
			expected: "postgres://localhost:5432/mydb?application_name=ops%40example",
		},
		{
			name:     "keyword/value string with a password",
			input:    "host=127.0.0.1 port=5432 user=app password=s3cret dbname=mydb",
			expected: "host=127.0.0.1 port=5432 user=app password=redacted dbname=mydb",
		},
		{
			// A quoted value, a backslash-escaped one, and the spacing around
			// the = signs are read the way libpq reads them and kept as written.
			name:     "keyword/value string with quoted and escaped secrets",
			input:    `host = db  password = 'it\'s a secret'  sslpassword=a\ b dbname=mydb`,
			expected: `host = db  password = redacted  sslpassword=redacted dbname=mydb`,
		},
		{
			name:     "keyword/value string without a secret",
			input:    "host=/tmp dbname=mydb sslmode=disable",
			expected: "host=/tmp dbname=mydb sslmode=disable",
		},
		{
			// The YDB SDK reads a static user and password from the
			// authority and an access token from the token parameter; it
			// reads no other credential from a URL.
			name:     "YDB URL with a user, a password and a token",
			input:    "ydbs://user:secret123@ydb.example:2135/local?token=t0k3n",
			expected: "ydbs://user:***@ydb.example:2135/local?token=redacted",
		},
		{
			name:     "YDB URL with its database in a parameter",
			input:    "ydb://localhost:2136/?database=%2Flocal&token=t0k3n",
			expected: "ydb://localhost:2136/?database=%2Flocal&token=redacted",
		},
		{
			// internal/ydburl refuses a user in the monitoring endpoint, and
			// a command prints the URL before it is parsed: the line naming
			// the target must not carry the password the refusal is about.
			name:     "YDB URL with credentials in its monitoring endpoint",
			input:    "ydb://h:2136/local?monitoring=http://viewer:s3cret@mon.example:8765&go_balancer=disable",
			expected: "ydb://h:2136/local?go_balancer=disable&monitoring=http%3A%2F%2Fredacted%40mon.example%3A8765",
		},
		{
			// A user name alone can be a token, and the endpoint takes none of its own.
			name:     "YDB URL with a user name alone in its monitoring endpoint",
			input:    "ydbs://h/local?monitoring=https://t0k3n@mon.example:8765",
			expected: "ydbs://h/local?monitoring=https%3A%2F%2Fredacted%40mon.example%3A8765",
		},
		{
			name:     "YDB URL with credentials in a monitoring parameter spelled in capitals",
			input:    "ydb://h/local?MONITORING=http://viewer:s3cret@mon.example:8765",
			expected: "ydb://h/local?MONITORING=http%3A%2F%2Fredacted%40mon.example%3A8765",
		},
		{
			// No scheme, so no user info can be read: the value goes whole.
			name:     "YDB URL with credentials in a monitoring endpoint that names no scheme",
			input:    "ydb://h/local?monitoring=viewer:s3cret@mon.example:8765",
			expected: "ydb://h/local?monitoring=redacted",
		},
		{
			// The port does not parse, so neither does the endpoint.
			name:     "YDB URL with a monitoring endpoint that does not parse",
			input:    "ydb://h/local?monitoring=http://viewer:s3cret@mon.example:87x5",
			expected: "ydb://h/local?monitoring=redacted",
		},
		{
			name:     "YDB URL with a monitoring endpoint and no credentials",
			input:    "ydb://h/local?monitoring=http://mon.example:8765",
			expected: "ydb://h/local?monitoring=http%3A%2F%2Fmon.example%3A8765",
		},
		{
			// The parameter is YDB's; another scheme's URL is left as it is,
			// as the MySQL rows below leave a URL-shaped value alone.
			name:     "PostgreSQL URL with a monitoring parameter",
			input:    "postgres://user@localhost:5432/mydb?monitoring=http://viewer:s3cret@mon.example:8765",
			expected: "postgres://user@localhost:5432/mydb?monitoring=http%3A%2F%2Fviewer%3As3cret%40mon.example%3A8765",
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
			// net/url refuses the tcp() form, so only the MySQL-family branch
			// can redact it: a scheme the connector opens and that branch did
			// not recognize would print the password whole.
			name:     "maria tcp URL with password",
			input:    "maria://root:password@tcp(localhost:3306)/testdb",
			expected: "maria://root:***@tcp(localhost:3306)/testdb",
		},
		{
			name:     "upper-case MariaDB tcp URL with password",
			input:    "MARIADB://root:password@tcp(localhost:3306)/testdb",
			expected: "MARIADB://root:***@tcp(localhost:3306)/testdb",
		},
		{
			name:     "maria URL with password",
			input:    "maria://root:password@localhost:3306/testdb",
			expected: "maria://root:***@localhost:3306/testdb",
		},
		{
			name:     "socket URL with password",
			input:    "mysql+unix://root:password@/run/mysqld/mysqld.sock?database=testdb",
			expected: "mysql+unix://root:***@/run/mysqld/mysqld.sock?database=testdb",
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
