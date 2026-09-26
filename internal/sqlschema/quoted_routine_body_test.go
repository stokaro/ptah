package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/sqlschema"
)

// A routine body written as a string literal is read as the text the server
// stores, which is what the plan writes back between dollar quotes. Each want
// is the pg_proc.prosrc PostgreSQL 18.6 stored for the statement. Read with its
// quoting kept, a function's body keeps both quotes of each doubled pair and
// fails to apply, a procedure's body is cut at its first doubled quote, and a
// procedure whose body is an escape string has no body at all
// (stokaro/ptah#3691).
func TestRead_AQuotedRoutineBodyIsItsText(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      string
	}{
		{
			name:      "a function body with a doubled quote",
			statement: `CREATE FUNCTION f() RETURNS text LANGUAGE sql AS 'SELECT ''x''';`,
			want:      `SELECT 'x'`,
		},
		{
			name:      "a function body in an escape string",
			statement: `CREATE FUNCTION f() RETURNS text LANGUAGE sql AS E'SELECT \'x\'';`,
			want:      `SELECT 'x'`,
		},
		{
			name:      "a procedure body with a doubled quote",
			statement: `CREATE PROCEDURE p() LANGUAGE sql AS 'INSERT INTO t VALUES (''x'')';`,
			want:      `INSERT INTO t VALUES ('x')`,
		},
		{
			name:      "a procedure body in an escape string",
			statement: `CREATE PROCEDURE p() LANGUAGE sql AS E'INSERT INTO t VALUES (\'x\')';`,
			want:      `INSERT INTO t VALUES ('x')`,
		},
		{
			name:      "a tagged dollar quote holding two dollars",
			statement: `CREATE FUNCTION f() RETURNS text LANGUAGE sql AS $f$SELECT $$x$$$f$;`,
			want:      `SELECT $$x$$`,
		},
		{
			name:      "a backslash in a standard string",
			statement: `CREATE FUNCTION f() RETURNS text LANGUAGE sql AS 'SELECT ''C:\new''';`,
			want:      `SELECT 'C:\new'`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.statement), platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Functions, qt.HasLen, 1)
			c.Assert(database.Functions[0].Body, qt.Equals, test.want)
		})
	}
}

// A body whose closing quote is missing is refused rather than read as a body,
// for either statement.
func TestRead_AnUnterminatedRoutineBodyIsRefused(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		wantErr   string
	}{
		{
			name:      "a function",
			statement: `CREATE FUNCTION f() RETURNS text LANGUAGE sql AS 'SELECT 1`,
			wantErr:   `unsupported CREATE FUNCTION body: 'SELECT 1 is not a complete string literal at position \d+`,
		},
		{
			name:      "a procedure",
			statement: `CREATE PROCEDURE p() LANGUAGE sql AS 'SELECT 1`,
			wantErr:   `unsupported CREATE PROCEDURE body: 'SELECT 1 is not a complete string literal`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.statement), platform.Postgres)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Functions, qt.HasLen, 0)
		})
	}
}
