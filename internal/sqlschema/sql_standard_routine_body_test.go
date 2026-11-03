package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/sqlschema"
)

// A routine with a SQL-standard body is read as PostgreSQL 18.6 records it:
// a RETURN body keeps its keyword, as pg_get_function_sqlbody prints it, and
// a routine that names no language is in LANGUAGE sql, as pg_language reports
// it. Read without the keyword, a RETURN body is written inside dollar quotes
// and refused; read without the language, the routine is written as PL/pgSQL
// and a BEGIN ATOMIC body is not ordered after the table it reads.
func TestRead_ASQLStandardRoutineBodyIsReadAsTheServerRecordsIt(t *testing.T) {
	tests := []struct {
		name         string
		statement    string
		wantBody     string
		wantLanguage string
	}{
		{
			name:         "a RETURN body",
			statement:    "CREATE FUNCTION add2(a int, b int) RETURNS int LANGUAGE sql IMMUTABLE RETURN a + b;",
			wantBody:     "RETURN a + b",
			wantLanguage: "sql",
		},
		{
			name:         "a RETURN body that reads a query",
			statement:    "CREATE FUNCTION cnt() RETURNS bigint LANGUAGE sql STABLE RETURN (SELECT count(*) FROM t);",
			wantBody:     "RETURN (SELECT count(*) FROM t)",
			wantLanguage: "sql",
		},
		{
			name:         "a RETURN body that names no language",
			statement:    "CREATE FUNCTION add3(a int) RETURNS int RETURN a + 3;",
			wantBody:     "RETURN a + 3",
			wantLanguage: "sql",
		},
		{
			name:         "a BEGIN ATOMIC function that names no language",
			statement:    "CREATE FUNCTION cnt() RETURNS bigint STABLE BEGIN ATOMIC SELECT count(*) FROM t; END;",
			wantBody:     "BEGIN ATOMIC SELECT count(*) FROM t; END",
			wantLanguage: "sql",
		},
		{
			name:         "a BEGIN ATOMIC procedure that names no language",
			statement:    "CREATE PROCEDURE p() BEGIN ATOMIC INSERT INTO t VALUES (1); END;",
			wantBody:     "BEGIN ATOMIC INSERT INTO t VALUES (1); END",
			wantLanguage: "sql",
		},
		{
			name:         "a quoted function body that names no language",
			statement:    "CREATE FUNCTION one() RETURNS int AS $$ BEGIN RETURN 1; END $$;",
			wantBody:     "BEGIN RETURN 1; END",
			wantLanguage: "",
		},
		{
			name:         "a quoted procedure body that names no language",
			statement:    "CREATE PROCEDURE q() AS $$ BEGIN INSERT INTO t VALUES (1); END $$;",
			wantBody:     "BEGIN INSERT INTO t VALUES (1); END",
			wantLanguage: "",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.statement), platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Functions, qt.HasLen, 1)
			c.Assert(database.Functions[0].Body, qt.Equals, test.wantBody)
			c.Assert(database.Functions[0].Language, qt.Equals, test.wantLanguage)
		})
	}
}
