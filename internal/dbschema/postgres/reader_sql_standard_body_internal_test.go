package postgres

// White-box testing required: which queries the routine read issues decides
// whether it works on a server without pg_get_function_sqlbody, and the read
// that issues them, readFunctionsForSchema, is unexported. A live PostgreSQL 13
// is not in CI, so a fake server stands in for one.

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/dbtest"
)

// sqlBodyFakeServer answers the routine read with one LANGUAGE sql function
// whose prosrc is body. It answers the probe for pg_get_function_sqlbody
// with hasSQLBody, and the body read with stored. Every query it is asked is
// appended to asked.
func sqlBodyFakeServer(body string, hasSQLBody bool, stored string, asked *[]string) func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
	return func(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
		*asked = append(*asked, query)
		switch {
		case strings.Contains(query, "to_regproc('pg_catalog.pg_get_function_sqlbody')"):
			return dbtest.QueryResult{Columns: []string{"available"}, Rows: [][]driver.Value{{hasSQLBody}}}, nil
		case strings.Contains(query, "SELECT pg_get_function_sqlbody(p.oid)"):
			return dbtest.QueryResult{Columns: []string{"body"}, Rows: [][]driver.Value{{stored}}}, nil
		case strings.Contains(query, "AS function_name"):
			return dbtest.QueryResult{
				Columns: []string{"function_name", "parameters", "identity_arguments", "returns", "returns_set", "language",
					"security", "volatility", "body", "comment", "kind", "settings", "leakproof", "strict", "parallel"},
				Rows: [][]driver.Value{{"f", "", "", "bigint", false, "sql", "INVOKER", "STABLE", body, "", "function", "", false, false, "UNSAFE"}},
			}, nil
		default:
			return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
		}
	}
}

// TestReadFunctionsForSchema_ReadsASQLStandardBodyWhereTheServerPrintsOne pins
// the read of a routine whose prosrc is empty. The server is asked once
// whether it has pg_get_function_sqlbody: PostgreSQL 13 has none, an empty
// `AS ”` body leaves prosrc empty there too, and a query naming the function
// fails the whole read (42883). Without it the body stays empty; with it the
// body is the one the server prints. A quoted body is never asked about.
func TestReadFunctionsForSchema_ReadsASQLStandardBodyWhereTheServerPrintsOne(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		hasSQLBody bool
		stored     string
		want       string
		queries    int
	}{
		{name: "an empty body on a server without pg_get_function_sqlbody", hasSQLBody: false, want: "", queries: 2},
		{name: "an empty body on a server that prints SQL-standard bodies", hasSQLBody: true,
			stored: "BEGIN ATOMIC\n SELECT 1;\nEND\n", want: "BEGIN ATOMIC\n SELECT 1;\nEND", queries: 3},
		{name: "a quoted body", body: "SELECT 1", hasSQLBody: true, want: "SELECT 1", queries: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var asked []string
			db := dbtest.Open(t, sqlBodyFakeServer(test.body, test.hasSQLBody, test.stored, &asked))
			reader := NewPostgreSQLReader(db.SQL, "public")

			functions, err := reader.readFunctionsForSchema(t.Context(), "public")

			c.Assert(err, qt.IsNil)
			c.Assert(functions, qt.HasLen, 1)
			c.Assert(functions[0].Body, qt.Equals, test.want)
			c.Assert(asked, qt.HasLen, test.queries, qt.Commentf("%q", asked))
		})
	}
}
