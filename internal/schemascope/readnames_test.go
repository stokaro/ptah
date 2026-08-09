package schemascope_test

import (
	"database/sql/driver"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
	"go.5x5.cz/ptah/internal/schemascope"
)

// TestReadNames pins the one decision every comparison-side database read
// consumes (stokaro/ptah#1276).
//
// The load-bearing rows are the two PostgreSQL URL forms. A plain URL puts the
// whole realm under the read, because the document the read will be compared
// against describes the whole realm; a `search_path` URL leaves it at the one
// schema the operator pinned. Collapsing the first into the second is the
// cheaper implementation the tree had, and it is what made `schema diff` plan
// `DROP TABLE "extra"."b" CASCADE` against a database whose own description it
// had just been handed.
//
// The probe-call assertions are half the property: an explicit selection and a
// pinned URL must not query the server at all, so a fix that always probed
// would be a different behavior wearing the same answers.
func TestReadNames(t *testing.T) {
	realmProbeErr := errors.New("realm probe exploded")

	tests := []struct {
		name       string
		info       dbschematypes.DBInfo
		requested  []string
		probe      func(query string, args []driver.NamedValue) (dbtest.QueryResult, error)
		want       []string
		wantErr    string
		wantProbes int
	}{
		{
			name: "an explicit selection wins and asks the server nothing",
			info: dbschematypes.DBInfo{
				Dialect: "postgres",
				URL:     "postgres://localhost/db?sslmode=disable",
				Schema:  "public",
			},
			requested:  []string{"only_this"},
			probe:      realmProbeRows("public", "extra"),
			want:       []string{"only_this"},
			wantProbes: 0,
		},
		{
			name: "a comma-separated selection is split like the flag",
			info: dbschematypes.DBInfo{
				Dialect: "postgres",
				URL:     "postgres://localhost/db",
				Schema:  "public",
			},
			requested:  []string{"one,two"},
			probe:      realmProbeRows("public", "extra"),
			want:       []string{"one", "two"},
			wantProbes: 0,
		},
		{
			name: "a plain PostgreSQL URL covers the whole realm",
			info: dbschematypes.DBInfo{
				Dialect: "postgres",
				URL:     "postgres://localhost/db?sslmode=disable",
				Schema:  "public",
			},
			probe:      realmProbeRows("public", "extra"),
			want:       []string{"extra", "public"},
			wantProbes: 1,
		},
		{
			name: "a search_path URL covers the schema it pinned",
			info: dbschematypes.DBInfo{
				Dialect: "postgres",
				URL:     "postgres://localhost/db?search_path=public",
				Schema:  "public",
			},
			probe:      realmProbeRows("public", "extra"),
			want:       []string{"public"},
			wantProbes: 0,
		},
		{
			name: "a comma-carrying search_path pins nothing and covers the realm",
			info: dbschematypes.DBInfo{
				Dialect: "postgres",
				URL:     "postgres://localhost/db?search_path=public,extra",
				Schema:  "public",
			},
			probe:      realmProbeRows("public", "extra"),
			want:       []string{"extra", "public"},
			wantProbes: 1,
		},
		{
			name: "a MySQL connection covers the database it named",
			info: dbschematypes.DBInfo{
				Dialect: "mysql",
				URL:     "mysql://root@tcp(127.0.0.1:3306)/app",
				Schema:  "app",
			},
			probe:      realmProbeRows("app"),
			want:       []string{"app"},
			wantProbes: 0,
		},
		{
			name: "SQLite has one namespace and covers it",
			info: dbschematypes.DBInfo{
				Dialect: "sqlite",
				URL:     "sqlite://file?mode=memory",
				Schema:  "main",
			},
			probe:      realmProbeRows("main"),
			want:       []string{"main"},
			wantProbes: 0,
		},
		{
			name: "a connection reporting no schema leaves the reader its own default",
			info: dbschematypes.DBInfo{
				Dialect: "sqlite",
				URL:     "sqlite://file?mode=memory",
				Schema:  "   ",
			},
			probe:      realmProbeRows("main"),
			want:       nil,
			wantProbes: 0,
		},
		{
			name: "a failing realm probe is an error, not an empty realm",
			info: dbschematypes.DBInfo{
				Dialect: "postgres",
				URL:     "postgres://localhost/db",
				Schema:  "public",
			},
			probe: func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
				return dbtest.QueryResult{}, realmProbeErr
			},
			wantErr:    "failed to list realm schemas: realm probe exploded",
			wantProbes: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			db := dbtest.Open(t, test.probe)

			got, err := schemascope.ReadNames(t.Context(), test.info, test.requested, db.SQL)

			c.Assert(errString(err), qt.Equals, test.wantErr)
			c.Assert(got, qt.DeepEquals, test.want)
			c.Assert(db.QueryCount(), qt.Equals, test.wantProbes)
		})
	}
}

// realmProbeRows answers the realm probe with a fixed schema list.
func realmProbeRows(names ...string) func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
	return func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
		rows := make([][]driver.Value, 0, len(names))
		for _, name := range names {
			rows = append(rows, []driver.Value{name})
		}
		return dbtest.QueryResult{Columns: []string{"nspname"}, Rows: rows}, nil
	}
}

// errString renders an error for comparison, so the table can carry the wanted
// message as one field instead of a nil check plus a match in every row.
func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
