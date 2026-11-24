package dbreset_test

import (
	"database/sql/driver"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbreset"
	"ptah.run/internal/dbschema/dbtest"
)

// TestReadAutocommitBeforeDDL_HappyPath reads the setting as the session
// reports it, and a server without the setting as empty. Only "on" commits
// ahead of each schema statement.
func TestReadAutocommitBeforeDDL_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		answer      driver.Value
		want        dbreset.AutocommitBeforeDDL
		wantCommits bool
	}{
		{name: "on", answer: "on", want: dbreset.AutocommitBeforeDDLOn, wantCommits: true},
		{name: "off", answer: "off", want: dbreset.AutocommitBeforeDDLOff, wantCommits: false},
		{name: "spelled in capitals", answer: "ON", want: dbreset.AutocommitBeforeDDLOn, wantCommits: true},
		{name: "no such setting", answer: "", want: "", wantCommits: false},
		{name: "NULL", answer: nil, want: "", wantCommits: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(c, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
				return dbtest.QueryResult{Columns: []string{"coalesce"}, Rows: [][]driver.Value{{test.answer}}}, nil
			})

			got, err := dbreset.ReadAutocommitBeforeDDL(c.Context(), db.SQL)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
			c.Assert(got.CommitsEachSchemaStatement(), qt.Equals, test.wantCommits)
		})
	}
}

// TestReadAutocommitBeforeDDL_FailurePath reports a session that cannot
// answer, rather than reading it as one that holds its schema statements.
func TestReadAutocommitBeforeDDL_FailurePath(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(c, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
		return dbtest.QueryResult{}, errors.New("connection reset")
	})

	got, err := dbreset.ReadAutocommitBeforeDDL(c.Context(), db.SQL)

	c.Assert(err, qt.ErrorMatches, `read the session's autocommit_before_ddl: connection reset`)
	c.Assert(got, qt.Equals, dbreset.AutocommitBeforeDDL(""))
}
