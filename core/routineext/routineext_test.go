package routineext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/routineext"
)

// request asks for two bodies.
func request() routineext.ReadRequest {
	return routineext.ReadRequest{Target: "postgres", Bodies: []routineext.Body{{SQL: "a"}, {SQL: "b"}}}
}

// A reading may carry every kind, contents on a control statement, and an
// opaque control statement without contents.
func TestValidateRead_HappyPath(t *testing.T) {
	c := qt.New(t)
	result := routineext.ReadResult{Bodies: []routineext.Reading{
		{Statements: []routineext.Statement{
			{Kind: routineext.StatementSQL, SQL: "UPDATE t SET a = 1"},
			{Kind: routineext.StatementInert, Label: "return", SQL: "RETURN"},
			{Kind: routineext.StatementDynamic, Label: "execute", SQL: "EXECUTE 'x'"},
			{Kind: routineext.StatementControl, Label: "if", SQL: "IF a THEN DELETE FROM t; END IF",
				Statements: []routineext.Statement{{Kind: routineext.StatementSQL, SQL: "DELETE FROM t"}}},
		}, ColumnReads: true},
		{Statements: []routineext.Statement{{Kind: routineext.StatementControl, Label: "loop", SQL: "LOOP END LOOP", Opaque: true}}},
	}}

	c.Assert(routineext.ValidateRead(request(), result), qt.IsNil)
}

func TestValidateRead_FailurePath(t *testing.T) {
	sql := routineext.Statement{Kind: routineext.StatementSQL, SQL: "SELECT 1"}
	for _, test := range []struct {
		name    string
		bodies  []routineext.Reading
		wantErr string
	}{
		{name: "a reading missing", bodies: []routineext.Reading{{}}, wantErr: `.*a reading of 2 bodies returned 1`},
		{name: "an unknown kind", bodies: []routineext.Reading{{}, {Statements: []routineext.Statement{{Kind: "loud"}}}},
			wantErr: `body 1: .*statement 0 has unknown kind "loud"`},
		{name: "contents on a statement that steers none", bodies: []routineext.Reading{{Statements: []routineext.Statement{
			{Kind: routineext.StatementSQL, Statements: []routineext.Statement{sql}}}}, {}},
			wantErr: `body 0: .*statement 0 of kind "sql" carries contents`},
		{name: "an opaque statement that steers none", bodies: []routineext.Reading{{Statements: []routineext.Statement{
			{Kind: routineext.StatementDynamic, Opaque: true}}}, {}},
			wantErr: `body 0: .*statement 0 of kind "dynamic" carries contents`},
		{name: "an opaque statement with statements", bodies: []routineext.Reading{{Statements: []routineext.Statement{
			{Kind: routineext.StatementControl, Opaque: true, Statements: []routineext.Statement{sql}}}}, {}},
			wantErr: `body 0: .*statement 0 is opaque and carries statements`},
		{name: "a fault in a nested statement", bodies: []routineext.Reading{{}, {Statements: []routineext.Statement{
			{Kind: routineext.StatementControl, Statements: []routineext.Statement{sql, {Kind: "loud"}}}}}},
			wantErr: `body 1: statement 0: .*statement 1 has unknown kind "loud"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := routineext.ValidateRead(request(), routineext.ReadResult{Bodies: test.bodies})
			c.Assert(err, qt.ErrorIs, routineext.ErrInvalid)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
