package pgtableprobe_test

import (
	"context"
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dialect/postgres/pgtableprobe"
	"ptah.run/internal/dbschema/dbtest"
)

// severalCommands is the error PostgreSQL answers a prepared statement that
// holds two commands with.
type severalCommands struct{}

func (severalCommands) Error() string {
	return "ERROR: cannot insert multiple commands into a prepared statement (SQLSTATE 42601)"
}

func (severalCommands) SQLState() string { return "42601" }

// syntaxError is a refusal of the same code for another reason.
type syntaxError struct{}

func (syntaxError) Error() string { return `ERROR: syntax error at or near "x" (SQLSTATE 42601)` }

func (syntaxError) SQLState() string { return "42601" }

// refusingProbeSession is a session whose server refuses every statement that
// creates a table with refusal, and answers every read with key.
func refusingProbeSession(c *qt.C, refusal error, key string) probeSession {
	db := dbtest.OpenWithExec(c, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
		return dbtest.QueryResult{Columns: []string{"answer"}, Rows: [][]driver.Value{{key}}}, nil
	}, func(statement string, _ []driver.NamedValue) (driver.Result, error) {
		if strings.HasPrefix(statement, "CREATE ") {
			return nil, refusal
		}
		return driver.RowsAffected(0), nil
	})
	return probeSession{db: db.SQL}
}

// severalWarning is the warning for object whose probe the server refused as
// two commands.
func severalWarning(object string) string {
	return "warning: " + object + " is compared as written, not in the form the server stores: " +
		"the probe that spells it does not run, because the server reads more than one statement in its text\n"
}

// tableProbes runs each table probe on session, with the warning the run
// prints when the server refuses its statement as two commands.
var tableProbes = []struct {
	name    string
	normal  func(ctx context.Context, session probeSession) error
	warning string
}{
	{name: "partition key", warning: severalWarning("partition key of App.words"), normal: func(ctx context.Context, session probeSession) error {
		_, err := (pgtableprobe.Service{}).NormalizeFacets(ctx, probeRequest(session))
		return err
	}},
	{name: "partition bound", warning: severalWarning("partition bound of public.e1"), normal: func(ctx context.Context, session probeSession) error {
		_, err := (pgtableprobe.PartitionMemberService{}).NormalizeFacets(ctx, boundRequest(session, "FOR VALUES IN (1)"))
		return err
	}},
	{name: "exclusion constraint", warning: severalWarning(`exclusion constraint "No.Overlap" on "App.One"."Rooms"`), normal: func(ctx context.Context, session probeSession) error {
		_, err := (pgtableprobe.ExclusionService{}).NormalizeObjects(ctx, exclusionProbeRequest(session))
		return err
	}},
}

// TestNormalizeFacets_WarnWhenTheServerReadsSeveralStatements has the server
// refuse each table probe's prepared statement as two commands. The
// declaration is compared as written, and the run warns that names it.
func TestNormalizeFacets_WarnWhenTheServerReadsSeveralStatements(t *testing.T) {
	for _, test := range tableProbes {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, stream := backtickRun()

			err := test.normal(ctx, refusingProbeSession(c, severalCommands{}, "LIST (a)"))

			c.Assert(err, qt.IsNil)
			c.Assert(stream.String(), qt.Equals, test.warning)
		})
	}
}

// TestNormalizeFacets_RefusedForAnotherReasonRaiseNoWarning is the control: a
// refusal of the same SQLSTATE for a syntax error leaves the declaration
// unresolved without a warning, since the apply reports the server's answer.
func TestNormalizeFacets_RefusedForAnotherReasonRaiseNoWarning(t *testing.T) {
	for _, test := range tableProbes {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, stream := backtickRun()

			err := test.normal(ctx, refusingProbeSession(c, syntaxError{}, "LIST (a)"))

			c.Assert(err, qt.IsNil)
			c.Assert(stream.String(), qt.Equals, "")
		})
	}
}
