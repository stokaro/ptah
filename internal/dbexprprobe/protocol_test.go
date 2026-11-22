package dbexprprobe_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/dbexprprobe"
	"ptah.run/internal/dbschema/dbtest"
)

// marker is in every declaration below, so the statements that carry
// declared text are the ones that hold it.
const marker = "ptah_declared_marker"

// scriptedSession is a PostgreSQL server whose transaction is the scripted
// database's, rolled back after the body.
type scriptedSession struct{ db *sql.DB }

func (scriptedSession) Info() catalog.ServerInfo {
	return catalog.ServerInfo{Dialect: platform.Postgres}
}

func (s scriptedSession) WithRolledBackTransaction(ctx context.Context, _ string, body func(context.Context, *sql.Tx) error) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	return true, body(ctx, tx)
}

// routineAnswer answers the routine probe's read, so the probe goes on to
// drop the routine, and every other read with no row.
func routineAnswer(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	if !strings.Contains(query, "pg_get_function_arguments") {
		return dbtest.QueryResult{}, nil
	}
	return dbtest.QueryResult{
		Columns: []string{"arguments", "result", "set", "body"},
		Rows:    [][]driver.Value{{"a " + marker, "integer", false, "SELECT 1"}},
	}, nil
}

// marked is the statements that carry declared text.
func marked(statements []string) []string {
	var carrying []string
	for _, statement := range statements {
		if strings.Contains(statement, marker) {
			carrying = append(carrying, statement)
		}
	}
	return carrying
}

// TestProbes_PrepareEveryStatementThatCarriesDeclaredText runs each probe of
// the PostgreSQL family against a scripted server and finds every statement
// that carries the declaration prepared before it ran. A prepared statement
// is one command to a PostgreSQL-family server, where a statement run through
// tx.ExecContext with no arguments goes on the simple query protocol, which
// runs every command its text holds.
//
// The scripted server answers most reads with no row, so most probes end in
// an error after their statements ran; the error is not what this test reads.
func TestProbes_PrepareEveryStatementThatCarriesDeclaredText(t *testing.T) {
	columns := []dbexprprobe.CheckProbeColumn{{Name: "amount", Type: "text"}}
	for _, test := range []struct {
		name string
		// probe runs the probe on session.
		probe func(ctx context.Context, session scriptedSession)
		// statements is how many statements carrying the declaration run.
		statements int
	}{
		{name: "CHECK", statements: 1, probe: func(ctx context.Context, session scriptedSession) {
			_, _ = dbexprprobe.ResolveCheckExpressions(ctx, session, []dbexprprobe.CheckExpressionProbe{{
				Key: "k", Table: "t", Columns: columns, Expression: "amount <> '" + marker + "'"}})
		}},
		{name: "domain", statements: 1, probe: func(ctx context.Context, session scriptedSession) {
			_, _ = dbexprprobe.ProbeDomainExpressions(ctx, session, platform.Postgres, []dbexprprobe.DomainExpressionProbe{{
				Key: "k", BaseType: "text", Check: "VALUE <> '" + marker + "'"}})
		}},
		{name: "view", statements: 1, probe: func(ctx context.Context, session scriptedSession) {
			_, _ = dbexprprobe.ResolveViewBodies(ctx, session, []dbexprprobe.ViewBodyProbe{{Key: "k", Body: "SELECT '" + marker + "' AS x"}})
		}},
		{name: "generated column", statements: 1, probe: func(ctx context.Context, session scriptedSession) {
			_, _, _ = dbexprprobe.ResolveGeneratedColumns(ctx, session, []dbexprprobe.GeneratedColumnProbe{{
				Key: "k", Table: "t", Column: "g", Type: "text", Columns: columns, Expression: "amount || '" + marker + "'"}})
		}},
		{name: "index", statements: 1, probe: func(ctx context.Context, session scriptedSession) {
			_, _ = dbexprprobe.ResolveIndexExpressions(ctx, session, []dbexprprobe.IndexExpressionProbe{{
				Key: "k", Table: "t", Columns: columns, Expression: "lower(amount)", Predicate: "amount <> '" + marker + "'"}})
		}},
		{name: "trigger", statements: 1, probe: func(ctx context.Context, session scriptedSession) {
			_, _ = dbexprprobe.ResolveTriggerConditions(ctx, session, []dbexprprobe.TriggerConditionProbe{{
				Key: "k", Table: "t", Columns: columns, Timing: "AFTER", Event: "INSERT", When: "NEW.amount <> '" + marker + "'"}})
		}},
		{name: "routine and its drop", statements: 2, probe: func(ctx context.Context, session scriptedSession) {
			_, _ = dbexprprobe.ResolveRoutineArguments(ctx, session, []dbexprprobe.RoutineArgumentsProbe{{
				Key: "k", Name: "f",
				Statement: "CREATE FUNCTION pg_temp.f(a " + marker + ") RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$",
				Drop:      "DROP FUNCTION pg_temp.f(" + marker + ")"}})
		}},
		{name: "column spelling", statements: 1, probe: func(ctx context.Context, session scriptedSession) {
			statement := "CREATE TEMPORARY TABLE t (amount text DEFAULT '" + marker + "')"
			_, _, _ = dbexprprobe.ResolveColumnSpellings(ctx, session, []dbexprprobe.ColumnSpellingProbe{{
				Table: "t", Schema: "public", Declared: "t", Statement: statement,
				Columns: []dbexprprobe.ColumnSpellingColumn{{Key: "k", Name: "amount", Table: "t", Statement: statement}}}})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var sent []string
			db := dbtest.OpenWithExec(c, routineAnswer, func(statement string, _ []driver.NamedValue) (driver.Result, error) {
				sent = append(sent, statement)
				return driver.RowsAffected(0), nil
			})

			test.probe(t.Context(), scriptedSession{db: db.SQL})

			c.Assert(marked(sent), qt.HasLen, test.statements)
			c.Assert(marked(db.Prepared()), qt.DeepEquals, marked(sent))
		})
	}
}

// TestProbes_WriteALineBreakAfterTheDeclaredText runs each probe whose
// declared text ends a clause on a declaration that ends in a line comment.
// The probe statement writes a line break after the text, so the comment ends
// before the parentheses the statement closes the clause with; without it the
// comment takes them, the server refuses the probe, and the declaration is
// planned again on every run.
func TestProbes_WriteALineBreakAfterTheDeclaredText(t *testing.T) {
	columns := []dbexprprobe.CheckProbeColumn{{Name: "amount", Type: "text"}}
	const declared = "amount <> '' -- " + marker
	for _, test := range []struct {
		name string
		// probe runs the probe on session for declared.
		probe func(ctx context.Context, session scriptedSession)
	}{
		{name: "CHECK", probe: func(ctx context.Context, session scriptedSession) {
			_, _ = dbexprprobe.ResolveCheckExpressions(ctx, session, []dbexprprobe.CheckExpressionProbe{{
				Key: "k", Table: "t", Columns: columns, Expression: declared}})
		}},
		{name: "domain", probe: func(ctx context.Context, session scriptedSession) {
			_, _ = dbexprprobe.ProbeDomainExpressions(ctx, session, platform.Postgres, []dbexprprobe.DomainExpressionProbe{{
				Key: "k", BaseType: "text", Check: declared}})
		}},
		{name: "view", probe: func(ctx context.Context, session scriptedSession) {
			_, _ = dbexprprobe.ResolveViewBodies(ctx, session, []dbexprprobe.ViewBodyProbe{{Key: "k", Body: declared}})
		}},
		{name: "generated column", probe: func(ctx context.Context, session scriptedSession) {
			_, _, _ = dbexprprobe.ResolveGeneratedColumns(ctx, session, []dbexprprobe.GeneratedColumnProbe{{
				Key: "k", Table: "t", Column: "g", Type: "text", Columns: columns, Expression: declared}})
		}},
		{name: "index expression", probe: func(ctx context.Context, session scriptedSession) {
			_, _ = dbexprprobe.ResolveIndexExpressions(ctx, session, []dbexprprobe.IndexExpressionProbe{{
				Key: "k", Table: "t", Columns: columns, Expression: declared}})
		}},
		{name: "index predicate", probe: func(ctx context.Context, session scriptedSession) {
			_, _ = dbexprprobe.ResolveIndexExpressions(ctx, session, []dbexprprobe.IndexExpressionProbe{{
				Key: "k", Table: "t", Columns: columns, Expression: "lower(amount)", Predicate: declared}})
		}},
		{name: "trigger", probe: func(ctx context.Context, session scriptedSession) {
			_, _ = dbexprprobe.ResolveTriggerConditions(ctx, session, []dbexprprobe.TriggerConditionProbe{{
				Key: "k", Table: "t", Columns: columns, Timing: "AFTER", Event: "INSERT", When: declared}})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var sent []string
			db := dbtest.OpenWithExec(c, routineAnswer, func(statement string, _ []driver.NamedValue) (driver.Result, error) {
				sent = append(sent, statement)
				return driver.RowsAffected(0), nil
			})

			test.probe(t.Context(), scriptedSession{db: db.SQL})

			c.Assert(marked(sent), qt.HasLen, 1)
			c.Assert(marked(sent)[0], qt.Contains, declared+"\n")
		})
	}
}

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

// refusingServer is a scripted server that refuses each statement carrying
// the declaration with refusal, and answers the routine probe's read.
func refusingServer(c *qt.C, refusal error) scriptedSession {
	db := dbtest.OpenWithExec(c, routineAnswer, func(statement string, _ []driver.NamedValue) (driver.Result, error) {
		if strings.Contains(statement, marker) {
			return nil, refusal
		}
		return driver.RowsAffected(0), nil
	})
	return scriptedSession{db: db.SQL}
}

// severalProbes runs each probe of the PostgreSQL family whose statement the
// shared runners or the probe itself prepare, with the object named object.
var severalProbes = []struct {
	name   string
	object string
	probe  func(ctx context.Context, session scriptedSession) error
}{
	{name: "CHECK", object: "CHECK ck on t", probe: func(ctx context.Context, session scriptedSession) error {
		_, err := dbexprprobe.ResolveCheckExpressions(ctx, session, []dbexprprobe.CheckExpressionProbe{{
			Key: "k", Table: "t", Columns: []dbexprprobe.CheckProbeColumn{{Name: "amount", Type: "text"}},
			Expression: "amount <> '" + marker + "'", Object: "CHECK ck on t"}})
		return err
	}},
	{name: "domain", object: "domain public.d", probe: func(ctx context.Context, session scriptedSession) error {
		_, err := dbexprprobe.ProbeDomainExpressions(ctx, session, platform.Postgres, []dbexprprobe.DomainExpressionProbe{{
			Key: "k", BaseType: "text", Check: "VALUE <> '" + marker + "'", Object: "domain public.d"}})
		return err
	}},
	{name: "view", object: "view v", probe: func(ctx context.Context, session scriptedSession) error {
		_, err := dbexprprobe.ResolveViewBodies(ctx, session, []dbexprprobe.ViewBodyProbe{{
			Key: "k", Body: "SELECT '" + marker + "' AS x", Object: "view v"}})
		return err
	}},
	{name: "trigger", object: "trigger audit on t", probe: func(ctx context.Context, session scriptedSession) error {
		_, err := dbexprprobe.ResolveTriggerConditions(ctx, session, []dbexprprobe.TriggerConditionProbe{{
			Key: "k", Table: "t", Columns: []dbexprprobe.CheckProbeColumn{{Name: "amount", Type: "text"}},
			Timing: "AFTER", Event: "INSERT", When: "NEW.amount <> '" + marker + "'", Object: "trigger audit on t"}})
		return err
	}},
}

// TestProbes_WarnWhenTheServerReadsSeveralStatements has the server refuse
// the prepared probe statement as two commands. The declaration is compared
// as written, and the run warns that names it, as it does for text the check
// refuses before the probe runs.
func TestProbes_WarnWhenTheServerReadsSeveralStatements(t *testing.T) {
	for _, test := range severalProbes {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, stream := warnedRun()

			err := test.probe(ctx, refusingServer(c, severalCommands{}))

			c.Assert(err, qt.IsNil)
			c.Assert(stream.String(), qt.Equals, "warning: "+test.object+" is compared as written, not in the form "+
				"the server stores: the probe that spells it does not run, because the server reads more than one "+
				"statement in its text\n")
		})
	}
}

// TestProbes_RefusedForAnotherReasonRaiseNoWarning is the control: a refusal
// of the same SQLSTATE for a syntax error leaves the declaration unresolved
// without a warning or an error, since the apply reports the server's answer.
func TestProbes_RefusedForAnotherReasonRaiseNoWarning(t *testing.T) {
	for _, test := range severalProbes {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, stream := warnedRun()

			err := test.probe(ctx, refusingServer(c, syntaxError{}))

			c.Assert(err, qt.IsNil)
			c.Assert(stream.String(), qt.Equals, "")
		})
	}
}
