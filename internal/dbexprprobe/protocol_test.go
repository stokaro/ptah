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
			db := dbtest.OpenWithExec(t, routineAnswer, func(statement string, _ []driver.NamedValue) (driver.Result, error) {
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
			db := dbtest.OpenWithExec(t, routineAnswer, func(statement string, _ []driver.NamedValue) (driver.Result, error) {
				sent = append(sent, statement)
				return driver.RowsAffected(0), nil
			})

			test.probe(t.Context(), scriptedSession{db: db.SQL})

			c.Assert(marked(sent), qt.HasLen, 1)
			c.Assert(marked(sent)[0], qt.Contains, declared+"\n")
		})
	}
}
