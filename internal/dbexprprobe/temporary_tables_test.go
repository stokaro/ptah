package dbexprprobe_test

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/internal/dbexprprobe"
	"ptah.run/internal/dbschema/dbtest"
)

// dialectSession is scriptedSession on a server of dialect.
type dialectSession struct {
	scriptedSession
	dialect string
}

func (s dialectSession) Info() catalog.ServerInfo {
	return catalog.ServerInfo{Dialect: s.dialect}
}

// storedCheck is the spelling the scripted server stores the probed CHECK in.
const storedCheck = "(amount <> ''::text)"

// checkAnswer answers the CHECK probe's read with storedCheck.
func checkAnswer(string, []driver.NamedValue) (dbtest.QueryResult, error) {
	return dbtest.QueryResult{Columns: []string{"expression"}, Rows: [][]driver.Value{{storedCheck}}}, nil
}

// searchPathWithTempLast is the statement a probe named after its table runs
// first, so pg_temp is searched last.
const searchPathWithTempLast = "SELECT set_config('search_path', CASE\n" +
	"\tWHEN current_setting('search_path') = '' THEN 'pg_temp'\n" +
	"\tELSE current_setting('search_path') || ', pg_temp' END, true)"

// TestResolveCheckExpressions_TurnsOnTemporaryTablesWhereTheServerNeedsThem
// runs the CHECK probe against a scripted server of each dialect, for a probe
// table with a number and for one named after its table, and reads the
// statements it sent. CockroachDB refuses CREATE TEMPORARY TABLE until
// experimental_enable_temp_tables is on, so the probe turns it on for its own
// transaction first, before it moves pg_temp to the end of the search path;
// without that statement no CHECK probe answers there. PostgreSQL needs no
// setting and is sent none.
func TestResolveCheckExpressions_TurnsOnTemporaryTablesWhereTheServerNeedsThem(t *testing.T) {
	const (
		columns    = "(\"amount\" text, CONSTRAINT ptah_check_probe_ck CHECK (amount <> ''\n))"
		numbered   = "CREATE TEMPORARY TABLE ptah_check_probe_0 " + columns
		named      = "CREATE TEMPORARY TABLE pg_temp.\"t\" " + columns
		savepoint  = "SAVEPOINT ptah_check_probe"
		release    = "ROLLBACK TO SAVEPOINT ptah_check_probe"
		turnTempOn = "SELECT set_config('experimental_enable_temp_tables', 'on', true)"
	)
	for _, test := range []struct {
		name    string
		dialect string
		table   string
		want    []string
	}{
		{
			name:    "CockroachDB numbered table",
			dialect: platform.CockroachDB,
			want:    []string{savepoint, turnTempOn, numbered, release},
		},
		{
			name:    "CockroachDB table named after its table",
			dialect: platform.CockroachDB,
			table:   "t",
			want:    []string{savepoint, turnTempOn, searchPathWithTempLast, named, release},
		},
		{
			name:    "PostgreSQL numbered table",
			dialect: platform.Postgres,
			want:    []string{savepoint, numbered, release},
		},
		{
			name:    "PostgreSQL table named after its table",
			dialect: platform.Postgres,
			table:   "t",
			want:    []string{savepoint, searchPathWithTempLast, named, release},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var sent []string
			db := dbtest.OpenWithExec(c, checkAnswer, func(statement string, _ []driver.NamedValue) (driver.Result, error) {
				sent = append(sent, statement)
				return driver.RowsAffected(0), nil
			})
			session := dialectSession{scriptedSession: scriptedSession{db: db.SQL}, dialect: test.dialect}

			got, err := dbexprprobe.ResolveCheckExpressions(t.Context(), session, []dbexprprobe.CheckExpressionProbe{{
				Key: "k", Table: test.table, Columns: []dbexprprobe.CheckProbeColumn{{Name: "amount", Type: "text"}},
				Expression: "amount <> ''",
			}})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, map[string]config.CheckExpression{
				"k": {Expression: storedCheck, Resolved: true},
			})
			c.Assert(sent, qt.DeepEquals, test.want)
		})
	}
}

// sqlStateError is a server's refusal carrying a SQLSTATE, as the pgx driver
// reports one.
type sqlStateError struct{ message, state string }

func (e sqlStateError) Error() string    { return e.message + " (SQLSTATE " + e.state + ")" }
func (e sqlStateError) SQLState() string { return e.state }

// refusingProbeTable is a scripted server that refuses every CREATE TEMPORARY
// TABLE with refusal and runs every other statement.
func refusingProbeTable(refusal error) dbtest.ExecHandler {
	return func(statement string, _ []driver.NamedValue) (driver.Result, error) {
		if strings.HasPrefix(statement, "CREATE TEMPORARY TABLE") {
			return nil, refusal
		}
		return driver.RowsAffected(0), nil
	}
}

// TestResolveCheckExpressions_WarnsWhereTheRoleLacksAPrivilegeTheProbeNeeds
// runs the CHECK probe against a scripted server that refuses its temporary
// table. A refusal for lack of privilege, SQLSTATE 42501, is what PostgreSQL
// 19 and CockroachDB v26.4.1 answer a role without TEMPORARY on the database
// with, and what PostgreSQL 19 answers a role without USAGE on a schema the
// CHECK names: the CHECK is left unresolved and the run warns that it is
// compared as written, since nothing else says why it is planned again on
// every run. The warning names no privilege of its own; the server's answer
// after it does. Any other refusal, such as a syntax error, leaves the CHECK
// unresolved without one.
func TestResolveCheckExpressions_WarnsWhereTheRoleLacksAPrivilegeTheProbeNeeds(t *testing.T) {
	const notPermitted = "warning: CHECK ck_amount on t is compared as written, not in the form the server stores: " +
		"the probe that spells it does not run, because the role lacks a privilege the probe statement needs: "
	for _, test := range []struct {
		name    string
		dialect string
		refusal sqlStateError
		warning string
	}{
		{
			name:    "CockroachDB without TEMPORARY",
			dialect: platform.CockroachDB,
			refusal: sqlStateError{message: "user u does not have TEMPORARY privilege on database d", state: "42501"},
			warning: notPermitted + "user u does not have TEMPORARY privilege on database d (SQLSTATE 42501)\n",
		},
		{
			name:    "PostgreSQL without TEMPORARY",
			dialect: platform.Postgres,
			refusal: sqlStateError{message: `permission denied to create temporary tables in database "d"`, state: "42501"},
			warning: notPermitted + `permission denied to create temporary tables in database "d" (SQLSTATE 42501)` + "\n",
		},
		{
			name:    "PostgreSQL without USAGE on a schema the CHECK names",
			dialect: platform.Postgres,
			refusal: sqlStateError{message: "permission denied for schema private", state: "42501"},
			warning: notPermitted + "permission denied for schema private (SQLSTATE 42501)\n",
		},
		{
			name:    "PostgreSQL syntax error",
			dialect: platform.Postgres,
			refusal: sqlStateError{message: `syntax error at or near ")"`, state: "42601"},
			warning: "",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, stream := warnedRun()
			db := dbtest.OpenWithExec(c, checkAnswer, refusingProbeTable(test.refusal))
			session := dialectSession{scriptedSession: scriptedSession{db: db.SQL}, dialect: test.dialect}

			got, err := dbexprprobe.ResolveCheckExpressions(ctx, session, []dbexprprobe.CheckExpressionProbe{{
				Key: "k", Table: "t", Columns: []dbexprprobe.CheckProbeColumn{{Name: "amount", Type: "text"}},
				Expression: "amount <> ''", Object: "CHECK ck_amount on t",
			}})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, map[string]config.CheckExpression{"k": {}})
			c.Assert(stream.String(), qt.Equals, test.warning)
		})
	}
}
