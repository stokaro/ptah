package policyprobe_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/feature/pgpolicy"
	"ptah.run/feature/pgpolicy/policyprobe"
	"ptah.run/internal/dbschema/dbtest"
)

// scriptedSession runs the probe body in the scripted database's
// transaction, rolled back after it.
type scriptedSession struct{ db *sql.DB }

func (s scriptedSession) WithRolledBackTransaction(ctx context.Context, _ string, body func(context.Context, *sql.Tx) error) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	return true, body(ctx, tx)
}

// TestNormalizeObjects_PreparesTheStatementThatCarriesTheClauses finds the
// CREATE POLICY that carries the declared clauses prepared before it ran: a
// prepared statement is one command to a PostgreSQL-family server, where a
// statement run with no arguments through tx.ExecContext goes on the simple
// query protocol and runs every command its text holds. The scripted server
// answers the read back with no row, which fails the probe after the
// statement ran; the error is not what this test reads.
func TestNormalizeObjects_PreparesTheStatementThatCarriesTheClauses(t *testing.T) {
	c := qt.New(t)
	var policies []string
	db := dbtest.OpenWithExec(t, func(string, []driver.NamedValue) (dbtest.QueryResult, error) {
		return dbtest.QueryResult{}, nil
	}, func(statement string, _ []driver.NamedValue) (driver.Result, error) {
		policies = append(policies, statement)
		return driver.RowsAffected(0), nil
	})
	declared := &pgpolicy.DesiredPolicy{Using: new("owner_name = current_user"), WithCheck: new("owner_name <> ''")}

	_, _ = policyprobe.Service{}.NormalizeObjects(t.Context(), escapingRequest(scriptedSession{db: db.SQL}, declared))

	created := policyStatements(policies)
	c.Assert(created, qt.HasLen, 1)
	c.Assert(created[0], qt.Contains, "USING (owner_name = current_user\n)")
	c.Assert(created[0], qt.Contains, "WITH CHECK (owner_name <> ''\n)")
	c.Assert(policyStatements(db.Prepared()), qt.DeepEquals, created)
}

// policyStatements is the CREATE POLICY statements among statements.
func policyStatements(statements []string) []string {
	var policies []string
	for _, statement := range statements {
		if strings.HasPrefix(statement, "CREATE POLICY") {
			policies = append(policies, statement)
		}
	}
	return policies
}
