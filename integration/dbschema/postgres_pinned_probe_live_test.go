//go:build integration

package dbschema_test

import (
	"context"
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// tableExists asks the connection's own session whether a table of the schema
// is visible to it.
func tableExists(c *qt.C, conn *dbschema.DatabaseConnection, schemaName, table string) bool {
	c.Helper()
	var exists bool
	c.Assert(conn.QueryRowContext(c.Context(),
		`SELECT to_regclass(quote_ident($1) || '.' || quote_ident($2)) IS NOT NULL`, schemaName, table).
		Scan(&exists), qt.IsNil)
	return exists
}

// A comparison runs on a pinned session -- `schema apply` on its lock session,
// `migrate diff` on its replay session -- and the expression probes need a
// transaction to roll back. On a session outside any transaction the probe
// transaction runs there and takes back only what the body did
// (stokaro/ptah#3643).
func TestWithRolledBackTransaction_LivePostgresPinnedIdleSessionRunsAndRollsBack(t *testing.T) {
	c := qt.New(t)
	conn, schemaName := newFormsSchema(c)

	c.Assert(conn.WithSession(c.Context(), func(pinned *dbschema.DatabaseConnection) error {
		bodyRan := false
		ran, err := pinned.WithRolledBackTransaction(c.Context(), "pinned probe",
			func(ctx context.Context, tx *sql.Tx) error {
				bodyRan = true
				_, execErr := tx.ExecContext(ctx, `CREATE TABLE "`+schemaName+`".probe_leftover (id integer)`)
				return execErr
			})

		c.Assert(err, qt.IsNil)
		c.Assert(ran, qt.IsTrue)
		c.Assert(bodyRan, qt.IsTrue)
		c.Assert(tableExists(c, pinned, schemaName, "probe_leftover"), qt.IsFalse)
		return nil
	}), qt.IsNil)
}

// The control: a pinned session whose owner opened a transaction with a plain
// BEGIN is left alone. The probe transaction would have to roll back the
// owner's work to take back its own, so nothing runs, and the owner's table is
// still there afterwards.
func TestWithRolledBackTransaction_LivePostgresPinnedSessionInsideATransactionRunsNothing(t *testing.T) {
	c := qt.New(t)
	conn, schemaName := newFormsSchema(c)

	c.Assert(conn.WithSession(c.Context(), func(pinned *dbschema.DatabaseConnection) error {
		_, err := pinned.ExecContext(c.Context(), "BEGIN")
		c.Assert(err, qt.IsNil)
		_, err = pinned.ExecContext(c.Context(), `CREATE TABLE "`+schemaName+`".owner_work (id integer)`)
		c.Assert(err, qt.IsNil)

		bodyRan := false
		ran, err := pinned.WithRolledBackTransaction(c.Context(), "pinned probe",
			func(context.Context, *sql.Tx) error {
				bodyRan = true
				return nil
			})

		c.Assert(err, qt.IsNil)
		c.Assert(ran, qt.IsFalse)
		c.Assert(bodyRan, qt.IsFalse)
		c.Assert(tableExists(c, pinned, schemaName, "owner_work"), qt.IsTrue)
		_, err = pinned.ExecContext(c.Context(), "ROLLBACK")
		c.Assert(err, qt.IsNil)
		return nil
	}), qt.IsNil)
}
