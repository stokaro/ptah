package dbschema_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
)

// TestWithRolledBackTransaction_RollsBackWhatTheBodyCreated pins the guarantee
// the method is named for: the body's DDL runs, and nothing of it survives the
// call.
func TestWithRolledBackTransaction_RollsBackWhatTheBodyCreated(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "rollback.db")
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	ran, err := conn.WithRolledBackTransaction(ctx, "rollback test",
		func(ctx context.Context, tx *sql.Tx) error {
			_, execErr := tx.ExecContext(ctx, "CREATE TABLE probe_leftover (id INTEGER PRIMARY KEY)")
			return execErr
		})

	c.Assert(err, qt.IsNil)
	c.Assert(ran, qt.IsTrue)
	var count int
	c.Assert(conn.QueryRowContext(ctx,
		`SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'probe_leftover'`).
		Scan(&count), qt.IsNil)
	c.Assert(count, qt.Equals, 0)
}

// TestWithRolledBackTransaction_PinnedConnectionReportsFalseAndRunsNothing
// pins the documented contract for a connection already inside somebody else's
// session: ran false, a nil error, and a body that never runs. The false is
// deliberate rather than an error, because `schema apply` rehearses its plan
// on a pinned dev session and compares schemas there -- an error here would
// fail the rehearsal to protect it (see internal/dbexprprobe).
func TestWithRolledBackTransaction_PinnedConnectionReportsFalseAndRunsNothing(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "pinned.db")
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	c.Assert(conn.WithSession(ctx, func(pinned *dbschema.DatabaseConnection) error {
		bodyRan := false
		ran, err := pinned.WithRolledBackTransaction(ctx, "pinned test",
			func(ctx context.Context, tx *sql.Tx) error {
				bodyRan = true
				return nil
			})

		c.Assert(err, qt.IsNil)
		c.Assert(ran, qt.IsFalse)
		c.Assert(bodyRan, qt.IsFalse)
		return nil
	}), qt.IsNil)
}
