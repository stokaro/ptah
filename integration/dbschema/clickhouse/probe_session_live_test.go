//go:build integration

package clickhouse_test

import (
	"context"
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// ClickHouse's driver keeps no transaction and closes the connection its
// rollback ends, so a probe on a pinned connection runs on a session of its
// own. The probe answers, and the pinned session it left alone still runs the
// owner's next statement.
func TestWithRolledBackTransaction_RunsAProbeBesideAPinnedSessionLive(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, dbtarget.ClickHouse))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	var formatted string
	var ran bool
	var afterward int

	err = conn.WithSession(c.Context(), func(pinned *dbschema.DatabaseConnection) error {
		var probeErr error
		ran, probeErr = pinned.WithRolledBackTransaction(c.Context(), "probe beside a pinned session", func(ctx context.Context, tx *sql.Tx) error {
			return tx.QueryRowContext(ctx, `SELECT formatQuerySingleLine('SELECT v::Int64 > 0')`).Scan(&formatted)
		})
		c.Assert(probeErr, qt.IsNil)
		return pinned.QueryRowContext(c.Context(), "SELECT 1").Scan(&afterward)
	})

	c.Assert(err, qt.IsNil)
	c.Assert(ran, qt.IsTrue)
	c.Assert(formatted, qt.Equals, "SELECT CAST(v, 'Int64') > 0")
	c.Assert(afterward, qt.Equals, 1)
}
