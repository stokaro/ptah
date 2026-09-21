//go:build integration

package migrator_test

import (
	"context"
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasretry"
)

// TestOracleSerializationFailureIsRetryableLive asks Oracle for the conflict
// rather than constructing it.
//
// The revision-set transaction runs at serializable isolation, so a concurrent
// set is answered with ORA-08177 rather than serialized -- Oracle takes no
// advisory lock, so nothing above the transaction orders the two runs. The
// retry loop only retries what atlasretry.IsRetryable recognizes, and the
// driver publishes the server's number as a struct field rather than through
// any of the interfaces the other engines implement, so what the driver really
// returns is the only thing worth asserting on (stokaro/ptah#3462).
func TestOracleSerializationFailureIsRetryableLive(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn := connectOracle3298(c)
	const table = "ptah_3462_conflict"
	dropOracle3298Tables(c, conn, table)
	_, err := conn.ExecContext(ctx,
		"CREATE TABLE "+table+" (id NUMBER(10) PRIMARY KEY, n NUMBER(10))")
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, "INSERT INTO "+table+" (id, n) VALUES (1, 0)")
	c.Assert(err, qt.IsNil)

	// The reader takes its snapshot first, then the writer commits over it, so
	// the reader's own update meets a row that moved under it.
	reader := beginOracleSerializable(c, ctx, conn)
	var n int
	c.Assert(reader.QueryRowContext(ctx, "SELECT n FROM "+table+" WHERE id = 1").Scan(&n), qt.IsNil)
	writer := beginOracleSerializable(c, ctx, connectOracle3298(c))
	_, err = writer.ExecContext(ctx, "UPDATE "+table+" SET n = 1 WHERE id = 1")
	c.Assert(err, qt.IsNil)
	c.Assert(writer.Commit(), qt.IsNil)

	_, conflict := reader.ExecContext(ctx, "UPDATE "+table+" SET n = 2 WHERE id = 1")

	c.Assert(reader.Rollback(), qt.IsNil)
	c.Assert(conflict, qt.ErrorMatches, `(?s).*ORA-08177.*`)
	c.Assert(atlasretry.IsRetryable(conflict), qt.IsTrue,
		qt.Commentf("the retry loop sees this error and must retry it: %v", conflict))
}

// beginOracleSerializable opens the transaction the way the migrator does:
// Oracle's driver implements one isolation level through database/sql, so the
// level is asked for in SQL as the transaction's first statement.
func beginOracleSerializable(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
) *sql.Tx {
	c.Helper()
	session, err := conn.Conn(ctx)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = session.Close() })
	tx, err := session.BeginTx(ctx, nil)
	c.Assert(err, qt.IsNil)
	_, err = tx.ExecContext(ctx, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
	c.Assert(err, qt.IsNil)
	return tx
}
