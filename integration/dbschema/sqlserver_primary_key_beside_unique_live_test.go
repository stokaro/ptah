//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/sqlschema"
)

// TestSQLServerLiveRefusesPrimaryKeyBesideUniqueOnOneColumn is stokaro/ptah#2812
// asked of the server rather than of the renderer.
//
// Ptah refuses `a INT PRIMARY KEY UNIQUE` for SQL Server, and that refusal is
// only correct because the engine refuses it too. A renderer test can pin what
// Ptah does and can say nothing about whether the reason is real -- the same
// test passes if SQL Server were perfectly happy with the statement and Ptah
// had simply decided to be difficult.
//
// So the two halves are asserted against one server: the engine rejects the
// statement Ptah declines to write, and accepts the one Ptah writes instead.
// The version is whatever CI runs rather than a version named here, which is
// the point of asking rather than remembering.
func TestSQLServerLiveRefusesPrimaryKeyBesideUniqueOnOneColumn(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	table := fmt.Sprintf("ptah_pk_uq_%d", time.Now().UnixNano())
	quoted := quoteSQLServerIdentifier(table)
	defer func() {
		_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoted)
	}()

	// The engine's half. Ptah never sends this statement, which is exactly why
	// the reason it does not has to be measured somewhere.
	_, err = conn.ExecContext(ctx, "CREATE TABLE "+quoted+" (a INT PRIMARY KEY UNIQUE)")
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "8151")

	// Ptah's half, on the same declaration.
	_, err = renderSQLServer(c, `CREATE TABLE t (a INT PRIMARY KEY UNIQUE);`)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
}

// TestSQLServerLiveAcceptsTheTableLevelSpellingPtahWrites is the control, and
// it is what keeps the refusal from being a ban on the two words together.
//
// `a INT UNIQUE, PRIMARY KEY (a)` is a different statement. The assertion is
// that the source and Ptah's rendering of it produce the same indexes on this
// server -- one primary key, one unique constraint -- rather than that they
// produce any particular pair, because the pair is the engine's answer and not
// something this file should be choosing.
func TestSQLServerLiveAcceptsTheTableLevelSpellingPtahWrites(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	stamp := time.Now().UnixNano()
	source := fmt.Sprintf("ptah_src_%d", stamp)
	rendered := fmt.Sprintf("ptah_ren_%d", stamp)
	defer func() {
		_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoteSQLServerIdentifier(source))
		_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoteSQLServerIdentifier(rendered))
	}()

	_, err = conn.ExecContext(ctx,
		"CREATE TABLE "+quoteSQLServerIdentifier(source)+" (a INT UNIQUE, PRIMARY KEY (a))")
	c.Assert(err, qt.IsNil)

	statement, err := renderSQLServer(c, `CREATE TABLE t (a INT UNIQUE, PRIMARY KEY (a));`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, renameCreatedSQLServerTable(statement, rendered))
	c.Assert(err, qt.IsNil)

	sourceShape := sqlServerIndexShape(c, ctx, conn, source)
	// The source has to produce more than the key, or the comparison below
	// would hold for a rendering that emitted nothing at all.
	c.Assert(len(sourceShape) > 1, qt.IsTrue)
	c.Assert(sqlServerIndexShape(c, ctx, conn, rendered), qt.DeepEquals, sourceShape)
}

// renderSQLServer renders one CREATE TABLE for SQL Server, returning whatever
// the renderer answered so a caller can assert either outcome.
func renderSQLServer(c *qt.C, sql string) (string, error) {
	c.Helper()
	database, _, err := sqlschema.Read([]byte(sql), platform.SQLServer)
	c.Assert(err, qt.IsNil)
	ordered, err := renderer.GetOrderedCreateStatements(&database, platform.SQLServer)
	if err != nil {
		return "", err
	}
	c.Assert(ordered, qt.HasLen, 1)
	return ordered[0], nil
}

// renameCreatedSQLServerTable points one CREATE TABLE at another name, so both
// tables live in one database and one query reads them together.
func renameCreatedSQLServerTable(statement, to string) string {
	return "CREATE TABLE " + quoteSQLServerIdentifier(to) +
		statement[len("CREATE TABLE ")+len("[t]"):]
}

// sqlServerIndexShape is what the catalog says one table's indexes are.
//
// The index NAME is deliberately absent: SQL Server derives it and embeds a
// per-database hash, so two tables carrying the same declaration never share
// one. What the two have to agree on is the kinds.
func sqlServerIndexShape(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection, table string) []string {
	c.Helper()
	rows, err := conn.QueryContext(ctx, `SELECT is_primary_key, is_unique_constraint, is_unique
		FROM sys.indexes WHERE object_id = OBJECT_ID(@p1)
		ORDER BY is_primary_key DESC, is_unique_constraint DESC`, table)
	c.Assert(err, qt.IsNil)
	defer rows.Close()

	var found []string
	for rows.Next() {
		var primary, uniqueConstraint, unique bool
		c.Assert(rows.Scan(&primary, &uniqueConstraint, &unique), qt.IsNil)
		found = append(found, fmt.Sprintf("primary=%t unique_constraint=%t unique=%t",
			primary, uniqueConstraint, unique))
	}
	c.Assert(rows.Err(), qt.IsNil)
	return found
}
