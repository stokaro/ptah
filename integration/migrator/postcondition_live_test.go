//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// A postcondition is evaluated after the body it belongs to has committed,
// which is a claim only a server can settle: a unit test can say the assertion
// ran, not that the transaction around the body had already ended when it did
// (stokaro/ptah#3405).
//
// The fixture backfills `id > 0`, so a row with id 0 keeps its null tier and
// the postcondition does not hold. What the server is asked afterwards is
// whether the body's work is still there and whether the revision says applied.
func TestMigrationPostcondition_PostgresKeepsTheAppliedBody(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, postgresTestURL(t))
	c.Assert(err, qt.IsNil)
	// Closed through a cleanup rather than a defer, and registered first so it
	// runs last: cleanups run in reverse, and a connection the defer closed is
	// one the drop below cannot reach.
	t.Cleanup(func() { _ = conn.Close() })

	table := "ptah_postcondition_accounts"
	migrationsTable := "ptah_postcondition_revisions"
	dropPostconditionFixture(c, ctx, conn, table, migrationsTable)
	t.Cleanup(func() { dropPostconditionFixture(c, context.Background(), conn, table, migrationsTable) })
	_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id BIGINT PRIMARY KEY)", table))
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id) VALUES (0)", table))
	c.Assert(err, qt.IsNil)

	up := fmt.Sprintf(
		`-- +ptah check name="every_row_has_a_tier" phase=after `+
			`assert="SELECT count(*) = 0 FROM %s WHERE tier IS NULL"`+"\n"+
			"ALTER TABLE %s ADD COLUMN tier TEXT;\n"+
			"UPDATE %s SET tier = 'free' WHERE id > 0;\n",
		table, table, table,
	)
	mig := migrator.NewMigrator(
		conn,
		migrator.NewRegisteredMigrationProvider(
			migrator.CreateMigrationFromSQL(1, "backfill_tier", up,
				fmt.Sprintf("ALTER TABLE %s DROP COLUMN tier;\n", table)),
		),
	).WithMigrationsTable("", migrationsTable).WithMigrationLockTimeout(10 * time.Second)
	c.Assert(mig.Initialize(ctx), qt.IsNil)

	err = mig.MigrateUp(ctx)

	var postErr *migrator.PostMigrationCheckFailedError
	c.Assert(err, qt.ErrorAs, &postErr, qt.Commentf("want PostMigrationCheckFailedError, got %v", err))
	// The server still has the column the body added and the rows it wrote, so
	// the assertion was asked about committed state rather than about a
	// transaction that was about to be rolled back.
	c.Assert(postconditionRowsWithoutATier(c, ctx, conn, table), qt.Equals, 1)
	c.Assert(postconditionTierColumns(c, ctx, conn, table), qt.Equals, 1)
	status, err := mig.GetMigrationStatus(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(status.CurrentVersion, qt.Equals, int64(1))
	c.Assert(status.DirtyRevision, qt.IsNil)
}

func dropPostconditionFixture(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	table, migrationsTable string,
) {
	c.Helper()
	for _, name := range []string{table, migrationsTable} {
		_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", name))
		c.Check(err, qt.IsNil)
	}
}

func postconditionRowsWithoutATier(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	table string,
) int {
	c.Helper()
	var count int
	c.Assert(conn.QueryRowContext(
		ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE tier IS NULL", table),
	).Scan(&count), qt.IsNil)
	return count
}

func postconditionTierColumns(
	c *qt.C,
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	table string,
) int {
	c.Helper()
	var count int
	c.Assert(conn.QueryRowContext(
		ctx,
		"SELECT count(*) FROM information_schema.columns WHERE table_name = $1 AND column_name = 'tier'",
		table,
	).Scan(&count), qt.IsNil)
	return count
}
