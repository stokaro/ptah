//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// A retry refuses a row whose first statement was interrupted, and the row it
// refuses is the one a real interruption produces.
//
// The unit test for this fabricates the row with an UPDATE, which measures the
// rule and not the shape. What records applied=0 beside the unknown-outcome
// marker is the server, the driver and the cancellation between them: the
// statement was in flight, so no checkpoint was written, and whether it
// committed is what nothing knows. Only an engine can be asked for that row.
func TestMigrateUpAllowDirtyRefusesAnInterruptedFirstStatementLive(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, postgresTestURL(t))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	suffix := time.Now().UnixNano()
	migrationsTable := fmt.Sprintf("schema_migrations_first_stmt_%d", suffix)
	itemsTable := fmt.Sprintf("ptah_first_stmt_items_%d", suffix)
	defer func() {
		_, _ = conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+itemsTable)
		_, _ = conn.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+migrationsTable)
	}()

	// The table exists before the run, so the migration's one statement is an
	// INSERT that sleeps. Interrupting it leaves the row this test is about,
	// and makes the harm concrete: a replay would insert the same id again.
	_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id BIGINT PRIMARY KEY)", itemsTable))
	c.Assert(err, qt.IsNil)

	body := fstest.MapFS{
		"000001_slow.up.sql": {Data: fmt.Appendf(nil,
			"-- +ptah no_transaction\nINSERT INTO %s (id) SELECT 1 FROM pg_sleep(10)",
			itemsTable,
		)},
		"000001_slow.down.sql": {Data: fmt.Appendf(nil, "DELETE FROM %s WHERE id = 1", itemsTable)},
	}
	provider, err := migrator.NewFSMigrationProvider(body)
	c.Assert(err, qt.IsNil)
	m := migrator.NewMigrator(conn, provider).WithMigrationsTable("", migrationsTable)
	c.Assert(m.Initialize(ctx), qt.IsNil)

	execCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	c.Assert(m.MigrateUp(execCtx), qt.IsNotNil)

	var applied int
	var failure string
	c.Assert(
		conn.QueryRowContext(ctx,
			"SELECT applied, error FROM "+migrationsTable+" WHERE version = 1").Scan(&applied, &failure),
		qt.IsNil,
	)
	c.Assert(applied, qt.Equals, 0)
	c.Assert(failure, qt.Equals, "statement execution outcome is unknown after process interruption")

	err = migrator.NewMigrator(conn, provider).
		WithMigrationsTable("", migrationsTable).
		MigrateUpWithOptions(ctx, migrator.MigrateUpOptions{AllowDirty: true})

	c.Assert(err, qt.ErrorMatches,
		`(?s).*migration 1 cannot resume automatically: the outcome of statement 1 is unknown.*`)

	// The refusal has to mean the statement was not sent again. Reading the
	// table back says so in the terms the operator cares about: no second row.
	var rows int
	c.Assert(conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+itemsTable).Scan(&rows), qt.IsNil)
	c.Assert(rows, qt.Equals, 0)
}
