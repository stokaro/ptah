package migrator_test

import (
	"context"
	"os"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
)

type clickHouseRevisionProgress struct {
	State   string
	Applied int
	Total   int
	Failure string
}

func TestNoTransactionCheckpoint_ClickHousePtahRevisionIsVisibleToObserver(t *testing.T) {
	c := qt.New(t)
	conn := openNoTransactionClickHouse(t)
	defer dbschema.CloseAndWarn(conn)
	const revisionsTable = "ptah_issue_152_revisions"
	dropNoTransactionClickHouseTable(c, conn, revisionsTable)
	defer dropNoTransactionClickHouseTable(c, conn, revisionsTable)

	progress := make([]clickHouseRevisionProgress, 0, 2)
	observer := migrator.StatementObserverFunc(func(ctx context.Context, _ migrator.StatementEvent) error {
		var snapshot clickHouseRevisionProgress
		err := conn.QueryRowContext(
			ctx,
			"SELECT state, applied, total, COALESCE(error, '') FROM "+revisionsTable+" WHERE version = 1",
		).Scan(&snapshot.State, &snapshot.Applied, &snapshot.Total, &snapshot.Failure)
		progress = append(progress, snapshot)
		return err
	})
	mig := newNoTransactionClickHouseMigrator(c, conn, observer).
		WithMigrationsTable("", revisionsTable)

	c.Assert(mig.MigrateUp(t.Context()), qt.IsNil)
	c.Assert(progress, qt.DeepEquals, []clickHouseRevisionProgress{
		{State: "pending", Applied: 1, Total: 2},
		{State: "pending", Applied: 2, Total: 2},
	})
}

func TestNoTransactionCheckpoint_ClickHouseAtlasRevisionIsVisibleToObserver(t *testing.T) {
	c := qt.New(t)
	conn := openNoTransactionClickHouse(t)
	defer dbschema.CloseAndWarn(conn)
	const revisionsTable = "ptah_issue_152_atlas_revisions"
	dropNoTransactionClickHouseTable(c, conn, revisionsTable)
	defer dropNoTransactionClickHouseTable(c, conn, revisionsTable)

	progress := make([]clickHouseRevisionProgress, 0, 2)
	observer := migrator.StatementObserverFunc(func(ctx context.Context, _ migrator.StatementEvent) error {
		var snapshot clickHouseRevisionProgress
		err := conn.QueryRowContext(
			ctx,
			"SELECT applied, total, COALESCE(error, '') FROM "+revisionsTable+" WHERE version = '1'",
		).Scan(&snapshot.Applied, &snapshot.Total, &snapshot.Failure)
		progress = append(progress, snapshot)
		return err
	})
	mig := newNoTransactionClickHouseMigrator(c, conn, observer).
		WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).
		WithMigrationsTable("", revisionsTable)

	c.Assert(mig.MigrateUp(t.Context()), qt.IsNil)
	c.Assert(progress, qt.DeepEquals, []clickHouseRevisionProgress{
		{Applied: 1, Total: 2},
		{Applied: 2, Total: 2},
	})
}

func TestNoTransactionRepair_ClickHouseAtlasRevisionAfterManualReconciliation(t *testing.T) {
	c := qt.New(t)
	conn := openNoTransactionClickHouse(t)
	defer dbschema.CloseAndWarn(conn)
	const revisionsTable = "ptah_issue_152_atlas_repair"
	dropNoTransactionClickHouseTable(c, conn, revisionsTable)
	defer dropNoTransactionClickHouseTable(c, conn, revisionsTable)

	mig := newNoTransactionClickHouseMigrator(c, conn, nil).
		WithRevisionTableFormat(migrator.RevisionTableFormatAtlas).
		WithMigrationsTable("", revisionsTable)
	c.Assert(mig.MigrateUp(t.Context()), qt.IsNil)
	_, err := conn.ExecContext(t.Context(), `ALTER TABLE `+revisionsTable+`
		UPDATE applied = 0, error = 'statement execution outcome is unknown after process interruption', error_stmt = 'SELECT 1'
		WHERE version = '1'
		SETTINGS mutations_sync = 1`)
	c.Assert(err, qt.IsNil)

	err = mig.RepairMigration(t.Context(), migrator.RepairMigrationOptions{Version: 1})
	c.Assert(err, qt.IsNil)
	var applied, total int
	var failure string
	c.Assert(conn.QueryRowContext(
		t.Context(),
		"SELECT applied, total, COALESCE(error, '') FROM "+revisionsTable+" WHERE version = '1'",
	).Scan(&applied, &total, &failure), qt.IsNil)
	c.Assert(applied, qt.Equals, 2)
	c.Assert(total, qt.Equals, 2)
	c.Assert(failure, qt.Equals, "")
}

func openNoTransactionClickHouse(t *testing.T) *dbschema.DatabaseConnection {
	t.Helper()
	dbURL := os.Getenv("CLICKHOUSE_URL")
	if dbURL == "" {
		t.Skip("CLICKHOUSE_URL not set")
	}
	conn, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
	qt.Assert(t, err, qt.IsNil)
	return conn
}

func newNoTransactionClickHouseMigrator(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	observer migrator.StatementObserver,
) *migrator.Migrator {
	c.Helper()
	mig, err := migrator.NewFSMigrator(
		conn,
		fstest.MapFS{
			"000001_probe.up.sql": {
				Data: []byte("-- +ptah no_transaction\nSELECT 1;\nSELECT 2;\n"),
			},
			"000001_probe.down.sql": {
				Data: []byte("-- +ptah no_transaction\nSELECT 2;\nSELECT 1;\n"),
			},
		},
		migrator.WithStatementObserver(observer),
	)
	c.Assert(err, qt.IsNil)
	return mig
}

func dropNoTransactionClickHouseTable(c *qt.C, conn *dbschema.DatabaseConnection, table string) {
	c.Helper()
	_, err := conn.ExecContext(c.Context(), "DROP TABLE IF EXISTS "+table+" SYNC")
	c.Assert(err, qt.IsNil)
}
