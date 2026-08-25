package migrator

import (
	"context"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/migration/migrationfile"
)

func TestMigration_Basic(t *testing.T) {
	c := qt.New(t)

	// Test creating a new migration
	migration := &Migration{
		Version:     1,
		Description: "Test migration",
		Up:          NoopMigrationFunc,
		Down:        NoopMigrationFunc,
	}

	c.Assert(migration.Version, qt.Equals, int64(1))
	c.Assert(migration.Description, qt.Equals, "Test migration")
	c.Assert(migration.Up, qt.IsNotNil)
	c.Assert(migration.Down, qt.IsNotNil)
}

func TestAtlasRevisionTypeSQLArgRemainsNumeric(t *testing.T) {
	t.Parallel()
	c := qt.New(t)

	value := (AtlasRevisionTypeBaseline | AtlasRevisionTypeApplied).sqlArg()

	c.Assert(value, qt.Equals, uint64(3))
}

func TestNoopMigrationFunc(t *testing.T) {
	c := qt.New(t)

	// Test that noop migration function doesn't error
	err := NoopMigrationFunc(context.Background(), nil)
	c.Assert(err, qt.IsNil)
}

func TestCreateMigrationFromSQL(t *testing.T) {
	c := qt.New(t)

	upSQL := "CREATE TABLE test (id SERIAL PRIMARY KEY)"
	downSQL := "DROP TABLE test"

	migration := CreateMigrationFromSQL(1, "Create test table", upSQL, downSQL)

	c.Assert(migration.Version, qt.Equals, int64(1))
	c.Assert(migration.Description, qt.Equals, "Create test table")
	c.Assert(migration.Up, qt.IsNotNil)
	c.Assert(migration.Down, qt.IsNotNil)
	c.Assert(migration.UpTxMode, qt.Equals, migrationfile.FileTxModeUnspecified)
	c.Assert(migration.DownTxMode, qt.Equals, migrationfile.FileTxModeUnspecified)

	// Test that the functions don't panic (we can't test execution without a real DB)
	c.Assert(migration.Up, qt.IsNotNil)
	c.Assert(migration.Down, qt.IsNotNil)
}

func TestCreateMigrationFromSQL_NoTransactionDirective(t *testing.T) {
	c := qt.New(t)

	migration := CreateMigrationFromSQL(1, "Add enum value",
		"-- +ptah no_transaction\nALTER TYPE mood ADD VALUE 'ok';",
		"-- manual down migration required",
	)

	c.Assert(migration.UpTxMode, qt.Equals, migrationfile.FileTxModeNone)
	c.Assert(migration.DownTxMode, qt.Equals, migrationfile.FileTxModeUnspecified)
}

func TestCreateMigrationFromSQL_DownNoTransactionDirective(t *testing.T) {
	c := qt.New(t)

	migration := CreateMigrationFromSQL(1, "Drop concurrent index",
		"SELECT 1;",
		"-- +ptah no_transaction\nDROP INDEX CONCURRENTLY users_email_idx;",
	)

	c.Assert(migration.UpTxMode, qt.Equals, migrationfile.FileTxModeUnspecified)
	c.Assert(migration.DownTxMode, qt.Equals, migrationfile.FileTxModeNone)
}

func TestCreateMigrationFromSQL_AtlasTxModeNoneDirective(t *testing.T) {
	c := qt.New(t)

	migration := CreateMigrationFromSQL(1, "Add concurrent index",
		"-- atlas:txmode none\n\nCREATE INDEX CONCURRENTLY users_email_idx ON users (email);",
		"DROP INDEX users_email_idx;",
	)

	c.Assert(migration.UpTxMode, qt.Equals, migrationfile.FileTxModeNone)
	c.Assert(migration.DownTxMode, qt.Equals, migrationfile.FileTxModeUnspecified)
}

func TestMigration_ExplicitDirectionalNoTransactionFieldsControlManualMigrations(t *testing.T) {
	c := qt.New(t)

	migration := &Migration{UpTxMode: migrationfile.FileTxModeNone, DownTxMode: migrationfile.FileTxModeNone}

	c.Assert(migration.upExecutionMode(), qt.Equals, migrationExecutionNoTransaction)
	c.Assert(migration.downExecutionMode(), qt.Equals, migrationExecutionNoTransaction)
}

func TestCreateMigrationFromSQL_InvalidNoTransactionDirective(t *testing.T) {
	c := qt.New(t)

	migration := CreateMigrationFromSQL(1, "Invalid directive",
		"-- +ptah no_transaction=maybe\nSELECT 1;",
		"",
	)

	err := migration.Up(context.Background(), nil)
	c.Assert(err, qt.ErrorMatches, `invalid up migration directives: invalid \+ptah no_transaction value "maybe": expected true or false`)
}

func TestMigrationStatus(t *testing.T) {
	c := qt.New(t)

	status := &MigrationStatus{
		CurrentVersion:    5,
		PendingMigrations: []int64{6, 7, 8},
		TotalMigrations:   8,
		HasPendingChanges: true,
	}

	c.Assert(status.CurrentVersion, qt.Equals, int64(5))
	c.Assert(status.PendingMigrations, qt.HasLen, 3)
	c.Assert(status.TotalMigrations, qt.Equals, 8)
	c.Assert(status.HasPendingChanges, qt.IsTrue)
}

func TestMigrationStatus_NoPending(t *testing.T) {
	c := qt.New(t)

	status := &MigrationStatus{
		CurrentVersion:    5,
		PendingMigrations: make([]int64, 0),
		TotalMigrations:   5,
		HasPendingChanges: false,
	}

	c.Assert(status.CurrentVersion, qt.Equals, int64(5))
	c.Assert(status.PendingMigrations, qt.HasLen, 0)
	c.Assert(status.TotalMigrations, qt.Equals, 5)
	c.Assert(status.HasPendingChanges, qt.IsFalse)
}

func TestSplitSQLStatementsForDialect_SQLServerGoBatchSeparator(t *testing.T) {
	c := qt.New(t)

	sql := `-- create first table
CREATE TABLE [user] ([id] INT PRIMARY KEY);
GO
-- create second table
CREATE TABLE [order] ([id] INT PRIMARY KEY);
GO -- trailing client separator comment
`

	result := splitSQLStatementsForDialect(sql, platform.SQLServer)

	c.Assert(result, qt.DeepEquals, []string{
		"CREATE TABLE [user] ([id] INT PRIMARY KEY)",
		"CREATE TABLE [order] ([id] INT PRIMARY KEY)",
	})
}

func TestSplitSQLStatementsForDialect_SQLServerPreservesBracketCommentMarkers(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE [owner's--/*value]]x] ([id] INT);
GO`

	result := splitSQLStatementsForDialect(sql, platform.SQLServer)

	c.Assert(result, qt.DeepEquals, []string{
		"CREATE TABLE [owner's--/*value]]x] ([id] INT)",
	})
}

func TestMigrationStatementCountForDialect_SQLServerGoBatchSeparator(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE [a] ([id] INT PRIMARY KEY);
GO
CREATE TABLE [b] ([id] INT PRIMARY KEY);
GO
`

	c.Assert(migrationStatementCountForDialect(sql, platform.SQLServer), qt.Equals, 2)
}

func TestMigrationStatementCountForDialect_SQLServerGoBatchCount(t *testing.T) {
	c := qt.New(t)

	sql := `INSERT INTO [audit_log] ([message]) VALUES ('repeat');
GO 3
`

	c.Assert(migrationStatementCountForDialect(sql, platform.SQLServer), qt.Equals, 3)
}

func TestFSMigrationProvider_PreservesTimeouts(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"0000000001_test.up.sql": &fstest.MapFile{
			Data: []byte("-- +ptah lock_timeout=3s\nCREATE TABLE test (id SERIAL PRIMARY KEY);"),
		},
		"0000000001_test.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE test;")},
	}

	provider, err := NewFSMigrationProvider(fsys)
	c.Assert(err, qt.IsNil)
	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)
	c.Assert(migrations[0].UpTimeouts.HasLockTimeout, qt.IsTrue)
	c.Assert(migrations[0].UpTimeouts.LockTimeout, qt.Equals, 3*time.Second)
}
