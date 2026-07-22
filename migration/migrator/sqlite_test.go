package migrator

import (
	"context"
	"errors"
	"net/url"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/dbschema"
)

func TestFSMigratorSQLiteAppliesMigrations(t *testing.T) {
	ctx := context.Background()
	dbURL := (&url.URL{
		Scheme: platform.SQLite,
		Path:   filepath.Join(t.TempDir(), "ptah-test.sqlite"),
	}).String()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect to SQLite: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("close SQLite connection: %v", err)
		}
	}()

	fsys := fstest.MapFS{
		"000001_create_users.up.sql": {
			Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE);"),
		},
		"000001_create_users.down.sql": {
			Data: []byte("DROP TABLE users;"),
		},
	}
	m, err := NewFSMigrator(conn, fsys)
	if err != nil {
		t.Fatalf("create filesystem migrator: %v", err)
	}
	if err := m.MigrateUp(ctx); err != nil {
		t.Fatalf("migrate up: %v", err)
	}

	var tableName string
	err = conn.QueryRow("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'users'").Scan(&tableName)
	if err != nil {
		t.Fatalf("find users table: %v", err)
	}
	if tableName != "users" {
		t.Fatalf("table name = %q, want users", tableName)
	}

	version, err := m.GetCurrentVersion(ctx)
	if err != nil {
		t.Fatalf("get current version: %v", err)
	}
	if version != 1 {
		t.Fatalf("current version = %d, want 1", version)
	}
}

func TestFSMigratorSQLiteTxModeAllRollsBackAllPendingBodies(t *testing.T) {
	ctx := context.Background()
	conn := openSQLiteMigratorTestDB(t)
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("close SQLite connection: %v", err)
		}
	}()

	m, err := NewFSMigrator(conn, fstest.MapFS{
		"000001_create_all_first.up.sql": {
			Data: []byte("CREATE TABLE tx_all_first (id INTEGER PRIMARY KEY);"),
		},
		"000001_create_all_first.down.sql": {
			Data: []byte("DROP TABLE tx_all_first;"),
		},
		"000002_fail_second.up.sql": {
			Data: []byte("INSERT INTO missing_tx_all_table (id) VALUES (1);"),
		},
		"000002_fail_second.down.sql": {
			Data: []byte("-- no-op"),
		},
	})
	if err != nil {
		t.Fatalf("create filesystem migrator: %v", err)
	}
	m = m.WithTransactionMode(MigrationTxModeAll)

	err = m.MigrateUp(ctx)
	if err == nil {
		t.Fatalf("MigrateUp unexpectedly succeeded")
	}
	if sqliteTableExists(t, conn, "tx_all_first") {
		t.Fatalf("tx-mode all left migration 1 table behind after migration 2 failed")
	}

	status, err := m.GetMigrationStatus(ctx)
	if err != nil {
		t.Fatalf("get migration status: %v", err)
	}
	if status.CurrentVersion != 0 {
		t.Fatalf("current version = %d, want 0", status.CurrentVersion)
	}
	if len(status.AppliedMigrations) != 0 {
		t.Fatalf("applied migrations = %v, want none after tx-mode all rollback", status.AppliedMigrations)
	}
	if status.DirtyRevision == nil {
		t.Fatalf("dirty revision is nil")
	}
	if status.DirtyRevision.Version != 2 {
		t.Fatalf("dirty revision version = %d, want 2", status.DirtyRevision.Version)
	}
	if status.DirtyRevision.Applied != 0 || status.DirtyRevision.Total != 1 {
		t.Fatalf("dirty progress = %d/%d, want 0/1", status.DirtyRevision.Applied, status.DirtyRevision.Total)
	}
}

func TestFSMigratorSQLiteTxModeNoneRecordsPartialProgress(t *testing.T) {
	ctx := context.Background()
	conn := openSQLiteMigratorTestDB(t)
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("close SQLite connection: %v", err)
		}
	}()

	m, err := NewFSMigrator(conn, fstest.MapFS{
		"000001_partial_none.up.sql": {
			Data: []byte(`CREATE TABLE tx_none_partial (id INTEGER PRIMARY KEY);
INSERT INTO missing_tx_none_table (id) VALUES (1);`),
		},
		"000001_partial_none.down.sql": {
			Data: []byte("DROP TABLE tx_none_partial;"),
		},
	})
	if err != nil {
		t.Fatalf("create filesystem migrator: %v", err)
	}
	m = m.WithTransactionMode(MigrationTxModeNone)

	err = m.MigrateUp(ctx)
	if err == nil {
		t.Fatalf("MigrateUp unexpectedly succeeded")
	}
	if !sqliteTableExists(t, conn, "tx_none_partial") {
		t.Fatalf("tx-mode none rolled back the statement that ran before the failure")
	}

	status, err := m.GetMigrationStatus(ctx)
	if err != nil {
		t.Fatalf("get migration status: %v", err)
	}
	if status.DirtyRevision == nil {
		t.Fatalf("dirty revision is nil")
	}
	if status.DirtyRevision.Version != 1 {
		t.Fatalf("dirty revision version = %d, want 1", status.DirtyRevision.Version)
	}
	if status.DirtyRevision.Applied != 1 || status.DirtyRevision.Total != 2 {
		t.Fatalf("dirty progress = %d/%d, want 1/2", status.DirtyRevision.Applied, status.DirtyRevision.Total)
	}
}

func TestFSMigratorSQLiteTxModeAllRejectsNoTransactionBeforeWrites(t *testing.T) {
	ctx := context.Background()
	conn := openSQLiteMigratorTestDB(t)
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("close SQLite connection: %v", err)
		}
	}()

	m, err := NewFSMigrator(conn, fstest.MapFS{
		"000001_no_tx.up.sql": {
			Data: []byte("-- +ptah no_transaction\nCREATE TABLE tx_all_rejected (id INTEGER PRIMARY KEY);"),
		},
		"000001_no_tx.down.sql": {
			Data: []byte("DROP TABLE tx_all_rejected;"),
		},
	})
	if err != nil {
		t.Fatalf("create filesystem migrator: %v", err)
	}
	m = m.WithTransactionMode(MigrationTxModeAll)

	err = m.MigrateUp(ctx)
	if err == nil {
		t.Fatalf("MigrateUp unexpectedly succeeded")
	}
	if err.Error() != "migration 1 is marked no_transaction and cannot run with tx-mode all" {
		t.Fatalf("MigrateUp error = %v", err)
	}
	if sqliteTableExists(t, conn, "tx_all_rejected") {
		t.Fatalf("tx-mode all validation left schema changes behind")
	}
	status, statusErr := m.GetMigrationStatus(ctx)
	if statusErr != nil {
		t.Fatalf("get migration status: %v", statusErr)
	}
	if status.DirtyRevision != nil {
		t.Fatalf("dirty revision = %#v, want nil", status.DirtyRevision)
	}
}

func TestFSMigratorSQLiteTxModeNoneRejectsTimeoutsBeforeWrites(t *testing.T) {
	ctx := context.Background()
	conn := openSQLiteMigratorTestDB(t)
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("close SQLite connection: %v", err)
		}
	}()

	m, err := NewFSMigrator(conn, fstest.MapFS{
		"000001_timeout.up.sql": {
			Data: []byte("-- +ptah statement_timeout=1s\nCREATE TABLE tx_none_timeout (id INTEGER PRIMARY KEY);"),
		},
		"000001_timeout.down.sql": {
			Data: []byte("DROP TABLE tx_none_timeout;"),
		},
	})
	if err != nil {
		t.Fatalf("create filesystem migrator: %v", err)
	}
	m = m.WithTransactionMode(MigrationTxModeNone)

	err = m.MigrateUp(ctx)
	if err == nil {
		t.Fatalf("MigrateUp unexpectedly succeeded")
	}
	if err.Error() != "migration 1 has timeouts and cannot run with tx-mode none" {
		t.Fatalf("MigrateUp error = %v", err)
	}
	if sqliteTableExists(t, conn, "tx_none_timeout") {
		t.Fatalf("tx-mode none timeout validation left schema changes behind")
	}
	status, statusErr := m.GetMigrationStatus(ctx)
	if statusErr != nil {
		t.Fatalf("get migration status: %v", statusErr)
	}
	if status.DirtyRevision != nil {
		t.Fatalf("dirty revision = %#v, want nil", status.DirtyRevision)
	}
}

func TestFSMigratorSQLiteTxModeValidationRunsBeforePreflight(t *testing.T) {
	ctx := context.Background()
	conn := openSQLiteMigratorTestDB(t)
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("close SQLite connection: %v", err)
		}
	}()

	m, err := NewFSMigrator(conn, fstest.MapFS{
		"000001_timeout.up.sql": {
			Data: []byte("-- +ptah statement_timeout=1s\nCREATE TABLE tx_preflight_timeout (id INTEGER PRIMARY KEY);"),
		},
		"000001_timeout.down.sql": {
			Data: []byte("DROP TABLE tx_preflight_timeout;"),
		},
	})
	if err != nil {
		t.Fatalf("create filesystem migrator: %v", err)
	}
	m = m.WithTransactionMode(MigrationTxModeNone)

	called := false
	err = m.MigrateUpWithPreflight(ctx, func(context.Context, MigrationPlan) error {
		called = true
		return nil
	})
	if err == nil {
		t.Fatalf("MigrateUpWithPreflight unexpectedly succeeded")
	}
	if called {
		t.Fatalf("preflight hook ran before transaction-mode validation")
	}
}

func TestMigrateUpWithPreflightAbortPreventsApply(t *testing.T) {
	ctx := context.Background()
	conn := openSQLiteMigratorTestDB(t)
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("close SQLite connection: %v", err)
		}
	}()

	m, err := NewFSMigrator(conn, fstest.MapFS{
		"000001_create_guarded.up.sql": {
			Data: []byte("CREATE TABLE guarded_preflight (id INTEGER PRIMARY KEY);"),
		},
		"000001_create_guarded.down.sql": {
			Data: []byte("DROP TABLE guarded_preflight;"),
		},
	})
	if err != nil {
		t.Fatalf("create filesystem migrator: %v", err)
	}
	hookErr := errors.New("preflight refused")
	err = m.MigrateUpWithPreflight(ctx, func(ctx context.Context, plan MigrationPlan) error {
		if plan.Direction != MigrationDirectionUp {
			t.Fatalf("direction = %q, want up", plan.Direction)
		}
		if plan.CurrentVersion != 0 || plan.TargetVersion != 1 {
			t.Fatalf("plan versions = %d -> %d, want 0 -> 1", plan.CurrentVersion, plan.TargetVersion)
		}
		if len(plan.Versions) != 1 || plan.Versions[0] != 1 {
			t.Fatalf("plan versions = %v, want [1]", plan.Versions)
		}
		var count int
		if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'guarded_preflight'").Scan(&count); err != nil {
			t.Fatalf("query guarded table before hook abort: %v", err)
		}
		if count != 0 {
			t.Fatalf("guarded table exists before hook abort")
		}
		return hookErr
	})
	if !errors.Is(err, hookErr) {
		t.Fatalf("MigrateUpWithPreflight error = %v, want %v", err, hookErr)
	}

	var count int
	if err := conn.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'guarded_preflight'").Scan(&count); err != nil {
		t.Fatalf("query guarded table after hook abort: %v", err)
	}
	if count != 0 {
		t.Fatalf("guarded table exists after hook abort")
	}
}

func TestMigrateDownToWithPreflightAbortPreventsRollback(t *testing.T) {
	ctx := context.Background()
	conn := openSQLiteMigratorTestDB(t)
	defer func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("close SQLite connection: %v", err)
		}
	}()

	m, err := NewFSMigrator(conn, fstest.MapFS{
		"000001_create_guarded.up.sql": {
			Data: []byte("CREATE TABLE guarded_rollback_preflight (id INTEGER PRIMARY KEY);"),
		},
		"000001_create_guarded.down.sql": {
			Data: []byte("DROP TABLE guarded_rollback_preflight;"),
		},
	})
	if err != nil {
		t.Fatalf("create filesystem migrator: %v", err)
	}
	if err := m.MigrateUp(ctx); err != nil {
		t.Fatalf("migrate up: %v", err)
	}

	hookErr := errors.New("rollback preflight refused")
	err = m.MigrateDownToWithPreflight(ctx, 0, func(_ context.Context, plan MigrationPlan) error {
		if plan.Direction != MigrationDirectionDown {
			t.Fatalf("direction = %q, want down", plan.Direction)
		}
		if plan.CurrentVersion != 1 || plan.TargetVersion != 0 {
			t.Fatalf("plan versions = %d -> %d, want 1 -> 0", plan.CurrentVersion, plan.TargetVersion)
		}
		if len(plan.Versions) != 1 || plan.Versions[0] != 1 {
			t.Fatalf("plan versions = %v, want [1]", plan.Versions)
		}
		return hookErr
	})
	if !errors.Is(err, hookErr) {
		t.Fatalf("MigrateDownToWithPreflight error = %v, want %v", err, hookErr)
	}

	version, err := m.GetCurrentVersion(ctx)
	if err != nil {
		t.Fatalf("get current version: %v", err)
	}
	if version != 1 {
		t.Fatalf("current version after hook abort = %d, want 1", version)
	}
	var count int
	if err := conn.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'guarded_rollback_preflight'").Scan(&count); err != nil {
		t.Fatalf("query guarded table after rollback hook abort: %v", err)
	}
	if count != 1 {
		t.Fatalf("guarded table count = %d, want 1", count)
	}
}

func sqliteTableExists(t *testing.T, conn *dbschema.DatabaseConnection, name string) bool {
	t.Helper()
	var count int
	if err := conn.QueryRowContext(
		context.Background(),
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
		name,
	).Scan(&count); err != nil {
		t.Fatalf("query SQLite table %q: %v", name, err)
	}
	return count > 0
}

func openSQLiteMigratorTestDB(t *testing.T) *dbschema.DatabaseConnection {
	t.Helper()
	dbURL := (&url.URL{
		Scheme: platform.SQLite,
		Path:   filepath.Join(t.TempDir(), "ptah-test.sqlite"),
	}).String()
	conn, err := dbschema.ConnectToDatabase(context.Background(), dbURL)
	if err != nil {
		t.Fatalf("connect to SQLite: %v", err)
	}
	return conn
}
