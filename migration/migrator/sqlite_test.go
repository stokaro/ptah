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
