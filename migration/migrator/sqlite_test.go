package migrator

import (
	"context"
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
