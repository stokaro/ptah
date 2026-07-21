package testkit

import (
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"
)

func TestSQLiteApplyMigrationsSeedAndSnapshot(t *testing.T) {
	db := StartSQLite(t)

	migrations := fstest.MapFS{
		"000001_create_users.up.sql": {
			Data: []byte(`
CREATE TABLE users (
	id INTEGER PRIMARY KEY,
	email TEXT NOT NULL UNIQUE,
	name TEXT
);
`),
		},
		"000001_create_users.down.sql": {
			Data: []byte("DROP TABLE users;"),
		},
	}

	ApplyMigrationsFromFS(t, db, migrations)
	Seed(t, db, []byte("INSERT INTO users (email, name) VALUES ('a@example.com', 'A');"))

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count); err != nil {
		t.Fatalf("count seeded users: %v", err)
	}
	if count != 1 {
		t.Fatalf("seeded users count = %d, want 1", count)
	}

	snapshot := Snapshot(t, db)
	for _, want := range []string{`"name": "users"`, `"name": "email"`, `"is_unique": true`} {
		if !strings.Contains(snapshot, want) {
			t.Fatalf("snapshot does not contain %q:\n%s", want, snapshot)
		}
	}
	if strings.Contains(snapshot, "schema_migrations") {
		t.Fatalf("snapshot contains migration metadata table:\n%s", snapshot)
	}
}

func TestApplyMigrationsFromSubFS(t *testing.T) {
	db := StartSQLite(t)
	root := fstest.MapFS{
		"migrations/000001_create_accounts.up.sql": {
			Data: []byte("CREATE TABLE accounts (id INTEGER PRIMARY KEY);"),
		},
		"migrations/000001_create_accounts.down.sql": {
			Data: []byte("DROP TABLE accounts;"),
		},
	}

	migrations, err := fs.Sub(root, "migrations")
	if err != nil {
		t.Fatalf("create migrations sub-fs: %v", err)
	}

	ApplyMigrationsFromFS(t, db, migrations)

	var name string
	err = db.QueryRow("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'accounts'").Scan(&name)
	if err != nil {
		t.Fatalf("find accounts table: %v", err)
	}
	if name != "accounts" {
		t.Fatalf("table name = %q, want accounts", name)
	}
}

func TestSeedSkipsCommentsAndEmptyStatements(t *testing.T) {
	db := StartSQLite(t)
	Seed(t, db, []byte(`
-- leading comment
CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT);

/* block comment */
INSERT INTO items (label) VALUES ('one');
`))

	var label string
	if err := db.QueryRow("SELECT label FROM items WHERE id = 1").Scan(&label); err != nil {
		t.Fatalf("read seeded item: %v", err)
	}
	if label != "one" {
		t.Fatalf("label = %q, want one", label)
	}
}

func TestPostgresDatabaseURLPreservesCredentialsAndQuery(t *testing.T) {
	got := postgresDatabaseURL("postgres://actual@127.0.0.1:5432/base?sslmode=disable", "ptah_case")
	want := "postgres://actual@127.0.0.1:5432/ptah_case?sslmode=disable"
	if got != want {
		t.Fatalf("URL = %q, want %q", got, want)
	}
}

func TestMySQLDSNDatabasePreservesCredentialsEndpointAndQuery(t *testing.T) {
	got := mysqlDSNDatabase("actual@tcp(127.0.0.1:3306)/base?parseTime=true&multiStatements=true", "ptah_case")
	want := "actual@tcp(127.0.0.1:3306)/ptah_case?parseTime=true&multiStatements=true"
	if got != want {
		t.Fatalf("DSN = %q, want %q", got, want)
	}
}
