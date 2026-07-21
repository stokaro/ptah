//go:build testkitcontainers

package testkit

import (
	"os"
	"testing"
	"testing/fstest"

	"github.com/testcontainers/testcontainers-go"

	"github.com/stokaro/ptah/dbschema"
)

func TestStartPostgresAppliesMigrations(t *testing.T) {
	skipIfContainerProviderUnavailable(t)
	testContainerDatabase(t, StartPostgres(t, WithReuseByName("ptah-testkit-postgres")))
}

func TestStartMySQLAppliesMigrations(t *testing.T) {
	skipIfContainerProviderUnavailable(t)
	testContainerDatabase(t, StartMySQL(t, WithReuseByName("ptah-testkit-mysql")))
}

func TestStartMariaDBAppliesMigrations(t *testing.T) {
	skipIfContainerProviderUnavailable(t)
	testContainerDatabase(t, StartMariaDB(t, WithReuseByName("ptah-testkit-mariadb")))
}

func skipIfContainerProviderUnavailable(t *testing.T) {
	t.Helper()
	if os.Getenv("CI") != "" {
		return
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)
}

func testContainerDatabase(t *testing.T, db *dbschema.DatabaseConnection) {
	t.Helper()

	migrations := fstest.MapFS{
		"000001_create_users.up.sql": {
			Data: []byte("CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255) NOT NULL UNIQUE);"),
		},
		"000001_create_users.down.sql": {
			Data: []byte("DROP TABLE users;"),
		},
	}
	ApplyMigrationsFromFS(t, db, migrations)
	Seed(t, db, []byte("INSERT INTO users (id, email) VALUES (1, 'a@example.com');"))

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count); err != nil {
		t.Fatalf("count users: %v", err)
	}
	if count != 1 {
		t.Fatalf("users count = %d, want 1", count)
	}
}
