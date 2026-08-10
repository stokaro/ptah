//go:build integration

package integration_test

import (
	"os"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	"github.com/testcontainers/testcontainers-go"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/testkit"
)

func TestStartPostgresAppliesMigrations(t *testing.T) {
	skipIfContainerProviderUnavailable(t)
	testContainerDatabase(t, testkit.StartPostgres(t, testkit.WithReuseByName("ptah-testkit-postgres")))
}

func TestStartMySQLAppliesMigrations(t *testing.T) {
	skipIfContainerProviderUnavailable(t)
	testContainerDatabase(t, testkit.StartMySQL(t, testkit.WithReuseByName("ptah-testkit-mysql")))
}

func TestStartMariaDBAppliesMigrations(t *testing.T) {
	skipIfContainerProviderUnavailable(t)
	testContainerDatabase(t, testkit.StartMariaDB(t, testkit.WithReuseByName("ptah-testkit-mariadb")))
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
	c := qt.New(t)

	migrations := fstest.MapFS{
		"000001_create_users.up.sql": {
			Data: []byte("CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255) NOT NULL UNIQUE);"),
		},
		"000001_create_users.down.sql": {
			Data: []byte("DROP TABLE users;"),
		},
	}
	testkit.ApplyMigrationsFromFS(t, db, migrations)
	testkit.Seed(t, db, []byte("INSERT INTO users (id, email) VALUES (1, 'a@example.com');"))

	var count int
	c.Assert(db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count), qt.IsNil)
	c.Assert(count, qt.Equals, 1)
}
