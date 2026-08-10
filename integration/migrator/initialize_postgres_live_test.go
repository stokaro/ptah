//go:build integration

package migrator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestInitializeDebug(t *testing.T) {
	c := qt.New(t)

	dbURL := postgresTestURL(t)

	// Connect to database
	ctx := t.Context()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

	// Clean up any existing schema_migrations table to ensure a clean test
	_, _ = conn.Exec("DROP TABLE IF EXISTS schema_migrations")

	// Create a migrator
	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider())

	// Test Initialize method directly
	err = m.Initialize(ctx)
	c.Assert(err, qt.IsNil, qt.Commentf("Initialize should not fail"))

	// Test that the table was created
	var count int
	row := conn.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'schema_migrations'")
	err = row.Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 1, qt.Commentf("schema_migrations table should exist"))

	// Test GetCurrentVersion
	version, err := m.GetCurrentVersion(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, 0, qt.Commentf("Initial version should be 0"))
}
