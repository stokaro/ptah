//go:build integration

package migrator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestInitializePostgresCreatesMetadata(t *testing.T) {
	c := qt.New(t)
	dbURL := postgresTestURL(t)
	ctx := t.Context()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer func() { _ = conn.Close() }()

	const migrationsTable = "schema_migrations_initialize_test"
	_, err = conn.Exec("DROP TABLE IF EXISTS " + migrationsTable)
	c.Assert(err, qt.IsNil)
	defer func() {
		_, cleanupErr := conn.Exec("DROP TABLE IF EXISTS " + migrationsTable)
		qt.Check(t, cleanupErr, qt.IsNil)
	}()

	m := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider()).
		WithMigrationsTable("", migrationsTable)

	err = m.Initialize(ctx)
	c.Assert(err, qt.IsNil)

	var count int
	row := conn.QueryRow(
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = $1",
		migrationsTable,
	)
	err = row.Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 1)

	version, err := m.GetCurrentVersion(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(0))
}
