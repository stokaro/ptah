package migrator_test

import (
	"log/slog"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/migrator"
)

func TestNewMigrator(t *testing.T) {
	c := qt.New(t)

	// Create a mock provider
	provider := migrator.NewRegisteredMigrationProvider()

	// Test with nil connection (should not panic)
	m := migrator.NewMigrator(nil, provider)
	c.Assert(m, qt.IsNotNil)
	c.Assert(m.MigrationProvider(), qt.Equals, provider)
}

func TestNewFSMigrator_Success(t *testing.T) {
	c := qt.New(t)

	// Create a test filesystem with valid migration files
	fsys := fstest.MapFS{
		"0000000001_create_users.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE users (id SERIAL PRIMARY KEY);"),
		},
		"0000000001_create_users.down.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE users;"),
		},
	}

	m, err := migrator.NewFSMigrator(nil, fsys)
	c.Assert(err, qt.IsNil)
	c.Assert(m, qt.IsNotNil)
	c.Assert(m.MigrationProvider().Migrations(), qt.HasLen, 1)
}

func TestNewFSMigrator_InvalidFilesystem(t *testing.T) {
	c := qt.New(t)

	// Create a filesystem with incomplete migrations
	fsys := fstest.MapFS{
		"0000000001_create_users.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE users (id SERIAL PRIMARY KEY);"),
		},
		// Missing down file
	}

	m, err := migrator.NewFSMigrator(nil, fsys)
	c.Assert(err, qt.IsNotNil)
	c.Assert(m, qt.IsNil)
	c.Assert(err.Error(), qt.Contains, "incomplete migrations found")
}

func TestMigrator_WithLogger(t *testing.T) {
	c := qt.New(t)

	provider := migrator.NewRegisteredMigrationProvider()
	m := migrator.NewMigrator(nil, provider)

	// Create a custom logger
	logger := slog.Default()
	m2 := m.WithLogger(logger)

	// Should return a new instance
	c.Assert(m2, qt.Not(qt.Equals), m)
	c.Assert(m2, qt.IsNotNil)
}

func TestMigrator_MigrationProvider(t *testing.T) {
	c := qt.New(t)

	provider := migrator.NewRegisteredMigrationProvider()
	m := migrator.NewMigrator(nil, provider)

	c.Assert(m.MigrationProvider(), qt.Equals, provider)
}

// Note: Due to the complexity of testing database operations and the current architecture,
// many of the Migrator methods would require significant refactoring to be easily testable
// without a real database connection. The tests above cover the basic functionality that
// can be tested without database dependencies.

// For comprehensive testing of migration execution, integration tests would be more appropriate.
