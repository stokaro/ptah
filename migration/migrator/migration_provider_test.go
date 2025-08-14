package migrator_test

import (
	"io/fs"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/migrator"
)

func TestNewRegisteredMigrationProvider(t *testing.T) {
	c := qt.New(t)

	// Test with no migrations
	provider := migrator.NewRegisteredMigrationProvider()
	c.Assert(provider, qt.IsNotNil)
	c.Assert(provider.Migrations(), qt.HasLen, 0)

	// Test with migrations
	migration1 := &migrator.Migration{
		Version:     1,
		Description: "First migration",
		Up:          migrator.NoopMigrationFunc,
		Down:        migrator.NoopMigrationFunc,
	}
	migration2 := &migrator.Migration{
		Version:     2,
		Description: "Second migration",
		Up:          migrator.NoopMigrationFunc,
		Down:        migrator.NoopMigrationFunc,
	}

	provider = migrator.NewRegisteredMigrationProvider(migration1, migration2)
	c.Assert(provider, qt.IsNotNil)
	c.Assert(provider.Migrations(), qt.HasLen, 2)
}

func TestRegisteredMigrationProvider_Register(t *testing.T) {
	c := qt.New(t)

	provider := migrator.NewRegisteredMigrationProvider()

	migration1 := &migrator.Migration{
		Version:     1,
		Description: "First migration",
		Up:          migrator.NoopMigrationFunc,
		Down:        migrator.NoopMigrationFunc,
	}

	// Register first migration
	provider.Register(migration1)
	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)
	c.Assert(migrations[0].Version, qt.Equals, 1)

	migration2 := &migrator.Migration{
		Version:     2,
		Description: "Second migration",
		Up:          migrator.NoopMigrationFunc,
		Down:        migrator.NoopMigrationFunc,
	}

	// Register second migration
	provider.Register(migration2)
	migrations = provider.Migrations()
	c.Assert(migrations, qt.HasLen, 2)
	c.Assert(migrations[0].Version, qt.Equals, 1)
	c.Assert(migrations[1].Version, qt.Equals, 2)
}

func TestRegisteredMigrationProvider_Sorting(t *testing.T) {
	c := qt.New(t)

	provider := migrator.NewRegisteredMigrationProvider()

	// Register migrations in reverse order
	migration3 := &migrator.Migration{
		Version:     3,
		Description: "Third migration",
		Up:          migrator.NoopMigrationFunc,
		Down:        migrator.NoopMigrationFunc,
	}
	migration1 := &migrator.Migration{
		Version:     1,
		Description: "First migration",
		Up:          migrator.NoopMigrationFunc,
		Down:        migrator.NoopMigrationFunc,
	}
	migration2 := &migrator.Migration{
		Version:     2,
		Description: "Second migration",
		Up:          migrator.NoopMigrationFunc,
		Down:        migrator.NoopMigrationFunc,
	}

	provider.Register(migration3)
	provider.Register(migration1)
	provider.Register(migration2)

	// Should be sorted by version
	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 3)
	c.Assert(migrations[0].Version, qt.Equals, 1)
	c.Assert(migrations[1].Version, qt.Equals, 2)
	c.Assert(migrations[2].Version, qt.Equals, 3)
}

func TestNewFSMigrationProvider_Success(t *testing.T) {
	c := qt.New(t)

	// Create a test filesystem with valid migration files
	fsys := fstest.MapFS{
		"0000000001_create_users.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE users (id SERIAL PRIMARY KEY);"),
		},
		"0000000001_create_users.down.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE users;"),
		},
		"0000000002_add_index.up.sql": &fstest.MapFile{
			Data: []byte("CREATE INDEX idx_users_id ON users(id);"),
		},
		"0000000002_add_index.down.sql": &fstest.MapFile{
			Data: []byte("DROP INDEX idx_users_id;"),
		},
	}

	provider, err := migrator.NewFSMigrationProvider(fsys)
	c.Assert(err, qt.IsNil)
	c.Assert(provider, qt.IsNotNil)

	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 2)
	c.Assert(migrations[0].Version, qt.Equals, 1)
	c.Assert(migrations[0].Description, qt.Equals, "Create Users")
	c.Assert(migrations[1].Version, qt.Equals, 2)
	c.Assert(migrations[1].Description, qt.Equals, "Add Index")
}

func TestNewFSMigrationProvider_IncompleteMigrations(t *testing.T) {
	c := qt.New(t)

	// Create a test filesystem with incomplete migration (missing down file)
	fsys := fstest.MapFS{
		"0000000001_create_users.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE users (id SERIAL PRIMARY KEY);"),
		},
		// Missing down file
	}

	provider, err := migrator.NewFSMigrationProvider(fsys)
	c.Assert(err, qt.IsNotNil)
	c.Assert(provider, qt.IsNil)
	c.Assert(err.Error(), qt.Contains, "incomplete migrations found")
}

func TestNewFSMigrationProvider_EmptyFilesystem(t *testing.T) {
	c := qt.New(t)

	// Create an empty filesystem
	fsys := fstest.MapFS{}

	provider, err := migrator.NewFSMigrationProvider(fsys)
	c.Assert(err, qt.IsNil)
	c.Assert(provider, qt.IsNotNil)

	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 0)
}

func TestNewFSMigrationProvider_InvalidFiles(t *testing.T) {
	c := qt.New(t)

	// Create a filesystem with invalid files that should be ignored
	fsys := fstest.MapFS{
		"0000000001_create_users.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE users (id SERIAL PRIMARY KEY);"),
		},
		"0000000001_create_users.down.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE users;"),
		},
		"invalid_file.txt": &fstest.MapFile{
			Data: []byte("This should be ignored"),
		},
		"README.md": &fstest.MapFile{
			Data: []byte("# Migrations"),
		},
	}

	provider, err := migrator.NewFSMigrationProvider(fsys)
	c.Assert(err, qt.IsNil)
	c.Assert(provider, qt.IsNotNil)

	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1) // Only the valid migration should be loaded
	c.Assert(migrations[0].Version, qt.Equals, 1)
}

func TestFSMigrationProvider_FilesystemError(t *testing.T) {
	c := qt.New(t)

	// Create a filesystem that will cause an error during walking
	fsys := &errorFS{}

	provider, err := migrator.NewFSMigrationProvider(fsys)
	c.Assert(err, qt.IsNotNil)
	c.Assert(provider, qt.IsNil)
}

// errorFS is a test filesystem that always returns an error
type errorFS struct{}

func (e *errorFS) Open(name string) (fs.File, error) {
	return nil, fs.ErrNotExist
}
