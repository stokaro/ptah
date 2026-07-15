package migrator_test

import (
	"context"
	"io/fs"
	"testing"
	"testing/fstest"
	"time"

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
	c.Assert(migrations[0].Version, qt.Equals, int64(1))

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
	c.Assert(migrations[0].Version, qt.Equals, int64(1))
	c.Assert(migrations[1].Version, qt.Equals, int64(2))
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
	c.Assert(migrations[0].Version, qt.Equals, int64(1))
	c.Assert(migrations[1].Version, qt.Equals, int64(2))
	c.Assert(migrations[2].Version, qt.Equals, int64(3))
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
	c.Assert(migrations[0].Version, qt.Equals, int64(1))
	c.Assert(migrations[0].Description, qt.Equals, "Create Users")
	c.Assert(migrations[1].Version, qt.Equals, int64(2))
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

func TestNewFSMigrationProvider_AtlasFormat(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"20220318104615_add_users.sql": &fstest.MapFile{Data: []byte("CREATE TABLE users (id INT);\n")},
		"20220318104614_team_A.sql":    &fstest.MapFile{Data: []byte("CREATE TABLE teams (id INT);\n")},
	}

	provider, err := migrator.NewFSMigrationProvider(fsys, migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas))
	c.Assert(err, qt.IsNil)

	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 2)
	c.Assert(migrations[0].Version, qt.Equals, int64(20220318104614))
	c.Assert(migrations[0].Description, qt.Equals, "Team A")
	c.Assert(migrations[1].Version, qt.Equals, int64(20220318104615))
	c.Assert(migrations[1].Description, qt.Equals, "Add Users")

	err = migrations[0].Down(context.Background(), nil)
	c.Assert(err, qt.ErrorMatches, `migration 20220318104614 has no Atlas down migration; dynamic Atlas-style down migrations are not implemented yet; add an atlas txtar down.sql section or migrate down manually`)
	var noDown *migrator.AtlasDownNotImplementedError
	c.Assert(err, qt.ErrorAs, &noDown)
	c.Assert(noDown.Version, qt.Equals, int64(20220318104614))
	c.Assert(noDown.Description, qt.Equals, "Team A")
}

func TestNewFSMigrationProvider_AtlasTxtarSectionsAndDirectives(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"20240305171147_section_boundary.sql": &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- migration.sql --
-- +ptah lock_timeout=3s
-- keep this marker-like SQL comment --
CREATE TABLE users (id INT PRIMARY KEY);

-- schema.sql --
SELECT 'ptah_extra_section_sentinel';

-- down.sql --
-- +ptah statement_timeout=30s
SELECT 'ptah_down_section_sentinel';
DROP TABLE users;
`)},
	}
	interceptor := &recordingInterceptor{}
	provider, err := migrator.NewFSMigrationProvider(
		fsys,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
		migrator.WithStatementInterceptor(interceptor),
	)
	c.Assert(err, qt.IsNil)

	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)
	migration := migrations[0]
	c.Assert(migration.UpTimeouts.HasLockTimeout, qt.IsTrue)
	c.Assert(migration.UpTimeouts.LockTimeout, qt.Equals, 3*time.Second)
	c.Assert(migration.DownTimeouts.HasStatementTimeout, qt.IsTrue)
	c.Assert(migration.DownTimeouts.StatementTimeout, qt.Equals, 30*time.Second)

	err = migration.Up(context.Background(), nil)
	c.Assert(err, qt.IsNil)
	c.Assert(interceptor.statements, qt.DeepEquals, []string{
		"CREATE TABLE users (id INT PRIMARY KEY)",
	})
	c.Assert(interceptor.directives, qt.DeepEquals, []map[string]string{
		{"lock_timeout": "3s"},
	})

	interceptor.statements = nil
	interceptor.directives = nil
	err = migration.Down(context.Background(), nil)
	c.Assert(err, qt.IsNil)
	c.Assert(interceptor.statements, qt.DeepEquals, []string{
		"SELECT 'ptah_down_section_sentinel'",
		"DROP TABLE users",
	})
	c.Assert(interceptor.directives, qt.DeepEquals, []map[string]string{
		{"statement_timeout": "30s"},
		{"statement_timeout": "30s"},
	})
}

func TestNewFSMigrationProvider_AtlasTxtarDownInvalidDirective(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"20240305171147_invalid_down_directive.sql": &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- migration.sql --
CREATE TABLE users (id INT PRIMARY KEY);

-- down.sql --
-- +ptah no_transaction=maybe
DROP TABLE users;
`)},
	}
	provider, err := migrator.NewFSMigrationProvider(fsys, migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas))
	c.Assert(provider, qt.IsNil)
	c.Assert(err, qt.ErrorMatches, `failed to load Atlas migration 20240305171147_invalid_down_directive.sql: invalid migration directives in 20240305171147_invalid_down_directive.sql#down.sql: invalid \+ptah no_transaction value "maybe": expected true or false`)
}

func TestNewFSMigrationProvider_AtlasTxtarDownNoTransactionIsMigrationLevel(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"20240305171147_down_no_transaction.sql": &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- migration.sql --
CREATE TABLE users (id INT PRIMARY KEY);

-- down.sql --
-- +ptah no_transaction
DROP TABLE users;
`)},
	}
	provider, err := migrator.NewFSMigrationProvider(fsys, migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas))
	c.Assert(err, qt.IsNil)

	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)
	c.Assert(migrations[0].NoTransaction, qt.IsTrue)
}

func TestNewFSMigrationProvider_UnknownOnlySQLFilesError(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"cleanup.sql": &fstest.MapFile{Data: []byte("DROP TABLE users;\n")},
	}

	provider, err := migrator.NewFSMigrationProvider(fsys)
	c.Assert(provider, qt.IsNil)
	c.Assert(err, qt.ErrorMatches, `no migration files matched format "auto"; unrecognized SQL files: cleanup.sql`)
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

func TestNewFSMigrationProvider_DescriptionEndingInUpIsNotAMigration(t *testing.T) {
	c := qt.New(t)

	// Regression for issue #245: with the unescaped dot in fileNameRe,
	// 0000000003_cleanup.sql used to register as version 3's UP migration
	// (description "Clea") and its SQL would run on migrate-up.
	fsys := fstest.MapFS{
		"0000000001_create_users.up.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE users (id SERIAL PRIMARY KEY);"),
		},
		"0000000001_create_users.down.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE users;"),
		},
		"0000000003_cleanup.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE users;"),
		},
		"0000000004_teardown.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE audit;"),
		},
	}

	provider, err := migrator.NewFSMigrationProvider(fsys)
	c.Assert(err, qt.IsNil, qt.Commentf("suffix-less files are skipped, not incomplete migrations"))
	c.Assert(provider, qt.IsNotNil)

	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)
	c.Assert(migrations[0].Version, qt.Equals, int64(1))
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
	c.Assert(migrations[0].Version, qt.Equals, int64(1))
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
