package migrator_test

import (
	"context"
	"io/fs"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/migrator"
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

func TestRegisteredMigrationProvider_MigrationsReturnsDefensiveCopy(t *testing.T) {
	c := qt.New(t)
	provider := migrator.NewRegisteredMigrationProvider(
		&migrator.Migration{
			Version:     2,
			Description: "Second migration",
			Up:          migrator.NoopMigrationFunc,
			Down:        migrator.NoopMigrationFunc,
		},
		&migrator.Migration{
			Version:     1,
			Description: "First migration",
			Up:          migrator.NoopMigrationFunc,
			Down:        migrator.NoopMigrationFunc,
		},
	)

	migrations := provider.Migrations()
	c.Assert(migrations[0].Version, qt.Equals, int64(1))
	migrations[0], migrations[1] = migrations[1], migrations[0]
	migrations = append(migrations, &migrator.Migration{Version: 3})
	c.Assert(migrations, qt.HasLen, 3)

	next := provider.Migrations()
	c.Assert(next, qt.HasLen, 2)
	c.Assert(next[0].Version, qt.Equals, int64(1))
	c.Assert(next[1].Version, qt.Equals, int64(2))
}

func TestRegisteredMigrationProvider_ConcurrentRegisterAndMigrations(t *testing.T) {
	c := qt.New(t)
	provider := migrator.NewRegisteredMigrationProvider()
	const (
		workers    = 4
		iterations = 100
	)

	var wg sync.WaitGroup
	errs := make(chan string, workers)
	for worker := range workers {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for idx := range iterations {
				version := int64(worker*iterations + idx + 1)
				provider.Register(&migrator.Migration{
					Version:     version,
					Description: "Concurrent migration",
					Up:          migrator.NoopMigrationFunc,
					Down:        migrator.NoopMigrationFunc,
				})
			}
		}(worker)
	}

	for range workers {
		wg.Go(func() {
			for range iterations {
				migrations := provider.Migrations()
				for idx := 1; idx < len(migrations); idx++ {
					if migrations[idx-1].Version > migrations[idx].Version {
						errs <- "migrations are not sorted"
						return
					}
				}
			}
		})
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		c.Errorf("%s", err)
	}
	c.Assert(provider.Migrations(), qt.HasLen, workers*iterations)
}

func TestNewFSMigrationProvider_LoadsCheckpoint(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"0000000001_init.up.sql":                  &fstest.MapFile{Data: []byte("CREATE TABLE users (id SERIAL PRIMARY KEY);")},
		"0000000001_init.down.sql":                &fstest.MapFile{Data: []byte("DROP TABLE users;")},
		"0000000002_snapshot.checkpoint.up.sql":   &fstest.MapFile{Data: []byte("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);")},
		"0000000002_snapshot.checkpoint.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE users;")},
	}

	provider, err := migrator.NewFSMigrationProvider(fsys)
	c.Assert(err, qt.IsNil)

	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 2)
	c.Assert(findMigrationByVersion(migrations, 1).IsCheckpoint, qt.IsFalse)
	c.Assert(findMigrationByVersion(migrations, 2).IsCheckpoint, qt.IsTrue)
	c.Assert(findMigrationByVersion(migrations, 2).Description, qt.Equals, "Snapshot")
}

func TestNewFSMigrationProvider_RejectsMixedCheckpointPair(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"0000000002_snapshot.checkpoint.up.sql": &fstest.MapFile{Data: []byte("CREATE TABLE users (id SERIAL PRIMARY KEY);")},
		"0000000002_snapshot.down.sql":          &fstest.MapFile{Data: []byte("DROP TABLE users;")},
	}

	_, err := migrator.NewFSMigrationProvider(fsys)
	c.Assert(err, qt.ErrorMatches, `.*mixes checkpoint and non-checkpoint files.*`)
}

func findMigrationByVersion(migrations []*migrator.Migration, version int64) *migrator.Migration {
	for _, m := range migrations {
		if m.Version == version {
			return m
		}
	}
	return &migrator.Migration{}
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

func TestFSMigrationProvider_MigrationsReturnsDefensiveCopy(t *testing.T) {
	c := qt.New(t)
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

	migrations := provider.Migrations()
	migrations[0], migrations[1] = migrations[1], migrations[0]
	migrations = append(migrations, &migrator.Migration{Version: 3})
	c.Assert(migrations, qt.HasLen, 3)

	next := provider.Migrations()
	c.Assert(next, qt.HasLen, 2)
	c.Assert(next[0].Version, qt.Equals, int64(1))
	c.Assert(next[1].Version, qt.Equals, int64(2))
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
		"atlas.sum": &fstest.MapFile{Data: []byte(
			"h1:directory\n" +
				"20220318104614_team_A.sql h1:teamhash\n" +
				"20220318104615_add_users.sql h1:userhash\n",
		)},
		"20220318104615_add_users.sql": &fstest.MapFile{Data: []byte("CREATE TABLE users (id INT);\n")},
		"20220318104614_team_A.sql":    &fstest.MapFile{Data: []byte("CREATE TABLE teams (id INT);\n")},
	}

	provider, err := migrator.NewFSMigrationProvider(fsys, migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas))
	c.Assert(err, qt.IsNil)

	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 2)
	c.Assert(migrations[0].Version, qt.Equals, int64(20220318104614))
	c.Assert(migrations[0].Description, qt.Equals, "Team A")
	c.Assert(migrations[0].Checksum, qt.Equals, "h1:teamhash")
	c.Assert(migrations[1].Version, qt.Equals, int64(20220318104615))
	c.Assert(migrations[1].Description, qt.Equals, "Add Users")
	c.Assert(migrations[1].Checksum, qt.Equals, "h1:userhash")

	err = migrations[0].Down(context.Background(), nil)
	c.Assert(err, qt.ErrorMatches, `migration 20220318104614 has no Atlas down migration; dynamic Atlas-style down migrations are not implemented yet; add an atlas txtar down.sql section or migrate down manually`)
	var noDown *migrator.AtlasDownNotImplementedError
	c.Assert(err, qt.ErrorAs, &noDown)
	c.Assert(noDown.Version, qt.Equals, int64(20220318104614))
	c.Assert(noDown.Description, qt.Equals, "Team A")
}

func TestNewFSMigrationProvider_AtlasImportedDirectionalFiles(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"1_initial.up.sql": &fstest.MapFile{Data: []byte(
			"-- +ptah statement_timeout=7s\n" +
				"CREATE TABLE users (id INT);\n",
		)},
		"1_initial.down.sql": &fstest.MapFile{Data: []byte(
			"-- +ptah lock_timeout=2s\n" +
				"DROP TABLE users;\n",
		)},
		"2_second.sql": &fstest.MapFile{Data: []byte("CREATE TABLE teams (id INT);\n")},
	}
	interceptor := &recordingInterceptor{}
	provider, err := migrator.NewFSMigrationProvider(
		fsys,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
		migrator.WithStatementInterceptor(interceptor),
	)
	c.Assert(err, qt.IsNil)

	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 2)
	c.Assert(migrations[0].Version, qt.Equals, int64(1))
	c.Assert(migrations[0].Description, qt.Equals, "Initial")
	c.Assert(migrations[0].UpTimeouts.HasStatementTimeout, qt.IsTrue)
	c.Assert(migrations[0].UpTimeouts.StatementTimeout, qt.Equals, 7*time.Second)
	c.Assert(migrations[0].DownTimeouts.HasLockTimeout, qt.IsTrue)
	c.Assert(migrations[0].DownTimeouts.LockTimeout, qt.Equals, 2*time.Second)

	err = migrations[0].Up(context.Background(), nil)
	c.Assert(err, qt.IsNil)
	c.Assert(interceptor.statements, qt.DeepEquals, []string{"CREATE TABLE users (id INT)"})

	interceptor.statements = nil
	err = migrations[0].Down(context.Background(), nil)
	c.Assert(err, qt.IsNil)
	c.Assert(interceptor.statements, qt.DeepEquals, []string{"DROP TABLE users"})

	err = migrations[1].Down(context.Background(), nil)
	c.Assert(err, qt.ErrorMatches, `migration 2 has no Atlas down migration; dynamic Atlas-style down migrations are not implemented yet; add an atlas txtar down.sql section or migrate down manually`)
}

func TestNewFSMigrationProvider_AtlasRepeatableFilesAreDiscoveryOnly(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"2_baseline.sql": &fstest.MapFile{Data: []byte("CREATE TABLE users (id INT);\n")},
		"3R_views.sql":   &fstest.MapFile{Data: []byte("CREATE VIEW active_users AS SELECT * FROM users;\n")},
	}

	files, err := migrator.DiscoverMigrationFiles(fsys, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 2)
	c.Assert(files[0].Path, qt.Equals, "3R_views.sql")
	c.Assert(files[0].Repeatable, qt.IsTrue)

	provider, err := migrator.NewFSMigrationProvider(fsys, migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas))
	c.Assert(err, qt.IsNil)
	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)
	c.Assert(migrations[0].Version, qt.Equals, int64(2))
	c.Assert(migrations[0].Description, qt.Equals, "Baseline")
}

func TestNewFSMigrationProvider_AtlasTemplateMigrations(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"1.sql": &fstest.MapFile{Data: []byte(`{{- if eq .Env "dev" }}
CREATE TABLE dev1 (id INT);
{{- else }}
CREATE TABLE prod1 (id INT);
{{- end }}
`)},
		"2.sql": &fstest.MapFile{Data: []byte(`{{- if eq .Env "dev" }}
{{ template "shared/users" "dev2" }}
{{- else }}
{{ template "shared/users" "prod2" }}
{{- end }}
`)},
		"shared/users.sql": &fstest.MapFile{Data: []byte(`{{- define "shared/users" }}
CREATE TABLE users_{{ $ }} (id INT);
{{- end }}
`)},
	}
	interceptor := &recordingInterceptor{}
	provider, err := migrator.NewFSMigrationProvider(
		fsys,
		migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas),
		migrator.WithStatementInterceptor(interceptor),
		migrator.WithAtlasTemplateData(migrator.AtlasTemplateData{Env: "dev"}),
	)
	c.Assert(err, qt.IsNil)

	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 2)
	c.Assert(migrations[0].Version, qt.Equals, int64(1))
	c.Assert(migrations[1].Version, qt.Equals, int64(2))

	err = migrations[0].Up(context.Background(), nil)
	c.Assert(err, qt.IsNil)
	c.Assert(interceptor.statements, qt.DeepEquals, []string{"CREATE TABLE dev1 (id INT)"})

	interceptor.statements = nil
	err = migrations[1].Up(context.Background(), nil)
	c.Assert(err, qt.IsNil)
	c.Assert(interceptor.statements, qt.DeepEquals, []string{"CREATE TABLE users_dev2 (id INT)"})
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

func TestNewFSMigrationProvider_AtlasTxtarDownNoTransactionIsDirectional(t *testing.T) {
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
	c.Assert(migrations[0].UpTxMode, qt.Equals, migrator.MigrationFileTxModeUnspecified)
	c.Assert(migrations[0].DownTxMode, qt.Equals, migrator.MigrationFileTxModeNone)
}

func TestNewFSMigrationProvider_AtlasTxModeNoneIsNoTransaction(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"20240305171147_concurrent_index.sql": &fstest.MapFile{Data: []byte(`-- atlas:txtar

-- migration.sql --
-- atlas:txmode none

CREATE INDEX CONCURRENTLY users_email_idx ON users (email);

-- down.sql --
DROP INDEX users_email_idx;
`)},
	}
	provider, err := migrator.NewFSMigrationProvider(fsys, migrator.WithMigrationDirFormat(migrator.MigrationDirFormatAtlas))
	c.Assert(err, qt.IsNil)

	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)
	c.Assert(migrations[0].UpTxMode, qt.Equals, migrator.MigrationFileTxModeNone)
	c.Assert(migrations[0].DownTxMode, qt.Equals, migrator.MigrationFileTxModeUnspecified)
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

// checkDirectiveLine is the documented pre-migration check syntax from
// docs/pre-migration-checks.md. A file carrying it must load through the
// provider; this regressed once when the timeout directive scanner rejected
// the `check` token.
const checkDirectiveLine = `-- +ptah check name="users_empty" assert="SELECT count(*) = 0 FROM users" on_fail=abort`

func TestNewFSMigrationProvider_CheckDirectiveMigrationLoads(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"0000000001_drop_users.up.sql": &fstest.MapFile{
			Data: []byte(checkDirectiveLine + "\nDROP TABLE users;\n"),
		},
		"0000000001_drop_users.down.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
		},
	}

	provider, err := migrator.NewFSMigrationProvider(fsys)
	c.Assert(err, qt.IsNil)

	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)
	// The check survives loading verbatim so it executes on apply.
	c.Assert(migrations[0].UpSQL, qt.Contains, checkDirectiveLine)
	checks, err := migrator.ParseChecks(migrations[0].UpSQL, "")
	c.Assert(err, qt.IsNil)
	c.Assert(checks, qt.HasLen, 1)
	c.Assert(checks[0].Name, qt.Equals, "users_empty")
}

func TestNewFSMigrationProvider_CheckAndTimeoutDirectivesInOneFile(t *testing.T) {
	c := qt.New(t)

	fsys := fstest.MapFS{
		"0000000001_drop_users.up.sql": &fstest.MapFile{
			Data: []byte(checkDirectiveLine + "\n-- +ptah lock_timeout=3s statement_timeout=30s\nDROP TABLE users;\n"),
		},
		"0000000001_drop_users.down.sql": &fstest.MapFile{
			Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
		},
	}

	provider, err := migrator.NewFSMigrationProvider(fsys)
	c.Assert(err, qt.IsNil)

	migrations := provider.Migrations()
	c.Assert(migrations, qt.HasLen, 1)
	c.Assert(migrations[0].UpTimeouts.HasLockTimeout, qt.IsTrue)
	c.Assert(migrations[0].UpTimeouts.LockTimeout, qt.Equals, 3*time.Second)
	c.Assert(migrations[0].UpTimeouts.HasStatementTimeout, qt.IsTrue)
	c.Assert(migrations[0].UpTimeouts.StatementTimeout, qt.Equals, 30*time.Second)
	checks, err := migrator.ParseChecks(migrations[0].UpSQL, "")
	c.Assert(err, qt.IsNil)
	c.Assert(checks, qt.HasLen, 1)
}

func TestNewFSMigrationProvider_BareUnknownDirectiveStillRejected(t *testing.T) {
	c := qt.New(t)

	// A bare token that no directive family owns (e.g. a typo'd timeout
	// directive that lost its value) must keep failing the load with the
	// established error message.
	fsys := fstest.MapFS{
		"0000000001_init.up.sql": &fstest.MapFile{
			Data: []byte("-- +ptah lock_timeuot\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
		},
		"0000000001_init.down.sql": &fstest.MapFile{
			Data: []byte("DROP TABLE users;\n"),
		},
	}

	provider, err := migrator.NewFSMigrationProvider(fsys)
	c.Assert(err, qt.ErrorMatches, `failed to load up migration 0000000001_init\.up\.sql: invalid \+ptah directive "lock_timeuot"`)
	c.Assert(provider, qt.IsNil)
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
