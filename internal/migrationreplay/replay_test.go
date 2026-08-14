package migrationreplay_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/migrationreplay"
	"go.5x5.cz/ptah/migration/migrator"
)

// TestReplayRoutesADockerDevURLToTheProvisioner replaced a test that asserted
// replay REFUSED every docker:// dev URL. It no longer does: it provisions one
// (stokaro/ptah#844).
//
// The URL is a docker one this build will not start, so the assertion needs no
// container runtime and starts nothing. That is not a convenience -- measured on
// the pinned community binary v1.3.0 on 2026-08-13, `docker://sqlite/latest/dev`
// answers `unsupported docker image "sqlite"` and exits 1, so provisioning it
// would be exiting 0 where that binary exits 1. The message therefore proves two
// things at once: the value reached the provisioning layer, and the layer
// refused the one form it must.
//
// The old fixture named `docker://postgres/16/dev`, which this build now starts.
// Left as it was, this unit test would pull an image and run a container.
func TestReplayRoutesADockerDevURLToTheProvisioner(t *testing.T) {
	c := qt.New(t)

	err := migrationreplay.Replay(context.Background(), migrationreplay.Options{
		DevURL: "docker://sqlite/latest/dev",
	})

	c.Assert(err, qt.ErrorMatches, `unsupported docker image "sqlite"`)
}

func TestReplayCleansDevDatabaseAndIgnoresExistingRevisionRows(t *testing.T) {
	c := qt.New(t)
	migrationsDir := t.TempDir()
	devDBPath := filepath.Join(t.TempDir(), "dev.db")
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_create_replay_runs.sql"),
		[]byte("CREATE TABLE replay_runs (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

	conn, err := dbschema.ConnectToDatabase(t.Context(), "sqlite://"+devDBPath)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(t.Context(), "CREATE TABLE stale_replay_runs (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(t.Context(), "CREATE TABLE schema_migrations (version BIGINT NOT NULL PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(t.Context(), "INSERT INTO schema_migrations (version) VALUES (1)")
	c.Assert(err, qt.IsNil)
	dbschema.CloseAndWarn(conn)

	err = migrationreplay.Replay(t.Context(), migrationreplay.Options{
		Dir:       migrationsDir,
		DirFormat: migrator.MigrationDirFormatAtlas,
		DevURL:    "sqlite://" + devDBPath,
	})
	c.Assert(err, qt.IsNil)
	conn, err = dbschema.ConnectToDatabase(t.Context(), "sqlite://"+devDBPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	assertSQLiteRealmObjectCount(c, conn, 0)
}

func TestReplayUsesProvidedFilesystemSnapshot(t *testing.T) {
	c := qt.New(t)
	migrationsDir := t.TempDir()
	devDBPath := filepath.Join(t.TempDir(), "dev.db")
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "1_invalid.sql"),
		[]byte("THIS IS NOT SQL;\n"),
		0o600,
	), qt.IsNil)
	snapshot := fstest.MapFS{
		"1_create_replay_runs.sql": {
			Data: []byte(`{{- if eq .Env "dev" }}
CREATE TABLE replay_runs (id INTEGER PRIMARY KEY);
{{- else }}
CREATE TABLE wrong_template_branch (id INTEGER PRIMARY KEY);
{{- end }}
`),
		},
	}

	err := migrationreplay.Replay(context.Background(), migrationreplay.Options{
		Dir:               migrationsDir,
		DirFormat:         migrator.MigrationDirFormatAtlas,
		DevURL:            "sqlite://" + devDBPath,
		FS:                snapshot,
		AtlasTemplateData: migrator.AtlasTemplateData{Env: "dev"},
	})

	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(t.Context(), "sqlite://"+devDBPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	assertSQLiteRealmObjectCount(c, conn, 0)
}

func TestReplayProviderFailurePreservesDevDatabase(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	devDBPath := filepath.Join(t.TempDir(), "dev.db")
	devURL := "sqlite://" + devDBPath
	conn, err := dbschema.ConnectToDatabase(ctx, devURL)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, "CREATE TABLE sentinel (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	dbschema.CloseAndWarn(conn)

	err = migrationreplay.Replay(ctx, migrationreplay.Options{
		DirFormat: migrator.MigrationDirFormatPtah,
		DevURL:    devURL,
		FS: fstest.MapFS{
			"0000000001_incomplete.up.sql": {
				Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
			},
		},
	})
	c.Assert(err, qt.ErrorMatches, "load migration directory: incomplete migrations found .*")

	conn, err = dbschema.ConnectToDatabase(ctx, devURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	err = conn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM main.sqlite_schema
		WHERE type = 'table'
		  AND name = 'sentinel'
	`).Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 1)
}

func TestReplayPreCanceledContextPreservesDevDatabase(t *testing.T) {
	c := qt.New(t)
	devURL := "sqlite://" + filepath.Join(t.TempDir(), "dev.db")
	conn, err := dbschema.ConnectToDatabase(t.Context(), devURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(t.Context(), "CREATE TABLE sentinel (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err = migrationreplay.ReplaySnapshotOnConnection(
		ctx,
		conn,
		fstest.MapFS{
			"1_create_users.sql": {
				Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
			},
		},
		migrator.MigrationDirFormatAtlas,
	)

	c.Assert(err, qt.ErrorIs, context.Canceled)
	var count int
	err = conn.QueryRowContext(
		t.Context(),
		"SELECT COUNT(*) FROM pragma_table_list WHERE schema = ? AND name = ?",
		"main",
		"sentinel",
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 1)
}

func TestReplayExecutionFailureCleansPartialDevDatabase(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	devDBPath := filepath.Join(t.TempDir(), "dev.db")
	devURL := "sqlite://" + devDBPath

	err := migrationreplay.Replay(ctx, migrationreplay.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
		DevURL:    devURL,
		FS: fstest.MapFS{
			"1_create_users.sql": {
				Data: []byte(`
CREATE TABLE users (id INTEGER PRIMARY KEY);
CREATE VIEW user_ids AS SELECT id FROM users;
`),
			},
			"2_invalid.sql": {
				Data: []byte("THIS IS NOT SQL;\n"),
			},
		},
	})
	c.Assert(err, qt.ErrorMatches, `(?s)replay migration 2 on dev database: .*`)

	conn, err := dbschema.ConnectToDatabase(ctx, devURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	err = conn.QueryRowContext(c.Context(), `
		SELECT COUNT(*)
		FROM pragma_table_list
		WHERE schema = ?
		  AND type IN ('table', 'view', 'virtual')
		  AND name NOT LIKE 'sqlite_%'
		  AND name <> 'schema_migrations'
	`, "main").Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 0)
}

func TestReplaySQLiteRejectsAttachedDatabaseEscape(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	devURL := "sqlite://" + filepath.Join(t.TempDir(), "dev.db")

	err := migrationreplay.Replay(ctx, migrationreplay.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
		DevURL:    devURL,
		FS: fstest.MapFS{
			"1_attach.sql": {
				Data: []byte(`
CREATE TABLE users (id INTEGER PRIMARY KEY);
ATTACH DATABASE ':memory:' AS aux;
CREATE TABLE aux.outside_realm (id INTEGER PRIMARY KEY);
`),
			},
		},
	})

	c.Assert(
		err,
		qt.ErrorMatches,
		`(?s)replay migration 1 on dev database: .*sqlite migration replay rejects ATTACH .*`,
	)
	conn, err := dbschema.ConnectToDatabase(ctx, devURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	err = conn.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM main.sqlite_schema
		WHERE name NOT LIKE 'sqlite_%'
	`).Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 0)
}

func TestWithReplayedSnapshot_UsesPinnedSQLiteSessionAndRestoresState(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	devURL := "sqlite://" + filepath.Join(t.TempDir(), "dev.db")
	conn, err := dbschema.ConnectToDatabase(ctx, devURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	callbackForeignKeys := -1
	callbackObjects := -1

	err = migrationreplay.WithReplayedSnapshot(
		ctx,
		conn,
		fstest.MapFS{
			"1_create_users.sql": {
				Data: []byte(`
PRAGMA foreign_keys = OFF;
CREATE TABLE users (id INTEGER PRIMARY KEY);
`),
			},
		},
		migrator.MigrationDirFormatAtlas,
		func(replayConn *dbschema.DatabaseConnection) error {
			err := replayConn.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&callbackForeignKeys)
			c.Assert(err, qt.IsNil)
			err = replayConn.QueryRowContext(ctx, `
				SELECT COUNT(*)
				FROM main.sqlite_schema
				WHERE type = 'table'
				  AND name = 'users'
			`).Scan(&callbackObjects)
			c.Assert(err, qt.IsNil)
			return nil
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(callbackForeignKeys, qt.Equals, 0)
	c.Assert(callbackObjects, qt.Equals, 1)
	var restoredForeignKeys int
	err = conn.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&restoredForeignKeys)
	c.Assert(err, qt.IsNil)
	c.Assert(restoredForeignKeys, qt.Equals, 1)
	assertSQLiteRealmObjectCount(c, conn, 0)
}

func TestWithReplayedSnapshot_CallbackFailureCleansDatabaseRealm(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	devURL := "sqlite://" + filepath.Join(t.TempDir(), "dev.db")
	conn, err := dbschema.ConnectToDatabase(ctx, devURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	consumeErr := errors.New("injected consumer failure")

	err = migrationreplay.WithReplayedSnapshot(
		ctx,
		conn,
		fstest.MapFS{
			"1_create_users.sql": {
				Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
			},
		},
		migrator.MigrationDirFormatAtlas,
		func(*dbschema.DatabaseConnection) error {
			return consumeErr
		},
	)

	c.Assert(err, qt.ErrorIs, consumeErr)
	assertSQLiteRealmObjectCount(c, conn, 0)
}

func TestWithReplayedSnapshot_NilCallbackPreservesDatabaseRealm(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	devURL := "sqlite://" + filepath.Join(t.TempDir(), "dev.db")
	conn, err := dbschema.ConnectToDatabase(ctx, devURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(ctx, "CREATE TABLE sentinel (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)

	err = migrationreplay.WithReplayedSnapshot(
		ctx,
		conn,
		fstest.MapFS{
			"1_create_users.sql": {
				Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
			},
		},
		migrator.MigrationDirFormatAtlas,
		nil,
	)

	c.Assert(err, qt.ErrorMatches, `consume replayed database callback is nil`)
	assertSQLiteRealmObjectCount(c, conn, 1)
}

func TestWithReplayedSnapshot_SerializesConcurrentRealmReplay(t *testing.T) {
	c := qt.New(t)
	devURL := "sqlite://" + filepath.Join(t.TempDir(), "dev.db")
	firstConn, err := dbschema.ConnectToDatabase(t.Context(), devURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(firstConn)
	secondConn, err := dbschema.ConnectToDatabase(t.Context(), devURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(secondConn)
	snapshot := fstest.MapFS{
		"1_create_users.sql": {
			Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
		},
	}
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstDone := make(chan error, 1)

	go func() {
		firstDone <- migrationreplay.WithReplayedSnapshot(
			t.Context(),
			firstConn,
			snapshot,
			migrator.MigrationDirFormatAtlas,
			func(*dbschema.DatabaseConnection) error {
				close(firstEntered)
				<-releaseFirst
				return nil
			},
		)
	}()
	<-firstEntered

	waitCtx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	err = migrationreplay.WithReplayedSnapshot(
		waitCtx,
		secondConn,
		snapshot,
		migrator.MigrationDirFormatAtlas,
		func(*dbschema.DatabaseConnection) error {
			return nil
		},
	)

	c.Assert(err, qt.ErrorIs, context.DeadlineExceeded)
	close(releaseFirst)
	c.Assert(<-firstDone, qt.IsNil)
	assertSQLiteRealmObjectCount(c, firstConn, 0)
}

func assertSQLiteRealmObjectCount(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	want int,
) {
	c.Helper()
	var count int
	err := conn.QueryRowContext(c.Context(), `
		SELECT COUNT(*)
		FROM main.sqlite_schema
		WHERE name NOT LIKE 'sqlite_%'
	`).Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, want)
}
