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

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/internal/migrationreplay"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// TestReplayRoutesADockerDevURLToTheProvisioner pins that replay provisions a
// docker:// dev URL rather than refusing it (stokaro/ptah#844).
//
// The URL is a docker one this build will not start, so the assertion needs no
// container runtime and starts nothing. That is not a convenience -- measured on
// the pinned community binary v1.3.0 on 2026-08-13, `docker://sqlite/latest/dev`
// answers `unsupported docker image "sqlite"` and exits 1, so provisioning it
// would be exiting 0 where that binary exits 1. The message therefore proves the
// routing and the refusal at once: the value reached the provisioning layer,
// and the layer refused the one form it must.
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
		DirFormat: migrationfile.DirFormatAtlas,
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
		DirFormat:         migrationfile.DirFormatAtlas,
		DevURL:            "sqlite://" + devDBPath,
		FS:                snapshot,
		AtlasTemplateData: migrationfile.AtlasTemplateData{Env: "dev"},
	})

	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(t.Context(), "sqlite://"+devDBPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	assertSQLiteRealmObjectCount(c, conn, 0)
}

func TestReplayFailureNamesExactEmptyRevision(t *testing.T) {
	c := qt.New(t)
	devDBPath := filepath.Join(t.TempDir(), "empty-revision.db")

	err := migrationreplay.Replay(t.Context(), migrationreplay.Options{
		DirFormat: migrationfile.DirFormatAtlas,
		DevURL:    "sqlite://" + devDBPath,
		FS: fstest.MapFS{
			"10_broken.sql": {Data: []byte("INSERT INTO missing_replay_table VALUES (1);\n")},
		},
		RevisionVersions: map[int64]string{10: ""},
	})

	c.Assert(err, qt.ErrorMatches, `(?s)replay migration "" on dev database: .*`)
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
		DirFormat: migrationfile.DirFormatPtah,
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
		migrationfile.DirFormatAtlas,
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
		DirFormat: migrationfile.DirFormatAtlas,
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
		DirFormat: migrationfile.DirFormatAtlas,
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

// TestWithReplayedSnapshot_UsesPinnedSQLiteSessionAndRestoresState runs a
// migration that opts out of the transaction, so its pragma changes the replay
// session the way it changes an applied one, and the consumer reads it on the
// same session before the replay restores it.
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
				Data: []byte(`-- atlas:txmode none

PRAGMA foreign_keys = OFF;
CREATE TABLE users (id INTEGER PRIMARY KEY);
`),
			},
		},
		migrationfile.DirFormatAtlas,
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

// TestWithReplayedSnapshot_TransactionalMigrationIgnoresAForeignKeysPragma is
// the counterpart of the test above for a migration that runs in a
// transaction. SQLite ignores a foreign_keys pragma inside a transaction, and
// the executor applies this file in one, so the replay session keeps
// enforcement on as an applied database would.
func TestWithReplayedSnapshot_TransactionalMigrationIgnoresAForeignKeysPragma(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(t.TempDir(), "dev.db"))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	callbackForeignKeys := -1

	err = migrationreplay.WithReplayedSnapshot(
		ctx,
		conn,
		fstest.MapFS{
			"1_create_users.sql": {
				Data: []byte("PRAGMA foreign_keys = OFF;\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
			},
		},
		migrationfile.DirFormatAtlas,
		func(replayConn *dbschema.DatabaseConnection) error {
			return replayConn.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&callbackForeignKeys)
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(callbackForeignKeys, qt.Equals, 1)
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
		migrationfile.DirFormatAtlas,
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
		migrationfile.DirFormatAtlas,
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
			migrationfile.DirFormatAtlas,
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
		migrationfile.DirFormatAtlas,
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

// An ObserveServer that refuses stops the replay where it stands: before the
// dev realm is cleaned and before a migration runs.
//
// The caller that needs this resolves a declared server version against the
// product the connection reports, and a version the product does not own is an
// input that was already wrong. Replaying first would destroy and rebuild a
// database to report it (stokaro/ptah#3420).
func TestReplayStopsWhenTheServerObserverRefuses(t *testing.T) {
	c := qt.New(t)
	migrationsDir := t.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "0000000001_users.up.sql"),
		[]byte("CREATE TABLE replay_observer_users (id INTEGER PRIMARY KEY);\n"), 0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "0000000001_users.down.sql"),
		[]byte("DROP TABLE replay_observer_users;\n"), 0o600,
	), qt.IsNil)
	devDBPath := filepath.Join(t.TempDir(), "dev.db")
	refusal := errors.New("the declared version names another product")
	replayed := 0

	err := migrationreplay.Replay(context.Background(), migrationreplay.Options{
		Dir:       migrationsDir,
		DirFormat: migrationfile.DirFormatPtah,
		DevURL:    "sqlite://" + devDBPath,
		ObserveServer: func(catalog.ServerInfo) error {
			return refusal
		},
		ObserveVersion: func(context.Context, *migrator.Migration, *dbschema.DatabaseConnection) error {
			replayed++
			return nil
		},
	})

	c.Assert(err, qt.ErrorIs, refusal)
	c.Assert(replayed, qt.Equals, 0)
	c.Assert(replayedTableExists(c, devDBPath), qt.IsFalse)
}

// replayedTableExists asks the dev database whether the migration ran, which
// is the side effect an aborted replay must not have left.
func replayedTableExists(c *qt.C, devDBPath string) bool {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+devDBPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var count int
	c.Assert(conn.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'replay_observer_users'`,
	).Scan(&count), qt.IsNil)
	return count > 0
}
