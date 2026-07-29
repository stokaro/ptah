package migrationreplay_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/internal/migrationreplay"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestReplayRejectsDockerDevURL(t *testing.T) {
	c := qt.New(t)

	err := migrationreplay.Replay(context.Background(), migrationreplay.Options{
		DevURL: "docker://postgres/16/dev",
	})

	c.Assert(err, qt.ErrorMatches, "docker --dev-url values are accepted by Atlas, but Ptah requires a directly connectable dev database URL for migration SQL replay")
}

func TestReplayCleansDevDatabaseAndIgnoresExistingRevisionRows(t *testing.T) {
	c := qt.New(t)
	migrationsDir := t.TempDir()
	devDBPath := filepath.Join(t.TempDir(), "dev.db")
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "1_create_replay_runs.sql"),
		[]byte("CREATE TABLE replay_runs (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

	err := migrationreplay.Replay(context.Background(), migrationreplay.Options{
		Dir:       migrationsDir,
		DirFormat: migrator.MigrationDirFormatAtlas,
		DevURL:    "sqlite://" + devDBPath,
	})
	c.Assert(err, qt.IsNil)

	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+devDBPath)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(context.Background(), "INSERT INTO replay_runs (id) VALUES (1)")
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(context.Background(), "CREATE TABLE schema_migrations (version BIGINT NOT NULL PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(context.Background(), "INSERT INTO schema_migrations (version) VALUES (1)")
	c.Assert(err, qt.IsNil)
	dbschema.CloseAndWarn(conn)

	err = migrationreplay.Replay(context.Background(), migrationreplay.Options{
		Dir:       migrationsDir,
		DirFormat: migrator.MigrationDirFormatAtlas,
		DevURL:    "sqlite://" + devDBPath,
	})
	c.Assert(err, qt.IsNil)
	assertReplayRunsRows(c, devDBPath, 0)
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
	assertReplayRunsRows(c, devDBPath, 0)
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
	err = conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM sentinel").Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 0)
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

func assertReplayRunsRows(c *qt.C, dbPath string, want int) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	var count int
	err = conn.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM replay_runs").Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, want)
}
