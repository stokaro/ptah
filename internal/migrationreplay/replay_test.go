package migrationreplay_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

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
