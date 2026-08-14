//go:build integration

package generator_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/generator"
)

func TestGenerateCheckpointFromShadowRejectsMalformedSQLiteToggleBeforeMutation(t *testing.T) {
	c := qt.New(t)
	shadowURL := "sqlite://" + filepath.Join(t.TempDir(), "shadow.db")
	shadow, err := dbschema.ConnectToDatabase(t.Context(), shadowURL)
	c.Assert(err, qt.IsNil)
	_, err = shadow.ExecContext(t.Context(), "CREATE TABLE preserve_before_checkpoint (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	dbschema.CloseAndWarn(shadow)

	migrationsDir := t.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "0000000001_init.up.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "0000000001_init.down.sql"),
		[]byte("DROP TABLE users;\n"),
		0o600,
	), qt.IsNil)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")

	_, _, err = generator.GenerateCheckpointFromShadow(t.Context(), generator.CheckpointFromShadowOptions{
		ShadowDatabaseURL: shadowURL,
		MigrationsDir:     migrationsDir,
		Dialect:           "sqlite",
	})

	c.Assert(err, qt.ErrorMatches,
		`checkpoint generation failed: validate SQLite virtual-table drop toggle: invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`)
	shadow, err = dbschema.ConnectToDatabase(t.Context(), shadowURL)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { dbschema.CloseAndWarn(shadow) })
	var preserved int
	err = shadow.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'preserve_before_checkpoint'",
	).Scan(&preserved)
	c.Assert(err, qt.IsNil)
	c.Assert(preserved, qt.Equals, 1)
}

func TestGenerateCheckpointFromShadowRejectsMalformedSQLiteToggleBeforeConnect(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")

	_, _, err := generator.GenerateCheckpointFromShadow(t.Context(), generator.CheckpointFromShadowOptions{
		ShadowDatabaseURL: "sqlite://" + filepath.Join(t.TempDir(), "missing", "shadow.db"),
		MigrationsDir:     t.TempDir(),
		Dialect:           "sqlite",
	})

	c.Assert(err, qt.ErrorMatches,
		`checkpoint generation failed: validate SQLite virtual-table drop toggle: invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "connect to shadow database")
}

func TestGenerateCheckpointFromShadowDoesNotApplySQLiteToggleToPostgres(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")

	_, _, err := generator.GenerateCheckpointFromShadow(t.Context(), generator.CheckpointFromShadowOptions{
		ShadowDatabaseURL: "postgres://localhost/database",
		MigrationsDir:     t.TempDir(),
		Dialect:           "postgres",
		ConnectTimeout:    time.Nanosecond,
	})

	c.Assert(err, qt.ErrorMatches, `checkpoint generation failed: connect to shadow database: .*`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP")
}
