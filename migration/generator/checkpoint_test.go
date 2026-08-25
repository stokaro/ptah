package generator_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/migrationsnapshot"
	"go.5x5.cz/ptah/migration/generator"
)

func TestGenerateCheckpointFromShadow_ReplaysHistoryIntoCumulativeSnapshot(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	writeMigration := func(name, up, down string) {
		c.Assert(os.WriteFile(filepath.Join(dir, name+".up.sql"), []byte(up), 0o600), qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(dir, name+".down.sql"), []byte(down), 0o600), qt.IsNil)
	}
	writeMigration("0000000001_init",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\n", "DROP TABLE users;\n")
	writeMigration("0000000002_add_email",
		"ALTER TABLE users ADD COLUMN email TEXT;\n", "ALTER TABLE users DROP COLUMN email;\n")

	shadowURL := "sqlite://" + filepath.Join(t.TempDir(), "shadow.db")

	up, down, err := generator.GenerateCheckpointFromShadow(context.Background(), generator.CheckpointFromShadowOptions{
		ShadowDatabaseURL: shadowURL,
		MigrationsDir:     dir,
		Dialect:           "sqlite",
	})
	c.Assert(err, qt.IsNil)

	// The checkpoint is the cumulative schema: one CREATE TABLE users carrying
	// both the original id column and the column added by the second migration.
	c.Assert(up, qt.Contains, "CREATE TABLE")
	c.Assert(up, qt.Contains, "users")
	c.Assert(up, qt.Contains, "email")
	c.Assert(down, qt.Contains, "DROP TABLE")
	c.Assert(down, qt.Contains, "users")
}

func TestGenerateCheckpointFromShadow_UsesProvidedSnapshotInsteadOfPath(t *testing.T) {
	c := qt.New(t)
	reopenedDir := t.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(reopenedDir, "0000000001_changed.up.sql"),
		[]byte("CREATE TABLE changed_after_verification (id INTEGER PRIMARY KEY);"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(reopenedDir, "0000000001_changed.down.sql"),
		[]byte("DROP TABLE changed_after_verification;"),
		0o600,
	), qt.IsNil)
	authorized := fstest.MapFS{
		"0000000001_authorized.up.sql": {Data: []byte(
			"CREATE TABLE authorized_snapshot (id INTEGER PRIMARY KEY);",
		)},
		"0000000001_authorized.down.sql": {Data: []byte(
			"DROP TABLE authorized_snapshot;",
		)},
	}

	up, _, err := generator.GenerateCheckpointFromShadow(t.Context(), generator.CheckpointFromShadowOptions{
		ShadowDatabaseURL: "sqlite://" + filepath.Join(t.TempDir(), "shadow.db"),
		MigrationsDir:     reopenedDir,
		MigrationsFS:      authorized,
		Dialect:           "sqlite",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(up, qt.Contains, "authorized_snapshot")
	c.Assert(up, qt.Not(qt.Contains), "changed_after_verification")
}

func TestGenerateCheckpointFromShadow_EmptyDirectoryErrors(t *testing.T) {
	c := qt.New(t)

	shadowURL := "sqlite://" + filepath.Join(t.TempDir(), "shadow.db")
	_, _, err := generator.GenerateCheckpointFromShadow(context.Background(), generator.CheckpointFromShadowOptions{
		ShadowDatabaseURL: shadowURL,
		MigrationsDir:     t.TempDir(),
		Dialect:           "sqlite",
	})
	c.Assert(err, qt.ErrorMatches, `.*no migrations found.*`)
}

func TestWriteCheckpointFilesWithOptions_RefusesChangedAuthorizedHistory(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	prior := filepath.Join(dir, "0000000001_init.up.sql")
	c.Assert(os.WriteFile(prior, []byte("CREATE TABLE original (id INT);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, "0000000001_init.down.sql"),
		[]byte("DROP TABLE original;\n"),
		0o600,
	), qt.IsNil)
	authorized, err := migrationsnapshot.CaptureDirectory(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(prior, []byte("CREATE TABLE tampered (id INT);\n"), 0o600), qt.IsNil)

	_, _, err = generator.WriteCheckpointFilesWithOptions(
		dir,
		2,
		"snapshot",
		"CREATE TABLE original (id INT);\n",
		"DROP TABLE original;\n",
		generator.CheckpointWriteOptions{AuthorizedMigrationsFS: authorized},
	)

	c.Assert(err, qt.ErrorIs, generator.ErrMigrationDirectoryChanged)
	matches, globErr := filepath.Glob(filepath.Join(dir, "*.checkpoint.*.sql"))
	c.Assert(globErr, qt.IsNil)
	c.Assert(matches, qt.HasLen, 0)
	_, statErr := os.Stat(filepath.Join(dir, "ptah.sum"))
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}
