package migratebaseline_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/migratebaseline"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/generator"
)

func TestMigrateBaselineCommandCreation(t *testing.T) {
	c := qt.New(t)

	cmd := migratebaseline.NewMigrateBaselineCommand()
	c.Assert(cmd, qt.IsNotNil)
	c.Assert(cmd.Use, qt.Equals, "baseline")
	c.Assert(cmd.Flag("db-url"), qt.IsNotNil)
	c.Assert(cmd.Flag("migrations-dir"), qt.IsNotNil)
	c.Assert(cmd.Flag("version"), qt.IsNotNil)
	c.Assert(cmd.Flag("force"), qt.IsNotNil)
	c.Assert(cmd.Flag("dry-run"), qt.IsNotNil)
	c.Assert(cmd.Flag("shadow-db"), qt.IsNotNil)
	c.Assert(cmd.Flag("dir-format"), qt.IsNotNil)
	c.Assert(cmd.Flag("migration-lock-timeout"), qt.IsNotNil)
}

func TestMigrateBaselineCommand_PreservesStructuredShadowFailure(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	dbURL := "sqlite://" + filepath.Join(dir, "target.db")
	target, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	_, err = target.ExecContext(t.Context(), "CREATE TABLE users (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	dbschema.CloseAndWarn(target)

	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
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

	cmd := migratebaseline.NewMigrateBaselineCommand()
	cmd.SetArgs([]string{
		"--db-url", dbURL,
		"--migrations-dir", migrationsDir,
		"--shadow-db", dbURL,
	})

	err = cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `baseline shadow check failed: shadow database must be distinct from target database`)
	var shadowErr *generator.ShadowVerificationError
	c.Assert(err, qt.ErrorAs, &shadowErr)
	c.Assert(shadowErr.Result.Stage, qt.Equals, "realm-check")
	c.Assert(shadowErr.Result.Mismatches, qt.DeepEquals, []generator.ShadowMismatch{{
		Kind:    "target_shadow_same_realm",
		Message: "shadow database must be distinct from target database",
	}})

	target, err = dbschema.ConnectToDatabase(t.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(target)
	var count int
	err = target.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'").Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, 0)
}
