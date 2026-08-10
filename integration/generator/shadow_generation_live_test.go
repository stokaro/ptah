//go:build integration

package generator_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/generator"
)

func TestGenerateMigrationShadowVerificationWithRealDB(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	dbURL, conn := openShadowTestPostgres(c)
	defer dbschema.CloseAndWarn(conn)
	shadowURL, shadowDatabase := createShadowTestPostgres(c, conn, dbURL)
	defer dropShadowTestPostgres(c, conn, shadowDatabase)
	releaseLock := acquireShadowTestLock(c, ctx, conn)
	defer releaseLock()
	defer func() {
		c.Assert(conn.SchemaWriter().DropAllTables(ctx), qt.IsNil)
	}()

	c.Run("broken prior migration aborts with missing column", func(c *qt.C) {
		dir := c.TempDir()
		entitiesDir := writeShadowEntities(c, dir)
		migrationsDir := filepath.Join(dir, "migrations")
		c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
		writePriorMigration(c, migrationsDir, "CREATE TABLE users (id SERIAL PRIMARY KEY);\n")

		prepareShadowTargetDB(c, ctx, conn)

		files, err := generator.GenerateMigration(ctx, generator.GenerateMigrationOptions{
			GoEntitiesDir:     entitiesDir,
			DatabaseURL:       dbURL,
			MigrationName:     "add_email",
			OutputDir:         migrationsDir,
			ShadowDatabaseURL: shadowURL,
		})

		c.Assert(files, qt.IsNil)
		c.Assert(err.Error(), qt.Contains, "shadow check failed: missing column users.name: ")
		var shadowErr *generator.ShadowVerificationError
		c.Assert(err, qt.ErrorAs, &shadowErr)
		c.Assert(shadowErr.Result.Stage, qt.Equals, "replay")
		c.Assert(shadowErr.Result.Mismatches, qt.HasLen, 1)
		c.Assert(shadowErr.Result.Mismatches[0].Kind, qt.Equals, "replay_error")
		c.Assert(shadowErr.Err, qt.IsNotNil)
		matches, globErr := filepath.Glob(filepath.Join(migrationsDir, "*.sql"))
		c.Assert(globErr, qt.IsNil)
		c.Assert(matches, qt.HasLen, 2)
	})

	c.Run("correct prior migration passes and writes files", func(c *qt.C) {
		dir := c.TempDir()
		entitiesDir := writeShadowEntities(c, dir)
		migrationsDir := filepath.Join(dir, "migrations")
		c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
		writePriorMigration(c, migrationsDir, "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL);\n")

		prepareShadowTargetDB(c, ctx, conn)

		files, err := generator.GenerateMigration(ctx, generator.GenerateMigrationOptions{
			GoEntitiesDir:     entitiesDir,
			DatabaseURL:       dbURL,
			MigrationName:     "add_email",
			OutputDir:         migrationsDir,
			ShadowDatabaseURL: shadowURL,
		})

		c.Assert(err, qt.IsNil)
		c.Assert(files, qt.IsNotNil)
		c.Assert(files.Files, qt.HasLen, 1)
		c.Assert(files.Files[0].UpFile, qt.Not(qt.Equals), "")
		c.Assert(files.Files[0].DownFile, qt.Not(qt.Equals), "")
		upSQL, readErr := os.ReadFile(files.Files[0].UpFile)
		c.Assert(readErr, qt.IsNil)
		c.Assert(string(upSQL), qt.Contains, "email")
	})
}
