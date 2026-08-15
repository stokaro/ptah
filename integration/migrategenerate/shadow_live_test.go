//go:build integration

package migrate_test

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/migrate"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/internal/sqlident"
	"go.5x5.cz/ptah/migration/generator"
)

func TestMigrateGenerateShadowVerificationWithRealDB(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	dbURL, conn := requireMigrateGeneratePostgresTestConnection(t, ctx)
	defer dbschema.CloseAndWarn(conn)
	shadowURL, shadowDatabase := createMigrateGenerateShadowDatabase(c, ctx, conn, dbURL)
	defer dropMigrateGenerateShadowDatabase(c, conn, shadowDatabase)
	releaseLock := acquireMigrateGenerateTestLock(c, ctx, conn)
	defer releaseLock()
	defer func() {
		c.Assert(conn.SchemaWriter().DropAllTables(ctx), qt.IsNil)
	}()

	t.Run("broken prior migration aborts before writing candidate files", func(t *testing.T) {
		c := qt.New(t)
		dir := c.TempDir()
		entitiesDir := writeMigrateGenerateShadowEntities(c, dir)
		migrationsDir := filepath.Join(dir, "migrations")
		c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
		writeMigrateGeneratePriorMigration(c, migrationsDir, "CREATE TABLE users (id SERIAL PRIMARY KEY);\n")
		prepareMigrateGenerateTargetDB(c, ctx, conn)

		var out bytes.Buffer
		cmd := migrate.NewMigrateGenerateCommand()
		cmd.SetOut(&out)
		cmd.SetArgs([]string{
			"--root-dir", entitiesDir,
			"--db-url", dbURL,
			"--migrations-dir", migrationsDir,
			"--name", "add_email",
			"--shadow-db", shadowURL,
		})
		err := cmd.Execute()

		c.Assert(err, qt.IsNotNil)
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

	t.Run("correct prior migration writes candidate files", func(t *testing.T) {
		c := qt.New(t)
		dir := c.TempDir()
		entitiesDir := writeMigrateGenerateShadowEntities(c, dir)
		migrationsDir := filepath.Join(dir, "migrations")
		c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
		writeMigrateGeneratePriorMigration(c, migrationsDir, "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL);\n")
		prepareMigrateGenerateTargetDB(c, ctx, conn)

		var out bytes.Buffer
		cmd := migrate.NewMigrateGenerateCommand()
		cmd.SetOut(&out)
		cmd.SetArgs([]string{
			"--root-dir", entitiesDir,
			"--db-url", dbURL,
			"--migrations-dir", migrationsDir,
			"--name", "add_email",
			"--shadow-db", shadowURL,
		})
		c.Assert(cmd.Execute(), qt.IsNil)
		c.Assert(out.String(), qt.Contains, "Generated migration files")
		matches, globErr := filepath.Glob(filepath.Join(migrationsDir, "*.sql"))
		c.Assert(globErr, qt.IsNil)
		c.Assert(matches, qt.HasLen, 4)
	})
}

func createMigrateGenerateShadowDatabase(
	c *qt.C,
	ctx context.Context,
	admin *dbschema.DatabaseConnection,
	baseURL string,
) (shadowURL, database string) {
	c.Helper()
	database = fmt.Sprintf("ptah_migrate_generate_shadow_%d", time.Now().UnixNano())
	_, err := admin.ExecContext(ctx, "CREATE DATABASE "+sqlident.Quote(platform.Postgres, database))
	c.Assert(err, qt.IsNil)
	parsed, err := url.Parse(baseURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + database
	parsed.RawPath = ""
	return parsed.String(), database
}

func dropMigrateGenerateShadowDatabase(
	c *qt.C,
	admin *dbschema.DatabaseConnection,
	database string,
) {
	c.Helper()
	_, _ = admin.ExecContext(
		context.Background(),
		"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
		database,
	)
	_, err := admin.ExecContext(
		context.Background(),
		"DROP DATABASE IF EXISTS "+sqlident.Quote(platform.Postgres, database),
	)
	c.Assert(err, qt.IsNil)
}

func requireMigrateGeneratePostgresTestConnection(
	t *testing.T,
	ctx context.Context,
) (string, *dbschema.DatabaseConnection) {
	c := qt.New(t)
	t.Helper()
	dbURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	c.Assert(platform.NormalizeDialect(conn.Info().Dialect), qt.Equals, platform.Postgres)
	return dbURL, conn
}

func acquireMigrateGenerateTestLock(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) func() {
	_, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock(156156156)")
	c.Assert(err, qt.IsNil)
	return func() {
		_, unlockErr := conn.ExecContext(ctx, "SELECT pg_advisory_unlock(156156156)")
		c.Assert(unlockErr, qt.IsNil)
	}
}

func writeMigrateGenerateShadowEntities(c *qt.C, dir string) string {
	entitiesDir := filepath.Join(dir, "entities")
	c.Assert(os.MkdirAll(entitiesDir, 0o755), qt.IsNil)
	content := `package entities

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="name" type="TEXT"
	Name string

	//ptah:schema:field name="email" type="TEXT"
	Email string
}
`
	c.Assert(os.WriteFile(filepath.Join(entitiesDir, "schema.go"), []byte(content), 0o600), qt.IsNil)
	return entitiesDir
}

func writeMigrateGeneratePriorMigration(c *qt.C, dir, upSQL string) {
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"), []byte(upSQL), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"), []byte("DROP TABLE IF EXISTS users;\n"), 0o600), qt.IsNil)
}

func prepareMigrateGenerateTargetDB(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) {
	c.Assert(conn.SchemaWriter().DropAllTables(ctx), qt.IsNil)
	_, err := conn.ExecContext(ctx, "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")
	c.Assert(err, qt.IsNil)
}
