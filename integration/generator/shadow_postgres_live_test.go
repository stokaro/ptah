//go:build integration

package generator_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/sqlident"
	"go.5x5.cz/ptah/migration/generator"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestVerifyBaselineShadow_ReplayErrorWithRealPostgres(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	dbURL := requireGeneratorPostgresURL(t)
	target, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(target)
	c.Assert(platform.NormalizeDialect(target.Info().Dialect), qt.Equals, platform.Postgres)
	shadowURL, shadowDatabase := createGeneratorTestPostgres(c, target, dbURL, "ptah_baseline_shadow")
	defer dropGeneratorTestPostgres(c, target, shadowDatabase)

	migrationsDir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "0000000001_init.up.sql"),
		[]byte("CREATE TABLE users (id SERIAL PRIMARY KEY);\nALTER TABLE users DROP COLUMN name;\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "0000000001_init.down.sql"),
		[]byte("DROP TABLE IF EXISTS users;\n"),
		0o600,
	), qt.IsNil)

	err = generator.VerifyBaselineShadow(ctx, generator.BaselineShadowVerifyOptions{
		ShadowDatabaseURL: shadowURL,
		TargetConn:        target,
		MigrationsDir:     migrationsDir,
		Version:           1,
		Dialect:           platform.Postgres,
		Capabilities:      target.Info().Capabilities,
	})

	c.Assert(err, qt.ErrorMatches, `baseline shadow check failed: missing column users\.name`)
	var shadowErr *generator.ShadowVerificationError
	c.Assert(err, qt.ErrorAs, &shadowErr)
	c.Assert(shadowErr.Result, qt.DeepEquals, generator.ShadowVerificationResult{
		Stage: "replay",
		Mismatches: []generator.ShadowMismatch{
			{
				Kind:    "replay_error",
				Message: "missing column users.name",
			},
		},
	})
	c.Assert(shadowErr.Err, qt.IsNotNil)
	c.Assert(shadowErr.Err.Error(), qt.Contains, `column "name" of relation "users" does not exist`)
	c.Assert(err, qt.ErrorIs, shadowErr.Err)
}

func TestGenerateMigration_ConcurrentIndexApplyAndRollbackWithRealPostgres(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	adminURL := requireGeneratorPostgresURL(t)
	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(admin)
	c.Assert(platform.NormalizeDialect(admin.Info().Dialect), qt.Equals, platform.Postgres)
	targetURL, targetDatabase := createGeneratorTestPostgres(c, admin, adminURL, "ptah_generator_concurrent")
	defer dropGeneratorTestPostgres(c, admin, targetDatabase)
	target, err := dbschema.ConnectToDatabase(ctx, targetURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(target)

	_, err = target.ExecContext(ctx, `
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL
		);
		INSERT INTO users (name, email) VALUES ('Ada', 'ada@example.com');
		ANALYZE users;
	`)
	c.Assert(err, qt.IsNil)

	dir := t.TempDir()
	entitiesDir := writeConcurrentIndexEntities(c, dir)
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)

	files, err := generator.GenerateMigration(ctx, generator.GenerateMigrationOptions{
		GoEntitiesDir: entitiesDir,
		DatabaseURL:   targetURL,
		MigrationName: "add_users_email_index",
		OutputDir:     migrationsDir,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.IsNotNil)
	c.Assert(files.Files, qt.HasLen, 2)
	c.Assert(files.Files[0].NoTransaction, qt.IsFalse)
	c.Assert(files.Files[1].NoTransaction, qt.IsTrue)

	concurrentPair := files.Files[1]
	upSQL, err := os.ReadFile(concurrentPair.UpFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(upSQL), qt.Contains, "-- +ptah no_transaction")
	c.Assert(string(upSQL), qt.Contains, `CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_users_email" ON "users" ("email");`)
	downSQL, err := os.ReadFile(concurrentPair.DownFile)
	c.Assert(err, qt.IsNil)
	c.Assert(string(downSQL), qt.Contains, "-- +ptah no_transaction")
	c.Assert(string(downSQL), qt.Contains, `DROP INDEX CONCURRENTLY IF EXISTS "idx_users_email";`)

	provider, err := migrator.NewFSMigrationProvider(os.DirFS(migrationsDir))
	c.Assert(err, qt.IsNil)
	migrations := migrator.NewMigrator(target, provider)
	c.Assert(migrations.MigrateUp(ctx), qt.IsNil)
	exists, valid := readGeneratorPostgresIndexState(c, target, "idx_users_email")
	c.Assert(exists, qt.IsTrue)
	c.Assert(valid, qt.IsTrue)

	c.Assert(migrations.MigrateDown(ctx), qt.IsNil)
	exists, _ = readGeneratorPostgresIndexState(c, target, "idx_users_email")
	c.Assert(exists, qt.IsFalse)
}

func requireGeneratorPostgresURL(t *testing.T) string {
	t.Helper()
	for _, name := range []string{"POSTGRES_TEST_DSN", "POSTGRES_URL", "TEST_DATABASE_URL", "TEST_DB_URL"} {
		if value := os.Getenv(name); value != "" {
			return value
		}
	}
	t.Skip("POSTGRES_TEST_DSN, POSTGRES_URL, TEST_DATABASE_URL, or TEST_DB_URL is not set")
	return ""
}

func createGeneratorTestPostgres(
	c *qt.C,
	admin *dbschema.DatabaseConnection,
	baseURL string,
	prefix string,
) (shadowURL, database string) {
	c.Helper()
	database = fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	_, err := admin.ExecContext(
		c.Context(),
		"CREATE DATABASE "+sqlident.Quote(platform.Postgres, database),
	)
	c.Assert(err, qt.IsNil)
	parsed, err := url.Parse(baseURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + database
	parsed.RawPath = ""
	return parsed.String(), database
}

func dropGeneratorTestPostgres(c *qt.C, admin *dbschema.DatabaseConnection, database string) {
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

func writeConcurrentIndexEntities(c *qt.C, dir string) string {
	c.Helper()
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
	//ptah:schema:index name="idx_users_email" fields="email"
	Email string
}
`
	c.Assert(os.WriteFile(filepath.Join(entitiesDir, "schema.go"), []byte(content), 0o600), qt.IsNil)
	return entitiesDir
}

func readGeneratorPostgresIndexState(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	indexName string,
) (exists, valid bool) {
	c.Helper()
	err := conn.QueryRowContext(c.Context(), `
		SELECT COUNT(*) = 1,
		       COALESCE(BOOL_AND(index.indisvalid AND index.indisready), false)
		FROM pg_index AS index
		JOIN pg_class AS class ON class.oid = index.indexrelid
		JOIN pg_namespace AS namespace ON namespace.oid = class.relnamespace
		WHERE namespace.nspname = current_schema()
		  AND class.relname = $1
	`, indexName).Scan(&exists, &valid)
	c.Assert(err, qt.IsNil)
	return exists, valid
}
