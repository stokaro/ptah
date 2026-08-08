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
)

func TestVerifyBaselineShadow_ReplayErrorWithRealPostgres(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	dbURL := requireBaselineShadowPostgresURL(t)
	target, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(target)
	c.Assert(platform.NormalizeDialect(target.Info().Dialect), qt.Equals, platform.Postgres)
	shadowURL, shadowDatabase := createBaselineShadowPostgres(c, target, dbURL)
	defer dropBaselineShadowPostgres(c, target, shadowDatabase)

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

func requireBaselineShadowPostgresURL(t *testing.T) string {
	t.Helper()
	for _, name := range []string{"POSTGRES_TEST_DSN", "POSTGRES_URL", "TEST_DATABASE_URL", "TEST_DB_URL"} {
		if value := os.Getenv(name); value != "" {
			return value
		}
	}
	t.Skip("POSTGRES_TEST_DSN, POSTGRES_URL, TEST_DATABASE_URL, or TEST_DB_URL is not set")
	return ""
}

func createBaselineShadowPostgres(
	c *qt.C,
	admin *dbschema.DatabaseConnection,
	baseURL string,
) (shadowURL, database string) {
	c.Helper()
	database = fmt.Sprintf("ptah_baseline_shadow_%d", time.Now().UnixNano())
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

func dropBaselineShadowPostgres(c *qt.C, admin *dbschema.DatabaseConnection, database string) {
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
