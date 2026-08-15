//go:build integration

package migratebaseline_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/migratebaseline"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/generator"
)

func TestVerifyBaselineShadowPostgresMismatchRequiresForce(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	dbURL, conn := requirePostgresBaselineTestConnection(t, c, ctx, "baseline shadow verification test")
	defer dbschema.CloseAndWarn(conn)

	suffix := time.Now().UnixNano()
	schema := fmt.Sprintf("ptah_issue_269_shadow_%d", suffix)
	shadowDBName := fmt.Sprintf("ptah_issue_269_shadow_db_%d", suffix)
	shadowDBURL := baselineShadowDatabaseURL(c, dbURL, shadowDBName)
	createBaselineShadowDatabase(c, ctx, conn, shadowDBName)
	defer dropBaselineShadowDatabase(c, ctx, conn, shadowDBName)
	defer func() {
		_, _ = conn.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+quotePostgresIdent(schema)+" CASCADE")
	}()

	_, err := conn.ExecContext(ctx, "CREATE SCHEMA "+quotePostgresIdent(schema))
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s.%s (id INTEGER PRIMARY KEY)",
		quotePostgresIdent(schema),
		quotePostgresIdent("users"),
	))
	c.Assert(err, qt.IsNil)

	migrationsDir := c.TempDir()
	writeBaselineShadowMigration(c, migrationsDir, schema)
	info := conn.Info()
	err = generator.VerifyBaselineShadow(ctx, generator.BaselineShadowVerifyOptions{
		ShadowDatabaseURL: shadowDBURL,
		TargetConn:        conn,
		MigrationsDir:     migrationsDir,
		Version:           1,
		Dialect:           info.Dialect,
		Capabilities:      info.Capabilities,
		Schemas:           []string{schema},
	})
	c.Assert(err, qt.ErrorMatches, `baseline shadow check failed: .*`)

	metadataTable := fmt.Sprintf("schema_migrations_issue_269_force_%d", suffix)
	defer func() {
		_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+quotePostgresIdent(metadataTable))
	}()
	cmd := migratebaseline.NewMigrateBaselineCommand()
	cmd.SetArgs([]string{
		"--db-url", dbURL,
		"--migrations-dir", migrationsDir,
		"--shadow-db", shadowDBURL,
		"--schemas", schema,
		"--migrations-table", metadataTable,
		"--force",
	})
	err = cmd.Execute()
	c.Assert(err, qt.IsNil)
}

func TestVerifyBaselineShadowPostgresMatchIgnoresShadowMetadata(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	dbURL, adminConn := requirePostgresBaselineTestConnection(t, c, ctx, "baseline shadow verification test")
	defer dbschema.CloseAndWarn(adminConn)

	suffix := time.Now().UnixNano()
	targetDBName := fmt.Sprintf("ptah_issue_269_target_db_%d", suffix)
	shadowDBName := fmt.Sprintf("ptah_issue_269_shadow_db_%d", suffix)
	targetDBURL := baselineShadowDatabaseURL(c, dbURL, targetDBName)
	shadowDBURL := baselineShadowDatabaseURL(c, dbURL, shadowDBName)
	createBaselineShadowDatabase(c, ctx, adminConn, targetDBName)
	createBaselineShadowDatabase(c, ctx, adminConn, shadowDBName)
	defer dropBaselineShadowDatabase(c, ctx, adminConn, targetDBName)
	defer dropBaselineShadowDatabase(c, ctx, adminConn, shadowDBName)

	targetConn, err := dbschema.ConnectToDatabase(ctx, targetDBURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(targetConn)
	_, err = targetConn.ExecContext(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
	c.Assert(err, qt.IsNil)

	migrationsDir := c.TempDir()
	writeBaselineShadowPublicMigration(c, migrationsDir)
	info := targetConn.Info()
	err = generator.VerifyBaselineShadow(ctx, generator.BaselineShadowVerifyOptions{
		ShadowDatabaseURL: shadowDBURL,
		TargetConn:        targetConn,
		MigrationsDir:     migrationsDir,
		Version:           1,
		Dialect:           info.Dialect,
		Capabilities:      info.Capabilities,
	})
	c.Assert(err, qt.IsNil)
}

func TestMigrateBaselineCommandPostgresWritesMetadataWithoutExecutingDDL(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()

	dbURL, adminConn := requirePostgresBaselineTestConnection(t, c, ctx, "migrate-baseline command test")
	defer dbschema.CloseAndWarn(adminConn)

	suffix := time.Now().UnixNano()
	targetDBName := fmt.Sprintf("ptah_issue_269_cli_target_%d", suffix)
	shadowDBName := fmt.Sprintf("ptah_issue_269_cli_shadow_%d", suffix)
	targetDBURL := baselineShadowDatabaseURL(c, dbURL, targetDBName)
	shadowDBURL := baselineShadowDatabaseURL(c, dbURL, shadowDBName)
	createBaselineShadowDatabase(c, ctx, adminConn, targetDBName)
	createBaselineShadowDatabase(c, ctx, adminConn, shadowDBName)
	defer dropBaselineShadowDatabase(c, ctx, adminConn, targetDBName)
	defer dropBaselineShadowDatabase(c, ctx, adminConn, shadowDBName)

	targetConn, err := dbschema.ConnectToDatabase(ctx, targetDBURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(targetConn)
	_, err = targetConn.ExecContext(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
	c.Assert(err, qt.IsNil)

	migrationsDir := c.TempDir()
	writeBaselineShadowPublicMigration(c, migrationsDir)
	metadataTable := fmt.Sprintf("schema_migrations_issue_269_cli_%d", suffix)
	cmd := migratebaseline.NewMigrateBaselineCommand()
	cmd.SetArgs([]string{
		"--db-url", targetDBURL,
		"--migrations-dir", migrationsDir,
		"--shadow-db", shadowDBURL,
		"--migrations-table", metadataTable,
		"--connect-timeout", "5s",
	})

	err = cmd.Execute()
	c.Assert(err, qt.IsNil)

	var version int64
	var state string
	err = targetConn.QueryRowContext(
		ctx,
		fmt.Sprintf("SELECT version, state FROM %s", quotePostgresIdent(metadataTable)),
	).Scan(&version, &state)
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(1))
	c.Assert(state, qt.Equals, "applied")
}

func postgresBaselineTestURL() string {
	for _, name := range []string{"POSTGRES_TEST_DSN", "TEST_DATABASE_URL", "POSTGRES_URL"} {
		if value := os.Getenv(name); value != "" {
			return value
		}
	}
	return ""
}

func requirePostgresBaselineTestConnection(
	t *testing.T,
	c *qt.C,
	ctx context.Context,
	purpose string,
) (string, *dbschema.DatabaseConnection) {
	t.Helper()

	dbURL := postgresBaselineTestURL()
	if dbURL == "" {
		t.Skip("PostgreSQL test database URL is not set")
	}

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	if err != nil {
		t.Skipf("test database is not available: %v", err)
	}
	if conn.Info().Dialect != "postgres" {
		dbschema.CloseAndWarn(conn)
		t.Skipf("%s requires PostgreSQL, got %s", purpose, conn.Info().Dialect)
	}

	c.Assert(conn, qt.IsNotNil)
	return dbURL, conn
}

func baselineShadowDatabaseURL(c *qt.C, dbURL, dbName string) string {
	c.Helper()

	parsed, err := url.Parse(dbURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + dbName
	return parsed.String()
}

func createBaselineShadowDatabase(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection, dbName string) {
	c.Helper()

	dropBaselineShadowDatabase(c, ctx, conn, dbName)
	_, err := conn.ExecContext(ctx, "CREATE DATABASE "+quotePostgresIdent(dbName))
	c.Assert(err, qt.IsNil)
}

func dropBaselineShadowDatabase(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection, dbName string) {
	c.Helper()

	_, err := conn.ExecContext(ctx, "DROP DATABASE IF EXISTS "+quotePostgresIdent(dbName)+" WITH (FORCE)")
	c.Assert(err, qt.IsNil)
}

func writeBaselineShadowMigration(c *qt.C, dir, schema string) {
	c.Helper()

	upSQL := fmt.Sprintf(
		"CREATE SCHEMA IF NOT EXISTS %s;\nCREATE TABLE %s.%s (id INTEGER PRIMARY KEY, name TEXT NOT NULL);\n",
		quotePostgresIdent(schema),
		quotePostgresIdent(schema),
		quotePostgresIdent("users"),
	)
	downSQL := fmt.Sprintf(
		"DROP TABLE IF EXISTS %s.%s;\nDROP SCHEMA IF EXISTS %s;\n",
		quotePostgresIdent(schema),
		quotePostgresIdent("users"),
		quotePostgresIdent(schema),
	)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"), []byte(upSQL), 0600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"), []byte(downSQL), 0600), qt.IsNil)
}

func writeBaselineShadowPublicMigration(c *qt.C, dir string) {
	c.Helper()

	upSQL := "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);\n"
	downSQL := "DROP TABLE IF EXISTS users;\n"
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"), []byte(upSQL), 0600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"), []byte(downSQL), 0600), qt.IsNil)
}

func quotePostgresIdent(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
