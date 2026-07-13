//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestMigrateGenerateShadowDatabaseE2E(t *testing.T) {
	dbURL := postgresE2EDatabaseURL(t)
	if dbURL == "" {
		t.Skip("POSTGRES_TEST_DSN or POSTGRES_URL is not set")
	}

	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	repoRoot := e2eRepoRoot(t)
	binaryPath := filepath.Join(t.TempDir(), "package-migrator")
	buildPackageMigrator(c, ctx, repoRoot, binaryPath)

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	testDBName := fmt.Sprintf("ptah_shadow_e2e_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, testDBName)
	defer dropE2EDatabase(c, context.Background(), adminDB, testDBName)

	testDBURL := replaceDatabaseName(c, dbURL, testDBName)

	t.Run("broken hand-edited migration aborts before writing candidate files", func(t *testing.T) {
		c := qt.New(t)
		dir := t.TempDir()
		entitiesDir := writeShadowE2EEntities(c, dir)
		migrationsDir := filepath.Join(dir, "migrations")
		c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
		writeShadowE2EPriorMigration(c, migrationsDir, "CREATE TABLE users (id SERIAL PRIMARY KEY);\n")
		prepareShadowE2ETargetDB(c, ctx, testDBURL)

		output, err := runPackageMigrator(
			ctx,
			repoRoot,
			binaryPath,
			"migrate", "generate",
			"--root-dir", entitiesDir,
			"--db-url", testDBURL,
			"--migrations-dir", migrationsDir,
			"--name", "add_email",
			"--shadow-db", testDBURL,
		)

		c.Assert(err, qt.Not(qt.IsNil))
		c.Assert(output, qt.Contains, "shadow check failed: missing column users.name")
		matches, globErr := filepath.Glob(filepath.Join(migrationsDir, "*.sql"))
		c.Assert(globErr, qt.IsNil)
		c.Assert(matches, qt.HasLen, 2)
	})

	t.Run("correct migration chain writes candidate files", func(t *testing.T) {
		c := qt.New(t)
		dir := t.TempDir()
		entitiesDir := writeShadowE2EEntities(c, dir)
		migrationsDir := filepath.Join(dir, "migrations")
		c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
		writeShadowE2EPriorMigration(c, migrationsDir, "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL);\n")
		prepareShadowE2ETargetDB(c, ctx, testDBURL)

		output, err := runPackageMigrator(
			ctx,
			repoRoot,
			binaryPath,
			"migrate", "generate",
			"--root-dir", entitiesDir,
			"--db-url", testDBURL,
			"--migrations-dir", migrationsDir,
			"--name", "add_email",
			"--shadow-db", testDBURL,
		)

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", output))
		c.Assert(output, qt.Contains, "Generated migration files")
		matches, globErr := filepath.Glob(filepath.Join(migrationsDir, "*.sql"))
		c.Assert(globErr, qt.IsNil)
		c.Assert(matches, qt.HasLen, 4)
		c.Assert(readFirstMatchingFile(c, migrationsDir, "*_add_email.up.sql"), qt.Contains, "email")
	})
}

func postgresE2EDatabaseURL(t *testing.T) string {
	t.Helper()
	for _, name := range []string{"POSTGRES_TEST_DSN", "POSTGRES_URL", "TEST_DATABASE_URL"} {
		if value := os.Getenv(name); value != "" {
			return value
		}
	}
	return ""
}

func e2eRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to resolve test file path")
	}
	return filepath.Dir(filepath.Dir(file))
}

func buildPackageMigrator(c *qt.C, ctx context.Context, repoRoot, binaryPath string) {
	cmd := exec.CommandContext(ctx, "go", "build", "-o", binaryPath, "./cmd")
	cmd.Dir = repoRoot
	output, err := cmd.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("go build output:\n%s", string(output)))
}

func runPackageMigrator(ctx context.Context, repoRoot, binaryPath string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, binaryPath, args...)
	cmd.Dir = repoRoot
	output, err := cmd.CombinedOutput()
	return string(output), err
}

func createE2EDatabase(c *qt.C, ctx context.Context, db *sql.DB, name string) {
	_, err := db.ExecContext(ctx, "CREATE DATABASE "+quoteE2EIdent(name))
	c.Assert(err, qt.IsNil)
}

func dropE2EDatabase(c *qt.C, ctx context.Context, db *sql.DB, name string) {
	_, _ = db.ExecContext(ctx, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()", name)
	_, err := db.ExecContext(ctx, "DROP DATABASE IF EXISTS "+quoteE2EIdent(name))
	c.Assert(err, qt.IsNil)
}

func quoteE2EIdent(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func replaceDatabaseName(c *qt.C, dbURL, databaseName string) string {
	parsed, err := url.Parse(dbURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + databaseName
	return parsed.String()
}

func prepareShadowE2ETargetDB(c *qt.C, ctx context.Context, dbURL string) {
	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS users CASCADE")
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS schema_migrations CASCADE")
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(ctx, "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")
	c.Assert(err, qt.IsNil)
}

func writeShadowE2EEntities(c *qt.C, dir string) string {
	entitiesDir := filepath.Join(dir, "entities")
	c.Assert(os.MkdirAll(entitiesDir, 0755), qt.IsNil)
	content := `package entities

//migrator:schema:table name="users"
type User struct {
	//migrator:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//migrator:schema:field name="name" type="TEXT"
	Name string

	//migrator:schema:field name="email" type="TEXT"
	Email string
}
`
	c.Assert(os.WriteFile(filepath.Join(entitiesDir, "schema.go"), []byte(content), 0600), qt.IsNil)
	return entitiesDir
}

func writeShadowE2EPriorMigration(c *qt.C, dir, upSQL string) {
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"), []byte(upSQL), 0600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"), []byte("DROP TABLE IF EXISTS users;\n"), 0600), qt.IsNil)
}

func readFirstMatchingFile(c *qt.C, dir, pattern string) string {
	matches, err := filepath.Glob(filepath.Join(dir, pattern))
	c.Assert(err, qt.IsNil)
	c.Assert(matches, qt.Not(qt.HasLen), 0)
	content, err := os.ReadFile(matches[0])
	c.Assert(err, qt.IsNil)
	return string(content)
}
