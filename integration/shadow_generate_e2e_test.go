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

	"go.5x5.cz/ptah/internal/dbtarget"
)

func TestMigrateGenerateShadowDatabaseE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)

	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	repoRoot := e2eRepoRoot(t)
	binaryPath := filepath.Join(t.TempDir(), "ptah")
	buildPtah(c.TB, ctx, repoRoot, binaryPath)

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	testID := time.Now().UnixNano()
	targetDBName := fmt.Sprintf("ptah_shadow_target_e2e_%d", testID)
	shadowDBName := fmt.Sprintf("ptah_shadow_replay_e2e_%d", testID)
	createE2EDatabase(c.TB, ctx, adminDB, targetDBName)
	defer dropE2EDatabase(c.TB, context.Background(), adminDB, targetDBName)
	createE2EDatabase(c.TB, ctx, adminDB, shadowDBName)
	defer dropE2EDatabase(c.TB, context.Background(), adminDB, shadowDBName)

	targetDBURL := replaceDatabaseName(c.TB, dbURL, targetDBName)
	shadowDBURL := replaceDatabaseName(c.TB, dbURL, shadowDBName)

	t.Run("broken hand-edited migration aborts before writing candidate files", func(t *testing.T) {
		c := qt.New(t)
		dir := t.TempDir()
		entitiesDir := writeShadowE2EEntities(c.TB, dir)
		migrationsDir := filepath.Join(dir, "migrations")
		c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
		writeShadowE2EPriorMigration(c.TB, migrationsDir, "CREATE TABLE users (id SERIAL PRIMARY KEY);\n")
		prepareShadowE2ETargetDB(c.TB, ctx, targetDBURL)

		output, err := runPtah(
			ctx,
			repoRoot,
			binaryPath,
			"migrations", "generate",
			"--root-dir", entitiesDir,
			"--db-url", targetDBURL,
			"--migrations-dir", migrationsDir,
			"--name", "add_email",
			"--shadow-db", shadowDBURL,
		)

		c.Assert(err, qt.IsNotNil)
		c.Assert(output, qt.Contains, "shadow check failed: missing column users.name")
		matches, globErr := filepath.Glob(filepath.Join(migrationsDir, "*.sql"))
		c.Assert(globErr, qt.IsNil)
		c.Assert(matches, qt.HasLen, 2)
	})

	t.Run("correct migration chain writes candidate files", func(t *testing.T) {
		c := qt.New(t)
		dir := t.TempDir()
		entitiesDir := writeShadowE2EEntities(c.TB, dir)
		migrationsDir := filepath.Join(dir, "migrations")
		c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
		writeShadowE2EPriorMigration(c.TB, migrationsDir, "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL);\n")
		prepareShadowE2ETargetDB(c.TB, ctx, targetDBURL)

		output, err := runPtah(
			ctx,
			repoRoot,
			binaryPath,
			"migrations", "generate",
			"--root-dir", entitiesDir,
			"--db-url", targetDBURL,
			"--migrations-dir", migrationsDir,
			"--name", "add_email",
			"--shadow-db", shadowDBURL,
		)

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", output))
		c.Assert(output, qt.Contains, "Generated migration files")
		matches, globErr := filepath.Glob(filepath.Join(migrationsDir, "*.sql"))
		c.Assert(globErr, qt.IsNil)
		c.Assert(matches, qt.HasLen, 4)
		c.Assert(readFirstMatchingFile(c.TB, migrationsDir, "*_add_email.up.sql"), qt.Contains, "email")
	})
}

func e2eRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to resolve test file path")
	}
	return filepath.Dir(filepath.Dir(file))
}

func buildPtah(tb testing.TB, ctx context.Context, repoRoot, binaryPath string) {
	c := qt.New(tb)
	cmd := exec.CommandContext(ctx, "go", "build", "-o", binaryPath, "./cmd/ptah")
	cmd.Dir = repoRoot
	output, err := cmd.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("go build output:\n%s", string(output)))
}

// buildPtahCompat builds the ptah-compat binary, the drop-in Atlas
// replacement that hosts the Atlas-compatible command tree removed from the
// main ptah binary in #850.
func buildPtahCompat(tb testing.TB, ctx context.Context, repoRoot, binaryPath string) {
	c := qt.New(tb)
	cmd := exec.CommandContext(ctx, "go", "build", "-o", binaryPath, "./cmd/ptah-compat")
	cmd.Dir = repoRoot
	output, err := cmd.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("go build output:\n%s", string(output)))
}

func runPtah(ctx context.Context, repoRoot, binaryPath string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, binaryPath, args...)
	cmd.Dir = repoRoot
	output, err := cmd.CombinedOutput()
	return string(output), err
}

func createE2EDatabase(tb testing.TB, ctx context.Context, db *sql.DB, name string) {
	c := qt.New(tb)
	_, err := db.ExecContext(ctx, "CREATE DATABASE "+quoteE2EIdent(name))
	c.Assert(err, qt.IsNil)
}

func dropE2EDatabase(tb testing.TB, ctx context.Context, db *sql.DB, name string) {
	c := qt.New(tb)
	_, _ = db.ExecContext(ctx, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()", name)
	_, err := db.ExecContext(ctx, "DROP DATABASE IF EXISTS "+quoteE2EIdent(name))
	c.Assert(err, qt.IsNil)
}

func quoteE2EIdent(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func replaceDatabaseName(tb testing.TB, dbURL, databaseName string) string {
	c := qt.New(tb)
	parsed, err := url.Parse(dbURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + databaseName
	return parsed.String()
}

func prepareShadowE2ETargetDB(tb testing.TB, ctx context.Context, dbURL string) {
	c := qt.New(tb)
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

func writeShadowE2EEntities(tb testing.TB, dir string) string {
	c := qt.New(tb)
	entitiesDir := filepath.Join(dir, "entities")
	c.Assert(os.MkdirAll(entitiesDir, 0755), qt.IsNil)
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
	c.Assert(os.WriteFile(filepath.Join(entitiesDir, "schema.go"), []byte(content), 0600), qt.IsNil)
	return entitiesDir
}

func writeShadowE2EPriorMigration(tb testing.TB, dir, upSQL string) {
	c := qt.New(tb)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"), []byte(upSQL), 0600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"), []byte("DROP TABLE IF EXISTS users;\n"), 0600), qt.IsNil)
}

func readFirstMatchingFile(tb testing.TB, dir, pattern string) string {
	c := qt.New(tb)
	matches, err := filepath.Glob(filepath.Join(dir, pattern))
	c.Assert(err, qt.IsNil)
	c.Assert(matches, qt.Not(qt.HasLen), 0)
	content, err := os.ReadFile(matches[0])
	c.Assert(err, qt.IsNil)
	return string(content)
}
