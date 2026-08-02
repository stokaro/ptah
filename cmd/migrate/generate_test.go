package migrate_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/cmd/migrate"
	"github.com/stokaro/ptah/config/projectconfig"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/generator"
	"github.com/stokaro/ptah/migration/safety"
)

func TestMigrateGenerateCommandExposesShadowDBFlag(t *testing.T) {
	c := qt.New(t)

	cmd := migrate.NewMigrateGenerateCommand()

	c.Assert(cmd.Name(), qt.Equals, "generate")
	c.Assert(cmd.Flags().Lookup("shadow-db"), qt.IsNotNil)
	c.Assert(cmd.Flags().Lookup("migrations-dir"), qt.IsNotNil)
	c.Assert(cmd.Flags().Lookup("config"), qt.IsNotNil)
	c.Assert(cmd.Flags().Lookup("env"), qt.IsNotNil)
}

func TestMigrateGenerateProjectConfigPrecedence(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "ptah.yaml"), []byte("migrate:\n  generate:\n    shadow_db: postgres://localhost/ptah_shadow\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(`env "local" {
  dev = "postgres://localhost/atlas_shadow"
}
`), 0o600), qt.IsNil)

	originalWD, err := os.Getwd()
	c.Assert(err, qt.IsNil)
	c.Assert(os.Chdir(dir), qt.IsNil)
	defer func() {
		c.Assert(os.Chdir(originalWD), qt.IsNil)
	}()

	cmd := migrate.NewMigrateGenerateCommand()
	c.Assert(cmd.ParseFlags([]string{"--shadow-db", "postgres://localhost/flag_shadow"}), qt.IsNil)
	flagShadow, err := cmd.Flags().GetString("shadow-db")
	c.Assert(err, qt.IsNil)
	cfg, err := dbcli.LoadProjectConfig(cmd, "")
	c.Assert(err, qt.IsNil)

	shadowDB := dbcli.EffectiveString(
		cmd,
		"shadow-db",
		flagShadow,
		cfg.StringValue(projectconfig.StringDevURL),
	)

	c.Assert(shadowDB, qt.Equals, "postgres://localhost/flag_shadow")

	cmd = migrate.NewMigrateGenerateCommand()
	cfg, err = dbcli.LoadProjectConfig(cmd, "")
	c.Assert(err, qt.IsNil)
	shadowDB = dbcli.EffectiveString(
		cmd,
		"shadow-db",
		"",
		cfg.StringValue(projectconfig.StringDevURL),
	)
	c.Assert(shadowDB, qt.Equals, "postgres://localhost/atlas_shadow")
}

func TestMigratePlanCommandRejectsAtlasApplyAtRoot(t *testing.T) {
	c := qt.New(t)

	cmd := migrate.NewMigrateCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"apply"})

	err := cmd.Execute()

	c.Assert(err, qt.ErrorMatches, `unexpected positional arguments \["apply"\]`)
}

func TestMigrateGenerateJSONReportWritesSiblingSafetyArtifact(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaFile := filepath.Join(dir, "schema.sql")
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.WriteFile(schemaFile, []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

	var stdout, stderr bytes.Buffer
	cmd := migrate.NewMigrateGenerateCommand()
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{
		"--schema-file", schemaFile,
		"--db-url", "sqlite:///" + filepath.Join(dir, "ptah.db"),
		"--migrations-dir", migrationsDir,
		"--name", "init",
		"--report", "json",
	})

	err := cmd.Execute()
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr.String()))

	reportFiles, err := filepath.Glob(filepath.Join(migrationsDir, "*_init.safety.json"))
	c.Assert(err, qt.IsNil)
	c.Assert(reportFiles, qt.HasLen, 1)
	c.Assert(stdout.String(), qt.Contains, "REPORT: ")
	c.Assert(stdout.String(), qt.Contains, filepath.Base(reportFiles[0]))

	rawReport, err := os.ReadFile(reportFiles[0])
	c.Assert(err, qt.IsNil)
	var report safety.Report
	c.Assert(json.Unmarshal(rawReport, &report), qt.IsNil)
	c.Assert(report.Highest, qt.Equals, safety.Safe)
	c.Assert(report.Destructive, qt.IsFalse)
	c.Assert(report.Assessments, qt.HasLen, 1)
}

func TestMigrateGenerateShadowVerificationWithRealDB(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()

	dbURL, conn := requireMigrateGeneratePostgresTestConnection(t, ctx)
	defer dbschema.CloseAndWarn(conn)
	releaseLock := acquireMigrateGenerateTestLock(c, ctx, conn)
	defer releaseLock()
	defer func() {
		c.Assert(conn.SchemaWriter().DropAllTables(ctx), qt.IsNil)
	}()

	c.Run("broken prior migration aborts before writing candidate files", func(c *qt.C) {
		dir := c.TempDir()
		entitiesDir := writeMigrateGenerateShadowEntities(c, dir)
		migrationsDir := filepath.Join(dir, "migrations")
		c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
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
			"--shadow-db", dbURL,
		})

		err := cmd.Execute()

		c.Assert(err, qt.ErrorMatches, `shadow check failed: missing column users\.name`)
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

	c.Run("correct prior migration writes candidate files", func(c *qt.C) {
		dir := c.TempDir()
		entitiesDir := writeMigrateGenerateShadowEntities(c, dir)
		migrationsDir := filepath.Join(dir, "migrations")
		c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
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
			"--shadow-db", dbURL,
		})

		err := cmd.Execute()

		c.Assert(err, qt.IsNil)
		c.Assert(out.String(), qt.Contains, "Generated migration files")
		matches, globErr := filepath.Glob(filepath.Join(migrationsDir, "*.sql"))
		c.Assert(globErr, qt.IsNil)
		c.Assert(matches, qt.HasLen, 4)
	})
}

func migrateGenerateTestDatabaseURL() string {
	for _, name := range []string{"TEST_DATABASE_URL", "TEST_DB_URL", "POSTGRES_TEST_DSN", "POSTGRES_URL"} {
		if value := os.Getenv(name); value != "" {
			return value
		}
	}
	return ""
}

func requireMigrateGeneratePostgresTestConnection(
	t *testing.T,
	ctx context.Context,
) (string, *dbschema.DatabaseConnection) {
	t.Helper()

	dbURL := migrateGenerateTestDatabaseURL()
	if dbURL == "" {
		t.Skip("PostgreSQL test database URL is not set")
	}

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	if err != nil {
		t.Skipf("test database is not available: %v", err)
	}
	if platform.NormalizeDialect(conn.Info().Dialect) != platform.Postgres {
		dbschema.CloseAndWarn(conn)
		t.Skipf("shadow CLI acceptance test requires PostgreSQL, got %s", conn.Info().Dialect)
	}

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

func writeMigrateGeneratePriorMigration(c *qt.C, dir, upSQL string) {
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"), []byte(upSQL), 0600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"), []byte("DROP TABLE IF EXISTS users;\n"), 0600), qt.IsNil)
}

func prepareMigrateGenerateTargetDB(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) {
	c.Assert(conn.SchemaWriter().DropAllTables(ctx), qt.IsNil)
	_, err := conn.ExecContext(ctx, "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")
	c.Assert(err, qt.IsNil)
}
