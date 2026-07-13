package generator

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestDescribeShadowDiffMissingColumn(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		TablesModified: []types.TableDiff{
			{
				TableName:    "users",
				ColumnsAdded: []string{"email", "name"},
			},
		},
	}

	c.Assert(describeShadowDiff(diff), qt.Equals, "missing column users.email")
}

func TestDescribeChangesIsDeterministic(t *testing.T) {
	c := qt.New(t)

	got := describeChanges(map[string]string{
		"nullable": "true -> false",
		"type":     "text -> varchar",
	})

	c.Assert(got, qt.Equals, "nullable true -> false, type text -> varchar")
}

func TestNextAvailableMigrationVersionChecksUpAndDownFiles(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	err := os.WriteFile(filepath.Join(dir, migrator.GenerateMigrationFileName(100, "add_email", "down")), []byte("SELECT 1;"), 0600)
	c.Assert(err, qt.IsNil)
	err = os.WriteFile(filepath.Join(dir, migrator.GenerateMigrationFileName(105, "future", "up")), []byte("SELECT 1;"), 0600)
	c.Assert(err, qt.IsNil)

	c.Assert(nextAvailableMigrationVersion(dir, 100, "add_email"), qt.Equals, 106)
}

func TestLoadPriorMigrationsMissingDir(t *testing.T) {
	c := qt.New(t)

	migrations, err := loadPriorMigrations(filepath.Join(t.TempDir(), "missing"))

	c.Assert(err, qt.IsNil)
	c.Assert(migrations, qt.HasLen, 0)
}

func TestGenerateMigrationShadowVerificationWithRealDB(t *testing.T) {
	dbURL := shadowTestDatabaseURL()
	if dbURL == "" {
		t.Skip("PostgreSQL test database URL is not set")
	}

	c := qt.New(t)
	ctx := context.Background()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	if err != nil {
		t.Skipf("test database is not available: %v", err)
	}
	defer dbschema.CloseAndWarn(conn)
	if platform.NormalizeDialect(conn.Info().Dialect) != platform.Postgres {
		t.Skipf("shadow acceptance test requires PostgreSQL, got %s", conn.Info().Dialect)
	}
	releaseLock := acquireShadowTestLock(c, ctx, conn)
	defer releaseLock()
	defer func() {
		c.Assert(conn.Writer().DropAllTables(), qt.IsNil)
	}()

	c.Run("broken prior migration aborts with missing column", func(c *qt.C) {
		dir := c.TempDir()
		entitiesDir := writeShadowEntities(c, dir)
		migrationsDir := filepath.Join(dir, "migrations")
		c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
		writePriorMigration(c, migrationsDir, "CREATE TABLE users (id SERIAL PRIMARY KEY);\n")

		prepareShadowTargetDB(c, ctx, conn)

		files, err := GenerateMigration(ctx, GenerateMigrationOptions{
			GoEntitiesDir:     entitiesDir,
			DatabaseURL:       dbURL,
			MigrationName:     "add_email",
			OutputDir:         migrationsDir,
			ShadowDatabaseURL: dbURL,
		})

		c.Assert(files, qt.IsNil)
		c.Assert(err, qt.ErrorMatches, `shadow check failed: missing column users\.name`)
		matches, globErr := filepath.Glob(filepath.Join(migrationsDir, "*.sql"))
		c.Assert(globErr, qt.IsNil)
		c.Assert(matches, qt.HasLen, 2)
	})

	c.Run("correct prior migration passes and writes files", func(c *qt.C) {
		dir := c.TempDir()
		entitiesDir := writeShadowEntities(c, dir)
		migrationsDir := filepath.Join(dir, "migrations")
		c.Assert(os.MkdirAll(migrationsDir, 0755), qt.IsNil)
		writePriorMigration(c, migrationsDir, "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL);\n")

		prepareShadowTargetDB(c, ctx, conn)

		files, err := GenerateMigration(ctx, GenerateMigrationOptions{
			GoEntitiesDir:     entitiesDir,
			DatabaseURL:       dbURL,
			MigrationName:     "add_email",
			OutputDir:         migrationsDir,
			ShadowDatabaseURL: dbURL,
		})

		c.Assert(err, qt.IsNil)
		c.Assert(files, qt.IsNotNil)
		c.Assert(files.UpFile, qt.Not(qt.Equals), "")
		c.Assert(files.DownFile, qt.Not(qt.Equals), "")
		upSQL, readErr := os.ReadFile(files.UpFile)
		c.Assert(readErr, qt.IsNil)
		c.Assert(string(upSQL), qt.Contains, "email")
	})
}

func shadowTestDatabaseURL() string {
	for _, name := range []string{"TEST_DATABASE_URL", "TEST_DB_URL", "POSTGRES_TEST_DSN", "POSTGRES_URL"} {
		if value := os.Getenv(name); value != "" {
			return value
		}
	}
	return ""
}

func acquireShadowTestLock(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) func() {
	_, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock(156156156)")
	c.Assert(err, qt.IsNil)

	return func() {
		_, unlockErr := conn.ExecContext(ctx, "SELECT pg_advisory_unlock(156156156)")
		c.Assert(unlockErr, qt.IsNil)
	}
}

func writeShadowEntities(c *qt.C, dir string) string {
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

func writePriorMigration(c *qt.C, dir, upSQL string) {
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"), []byte(upSQL), 0600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"), []byte("DROP TABLE IF EXISTS users;\n"), 0600), qt.IsNil)
}

func prepareShadowTargetDB(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) {
	c.Assert(conn.Writer().DropAllTables(), qt.IsNil)
	_, err := conn.ExecContext(ctx, "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")
	c.Assert(err, qt.IsNil)
}
