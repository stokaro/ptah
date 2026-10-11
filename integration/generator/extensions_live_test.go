//go:build integration

package generator_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
)

func TestGenerateMigration_ExtensionsWithRealPostgres(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	adminURL := requireGeneratorPostgresURL(t)
	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(admin)
	targetURL, targetDatabase := createGeneratorTestPostgres(c, admin, adminURL, "ptah_extensions")
	defer dropGeneratorTestPostgres(c, admin, targetDatabase)

	root := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(root, "schema.go"), []byte(`package testschema

//ptah:schema:extension name="pg_trgm" if_not_exists="true" comment="Test trigram extension"
//ptah:schema:extension name="btree_gin" if_not_exists="true" comment="Test btree_gin extension"
type TestExtensions struct{}

//ptah:schema:table name="test_table_generator"
type TestTable struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="name" type="VARCHAR(255)"
	Name string
}
`), 0o600), qt.IsNil)
	migrationsDir := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)

	files, err := generator.GenerateMigration(ctx, generator.GenerateMigrationOptions{
		Runtime:       must.Must(builtin.New()),
		GoEntitiesDir: root,
		DatabaseURL:   targetURL,
		MigrationName: "extensions",
		OutputDir:     migrationsDir,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.IsNotNil)
	c.Assert(files.Files, qt.HasLen, 1)
	upContent, err := os.ReadFile(files.Files[0].UpFile)
	c.Assert(err, qt.IsNil)
	downContent, err := os.ReadFile(files.Files[0].DownFile)
	c.Assert(err, qt.IsNil)
	upSQL := legacyRenderedSQL(string(upContent))
	downSQL := legacyRenderedSQL(string(downContent))
	c.Assert(upSQL, qt.Contains, "CREATE EXTENSION IF NOT EXISTS pg_trgm")
	c.Assert(upSQL, qt.Contains, "CREATE EXTENSION IF NOT EXISTS btree_gin")
	c.Assert(upSQL, qt.Contains, "CREATE TABLE test_table_generator")
	c.Assert(downSQL, qt.Contains, "DROP EXTENSION IF EXISTS pg_trgm")
	c.Assert(downSQL, qt.Contains, "DROP EXTENSION IF EXISTS btree_gin")
	c.Assert(downSQL, qt.Contains, "DROP TABLE IF EXISTS test_table_generator")
}

// TestGenerateMigration_ExtensionVersionUpdateWithRealPostgres pins that a
// raised extension version generates a migration. Its rollback had asked for
// the downgrade PostgreSQL refuses, and that refusal failed the whole
// generation, the up file included. The rollback now leaves the version and
// says so in the down file, the up file applies, and the database then holds
// the declared version.
func TestGenerateMigration_ExtensionVersionUpdateWithRealPostgres(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	adminURL := requireGeneratorPostgresURL(t)
	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(admin)
	targetURL, targetDatabase := createGeneratorTestPostgres(c, admin, adminURL, "ptah_extension_version")
	defer dropGeneratorTestPostgres(c, admin, targetDatabase)
	target, err := dbschema.ConnectToDatabase(ctx, targetURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(target)
	c.Assert(target.Writer().ExecuteSQL(ctx, "CREATE EXTENSION pg_trgm VERSION '1.5'"), qt.IsNil)
	declared := &schemamodel.Database{Extensions: []schemamodel.Extension{{Name: "pg_trgm", Schema: "public", Version: "1.6"}}}

	up, down := generateLiveMigrationSQL(c, target, declared)

	c.Assert(up, qt.Contains, `ALTER EXTENSION "pg_trgm" UPDATE TO '1.6';`)
	c.Assert(down, qt.Contains, `-- Recovery limit: "PostgreSQL updates an extension in one direction only: after the rollback, pg_trgm stays at version 1.6 rather than 1.5."`)
	c.Assert(down, qt.Not(qt.Contains), "UPDATE TO")
	c.Assert(target.Writer().ExecuteSQL(ctx, up), qt.IsNil)
	var version string
	c.Assert(target.QueryRowContext(ctx, "SELECT extversion FROM pg_extension WHERE extname = 'pg_trgm'").Scan(&version), qt.IsNil)
	c.Assert(version, qt.Equals, "1.6")
}
