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

func TestGenerateMigration_ExtensionsWithRealPostgres(t *testing.T) {
	c := qt.New(t)
	ctx := t.Context()
	adminURL := requireGeneratorPostgresURL(t)
	admin, err := dbschema.ConnectToDatabase(ctx, adminURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(admin)
	targetURL, targetDatabase := createGeneratorTestPostgres(c.TB, admin, adminURL, "ptah_extensions")
	defer dropGeneratorTestPostgres(c.TB, admin, targetDatabase)

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
