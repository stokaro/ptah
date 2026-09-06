package generator_test

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/generator"
	"ptah.run/migration/migrationfile"
)

// TestGenerateMigration_WritesAPairThatCreatesAndDropsTheTable is the happy
// path, and it is the first one this package has had.
//
// What stood here was named happy path, pointed at a directory with no entities
// and a `memory://` URL no scheme dispatcher accepts, and asserted that the
// error contained the substring "error" (stokaro/ptah#2502). Every stage of
// generation could have been broken under it.
//
// SQLite needs no server, so a real generation is a unit test: entities on
// disk, a database file, and the two artifacts read back.
func TestGenerateMigration_WritesAPairThatCreatesAndDropsTheTable(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	entities := writeEntities(c, root, `//ptah:schema:table name="widgets"
type Widget struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64

	//ptah:schema:field name="name" type="TEXT" not_null="true"
	Name string
}`)
	output := makeDir(c, root, "migrations")

	files, err := generator.GenerateMigration(context.Background(), generator.GenerateMigrationOptions{
		GoEntitiesDir: entities,
		DatabaseURL:   "sqlite://" + filepath.Join(root, "target.db"),
		MigrationName: "create_widgets",
		OutputDir:     output,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(files.Files, qt.HasLen, 1)
	pair := files.Files[0]

	// The names come from the production namer, so this reads them back through
	// the parser that owns the format rather than matching a pattern here.
	up, err := migrationfile.ParseFileName(filepath.Base(pair.UpFile))
	c.Assert(err, qt.IsNil)
	c.Assert(up.Version, qt.Equals, pair.Version)
	c.Assert(up.Direction, qt.Equals, "up")
	down, err := migrationfile.ParseFileName(filepath.Base(pair.DownFile))
	c.Assert(err, qt.IsNil)
	c.Assert(down.Version, qt.Equals, pair.Version)
	c.Assert(down.Direction, qt.Equals, "down")

	// And the artifacts say what the migration does, in both directions. A pair
	// written with an empty up file passes every assertion about names.
	c.Assert(readFile(c, pair.UpFile), qt.Contains, `CREATE TABLE "widgets"`)
	c.Assert(readFile(c, pair.UpFile), qt.Contains, `"name" TEXT NOT NULL`)
	c.Assert(readFile(c, pair.DownFile), qt.Contains, `DROP TABLE IF EXISTS "widgets"`)
}

// TestGenerateMigration_AnEmptyNameDefaultsToMigration covers the default the
// replaced test's own comment described and never checked -- it asserted only
// that a run with no name failed, for want of a fixture.
func TestGenerateMigration_AnEmptyNameDefaultsToMigration(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	entities := writeEntities(c, root, `//ptah:schema:table name="widgets"
type Widget struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64
}`)
	output := makeDir(c, root, "migrations")

	files, err := generator.GenerateMigration(context.Background(), generator.GenerateMigrationOptions{
		GoEntitiesDir: entities,
		DatabaseURL:   "sqlite://" + filepath.Join(root, "target.db"),
		OutputDir:     output,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(files.Files, qt.HasLen, 1)
	// On disk first, because that is what an operator sees, and then through
	// the parser -- which title-cases what it reads back, so the two spellings
	// are both the contract and neither stands for the other.
	c.Assert(filepath.Base(files.Files[0].UpFile), qt.Matches, `[0-9]+_migration\.up\.sql`)
	name, err := migrationfile.ParseFileName(filepath.Base(files.Files[0].UpFile))
	c.Assert(err, qt.IsNil)
	c.Assert(name.Name, qt.Equals, "Migration")
}

// TestGenerateMigration_AMissingEntitiesDirectoryIsRefusedByName asserts the
// validation boundary rather than that something, somewhere, failed.
//
// The replaced rows accepted any error from a run whose entities directory did
// not exist AND whose database URL named no scheme this build dispatches, so a
// failure at either stage satisfied them -- and one of them wrote to the shared
// absolute path /tmp/migrations while doing it.
func TestGenerateMigration_AMissingEntitiesDirectoryIsRefusedByName(t *testing.T) {
	c := qt.New(t)
	root := t.TempDir()
	missing := filepath.Join(root, "no-such-entities")

	_, err := generator.GenerateMigration(context.Background(), generator.GenerateMigrationOptions{
		GoEntitiesDir: missing,
		DatabaseURL:   "sqlite://" + filepath.Join(root, "target.db"),
		MigrationName: "create_widgets",
		OutputDir:     makeDir(c, root, "migrations"),
	})

	c.Assert(err, qt.IsNotNil)
	// Ptah's own clause and the name the caller gave, and not the sentence the
	// operating system appended: "no such file or directory" is one platform's
	// wording and Windows writes another.
	c.Assert(err.Error(), qt.Contains, "error parsing Go entities")
	c.Assert(err.Error(), qt.Contains, filepath.Base(missing))
}

func TestPlanMigrationRejectsMalformedSQLiteVirtualDropToggleBeforeDesiredSchema(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")

	_, err := generator.PlanMigration(context.Background(), generator.GenerateMigrationOptions{
		GoEntitiesDir: filepath.Join(t.TempDir(), "missing"),
		DatabaseURL:   "sqlite://" + filepath.Join(t.TempDir(), "target.db"),
		MigrationName: "toggle-order",
		OutputDir:     t.TempDir(),
	})

	c.Assert(err, qt.ErrorMatches,
		`invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "parse")
}

func TestPlanMigrationRejectsMalformedSQLiteVirtualDropToggleBeforeOutputPath(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	root := t.TempDir()

	_, err := generator.PlanMigration(context.Background(), generator.GenerateMigrationOptions{
		DatabaseURL:       "sqlite://" + filepath.Join(t.TempDir(), "target.db"),
		MigrationName:     "toggle-order",
		OutputDir:         filepath.Join(root, "..", "outside"),
		AllowedOutputRoot: root,
	})

	c.Assert(err, qt.ErrorMatches,
		`invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "output directory")
}

func TestPlanMigrationRejectsMalformedSQLiteConnectionToggleBeforeOutputPath(t *testing.T) {
	c := qt.New(t)
	connection, err := dbschema.ConnectToDatabase(
		context.Background(),
		"sqlite://"+filepath.Join(t.TempDir(), "target.db"),
	)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(connection)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	root := t.TempDir()

	_, err = generator.PlanMigration(context.Background(), generator.GenerateMigrationOptions{
		DBConn:            connection,
		MigrationName:     "toggle-order",
		OutputDir:         filepath.Join(root, "..", "outside"),
		AllowedOutputRoot: root,
	})

	c.Assert(err, qt.ErrorMatches,
		`invalid boolean value "not-a-boolean" for PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP`)
	c.Assert(err.Error(), qt.Not(qt.Contains), "output directory")
}

func TestPlanMigrationDoesNotApplySQLiteToggleToPostgresOutputPath(t *testing.T) {
	c := qt.New(t)
	t.Setenv("PTAH_SQLITE_ALLOW_VIRTUAL_TABLE_DROP", "not-a-boolean")
	root := t.TempDir()

	_, err := generator.PlanMigration(context.Background(), generator.GenerateMigrationOptions{
		DatabaseURL:       "postgres://localhost/database",
		MigrationName:     "toggle-isolation",
		OutputDir:         filepath.Join(root, "..", "outside"),
		AllowedOutputRoot: root,
	})

	c.Assert(err, qt.ErrorMatches, `error validating output directory: .*outside allowed root.*`)
}

func TestGenerateMigration_FilesystemPathResolution(t *testing.T) {
	c := qt.New(t)

	// This test verifies the fix for the filesystem path resolution bug
	// that was causing integration tests to fail with "invalid argument" errors
	// when using absolute paths in temporary directories

	// Create a temporary directory structure similar to integration tests
	tempDir := c.TempDir()
	entitiesDir := filepath.Join(tempDir, "entities")
	err := os.MkdirAll(entitiesDir, 0755)
	c.Assert(err, qt.IsNil)

	// Create minimal schema file in the entities directory
	schemaContent := `package entities

//ptah:schema:table name="test_table_filesystem"
type TestTable struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="name" type="VARCHAR(255)"
	Name string
}
`

	schemaPath := filepath.Join(entitiesDir, "schema.go")
	err = os.WriteFile(schemaPath, []byte(schemaContent), 0600)
	c.Assert(err, qt.IsNil)

	migrationsDir := filepath.Join(tempDir, "migrations")
	err = os.MkdirAll(migrationsDir, 0755)
	c.Assert(err, qt.IsNil)

	// Test with absolute path (like integration tests use)
	// This should NOT fail with "invalid argument" error
	opts := generator.GenerateMigrationOptions{
		GoEntitiesDir: entitiesDir, // Absolute path like /tmp/ptah_integration_test_*/entities
		GoEntitiesFS:  nil,         // This should trigger the default filesystem setup
		DatabaseURL:   "memory://test",
		MigrationName: "test_filesystem_path",
		OutputDir:     migrationsDir,
	}

	// This should not fail with filesystem path resolution errors
	_, err = generator.GenerateMigration(context.Background(), opts)

	// We expect this to fail due to memory database limitations, but NOT due to filesystem path issues
	c.Assert(err, qt.IsNotNil)

	// The error should be about database connection or parsing, NOT about filesystem paths
	errMsg := err.Error()
	c.Assert(errMsg, qt.Not(qt.Contains), "invalid argument",
		qt.Commentf("Should not have filesystem path resolution errors, got: %s", errMsg))
	// Not a substring: a filesystem failure is spelled "stat" on Unix and
	// "GetFileAttributesEx" on Windows, so matching one name asserts only the
	// platform it runs on. Asking whether the error is a *fs.PathError at all
	// is the same question without a spelling in it.
	c.Assert(err, qt.Not(qt.ErrorAs), new(*fs.PathError),
		qt.Commentf("Should not have filesystem path errors, got: %s", errMsg))

	// The error should be about database or parsing issues instead
	c.Assert(strings.Contains(errMsg, "database") || strings.Contains(errMsg, "parsing") || strings.Contains(errMsg, "memory"), qt.IsTrue,
		qt.Commentf("Expected database or parsing error, got: %s", errMsg))
}

func TestGenerateMigration_FilesystemPathResolution_RelativePath(t *testing.T) {
	c := qt.New(t)

	// Test the filesystem path resolution with relative paths as well
	// to ensure both absolute and relative paths work correctly

	// Create a temporary directory and change to it
	tempDir := c.TempDir()
	originalWd, err := os.Getwd()
	c.Assert(err, qt.IsNil)
	defer func() {
		err := os.Chdir(originalWd)
		c.Assert(err, qt.IsNil)
	}()

	err = os.Chdir(tempDir)
	c.Assert(err, qt.IsNil)

	// Create entities directory
	entitiesDir := "entities"
	err = os.MkdirAll(entitiesDir, 0755)
	c.Assert(err, qt.IsNil)

	// Create minimal schema file
	schemaContent := `package entities

//ptah:schema:table name="test_table_relative"
type TestTable struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64
}
`

	schemaPath := filepath.Join(entitiesDir, "schema.go")
	err = os.WriteFile(schemaPath, []byte(schemaContent), 0600)
	c.Assert(err, qt.IsNil)

	migrationsDir := "migrations"
	err = os.MkdirAll(migrationsDir, 0755)
	c.Assert(err, qt.IsNil)

	// Test with relative path
	opts := generator.GenerateMigrationOptions{
		GoEntitiesDir: entitiesDir, // Relative path like "./entities"
		GoEntitiesFS:  nil,         // This should trigger the default filesystem setup
		DatabaseURL:   "memory://test",
		MigrationName: "test_relative_path",
		OutputDir:     migrationsDir,
	}

	// This should not fail with filesystem path resolution errors
	_, err = generator.GenerateMigration(context.Background(), opts)

	// We expect this to fail due to memory database limitations, but NOT due to filesystem path issues
	c.Assert(err, qt.IsNotNil)

	// The error should be about database connection or parsing, NOT about filesystem paths
	errMsg := err.Error()
	c.Assert(errMsg, qt.Not(qt.Contains), "invalid argument",
		qt.Commentf("Should not have filesystem path resolution errors, got: %s", errMsg))
	// Not a substring: a filesystem failure is spelled "stat" on Unix and
	// "GetFileAttributesEx" on Windows, so matching one name asserts only the
	// platform it runs on. Asking whether the error is a *fs.PathError at all
	// is the same question without a spelling in it.
	c.Assert(err, qt.Not(qt.ErrorAs), new(*fs.PathError),
		qt.Commentf("Should not have filesystem path errors, got: %s", errMsg))
}

// writeEntities puts one annotated struct in its own package directory and
// returns the directory.
func writeEntities(c *qt.C, root, declaration string) string {
	c.Helper()
	dir := makeDir(c, root, "entities")
	source := "package entities\n\n" + declaration + "\n"
	c.Assert(os.WriteFile(filepath.Join(dir, "schema.go"), []byte(source), 0o600), qt.IsNil)
	return dir
}

// makeDir creates one directory under root.
func makeDir(c *qt.C, root, name string) string {
	c.Helper()
	dir := filepath.Join(root, name)
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	return dir
}

// readFile reads a generated artifact.
func readFile(c *qt.C, path string) string {
	c.Helper()
	body, err := os.ReadFile(path) //gosec:disable G304 -- the path is a temporary directory this test made
	c.Assert(err, qt.IsNil)
	return string(body)
}
