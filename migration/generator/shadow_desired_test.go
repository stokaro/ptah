package generator_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/goschema"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/shadow"
)

// usersEntities declares table users with the columns id, name and every
// further column given, each TEXT.
func usersEntities(columns ...string) string {
	source := "package models\n\n//ptah:schema:table name=\"users\"\ntype User struct {\n" +
		"\t//ptah:schema:field name=\"id\" type=\"INTEGER\" primary=\"true\"\n\tID int\n" +
		"\t//ptah:schema:field name=\"name\" type=\"TEXT\"\n\tName string\n"
	for _, column := range columns {
		source += "\t//ptah:schema:field name=\"" + column + "\" type=\"TEXT\"\n\tF" + column + " string\n"
	}
	return source + "}\n"
}

// shadowDesiredFixture is a target holding users(id, name), a migration
// directory that creates it, the entities that add column email, and the
// address of an empty shadow database.
func shadowDesiredFixture(c *qt.C) (dir, targetURL, shadowURL, modelsDir, migrationsDir string) {
	c.Helper()
	dir = c.TempDir()
	targetURL = "sqlite://" + filepath.Join(dir, "target.db")
	shadowURL = "sqlite://" + filepath.Join(dir, "shadow.db")
	target, err := dbschema.ConnectToDatabase(c.Context(), targetURL)
	c.Assert(err, qt.IsNil)
	_, err = target.ExecContext(c.Context(), "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	c.Assert(err, qt.IsNil)
	dbschema.CloseAndWarn(target)
	modelsDir = filepath.Join(dir, "models")
	c.Assert(os.MkdirAll(modelsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(modelsDir, "models.go"), []byte(usersEntities("email")), 0o600), qt.IsNil)
	migrationsDir = filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "0000000001_init.up.sql"), []byte("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "0000000001_init.down.sql"), []byte("DROP TABLE users;\n"), 0o600), qt.IsNil)
	return dir, targetURL, shadowURL, modelsDir, migrationsDir
}

// The shadow verification compares the replay with ShadowGenerated where it
// is set: a shadow desired schema that declares a column the migration does
// not add reports it missing, although the plan's own desired schema matches
// what the migration builds, and no migration is written.
func TestGenerateMigration_ShadowComparesWithTheShadowDesiredSchema(t *testing.T) {
	c := qt.New(t)
	dir, targetURL, shadowURL, modelsDir, migrationsDir := shadowDesiredFixture(c)
	runtime := must.Must(builtin.New())
	shadowModels := filepath.Join(dir, "shadow-models")
	c.Assert(os.MkdirAll(shadowModels, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(shadowModels, "models.go"), []byte(usersEntities("email", "phone")), 0o600), qt.IsNil)
	shadowDesired, err := goschema.ParseFS(runtime.Annotations(), os.DirFS(dir), "shadow-models")
	c.Assert(err, qt.IsNil)

	files, err := generator.GenerateMigration(c.Context(), generator.GenerateMigrationOptions{Runtime: runtime, GoEntitiesDir: modelsDir,
		DatabaseURL: targetURL, MigrationName: "add_email", OutputDir: migrationsDir, ShadowDatabaseURL: shadowURL, ShadowGenerated: shadowDesired,
	})

	c.Assert(files, qt.IsNil)
	var shadowErr *shadow.VerificationError
	c.Assert(err, qt.ErrorAs, &shadowErr)
	c.Assert(shadowErr.Error(), qt.Equals, "shadow check failed: missing column users.phone")
	written, err := filepath.Glob(filepath.Join(migrationsDir, "*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 2)
}

// Without ShadowGenerated the verification compares with the plan's desired
// schema, which the migration builds, and the migration is written.
func TestGenerateMigration_ShadowComparesWithThePlansDesiredSchema(t *testing.T) {
	c := qt.New(t)
	_, targetURL, shadowURL, modelsDir, migrationsDir := shadowDesiredFixture(c)

	files, err := generator.GenerateMigration(c.Context(), generator.GenerateMigrationOptions{Runtime: must.Must(builtin.New()), GoEntitiesDir: modelsDir,
		DatabaseURL: targetURL, MigrationName: "add_email", OutputDir: migrationsDir, ShadowDatabaseURL: shadowURL,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(files.Files, qt.HasLen, 1)
	written, err := filepath.Glob(filepath.Join(migrationsDir, "*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(written, qt.HasLen, 4)
}
