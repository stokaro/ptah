package schemaload_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/schemaload"
)

// --schema-file refused every env:// reference while --to accepted it, and the
// refusal said the scheme "is not resolved by ptah". It was: the flag reached
// the same atlas.hcl, but carried no environment for the reference to be read
// out of (stokaro/ptah#1760). These tests hold the resolution and both halves
// of the refusal that remains.

// writeEnvProject writes an atlas.hcl whose single env carries the given src
// attribute, plus the schema file it names, and returns the project directory.
func writeEnvProject(c *qt.C, src, schema string) string {
	dir := c.TempDir()
	config := "env \"dev\" {\n  src = \"" + src + "\"\n}\n"
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte(config), 0o600), qt.IsNil)
	if schema != "" {
		c.Assert(os.WriteFile(filepath.Join(dir, "desired.sql"), []byte(schema), 0o600), qt.IsNil)
	}
	return dir
}

// loadEnvProject evaluates the atlas.hcl written by writeEnvProject into the
// project environment the loader expands the reference through.
func loadEnvProject(c *qt.C, dir string) atlassource.ProjectEnv {
	config, err := projectconfig.Load(projectconfig.LoadOptions{
		AtlasPath: filepath.Join(dir, "atlas.hcl"),
		EnvName:   "dev",
	})
	c.Assert(err, qt.IsNil)
	return atlassource.ProjectEnv{Loaded: true, Config: config, BaseDir: dir}
}

func TestLoad_EnvReferenceResolvesThroughSelectedEnvironment(t *testing.T) {
	c := qt.New(t)

	dir := writeEnvProject(c, "file://desired.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

	database, err := schemaload.Load(schemaload.Options{
		SchemaFiles:     []string{"env://src"},
		ProjectEnv:      loadEnvProject(c, dir),
		EnvSelectorFlag: "env",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 1)
	c.Assert(database.Tables[0].Name, qt.Equals, "users")
}

// TestLoad_EnvReferenceResolvesToTheFileTheEnvironmentNames is the test that
// separates resolution from any file the loader might have found on its own:
// the environment names one of two schema files present, and the loaded table
// is the one it named.
func TestLoad_EnvReferenceResolvesToTheFileTheEnvironmentNames(t *testing.T) {
	c := qt.New(t)

	dir := writeEnvProject(c, "file://desired.sql", "CREATE TABLE named_by_env (id INTEGER PRIMARY KEY);\n")
	other := filepath.Join(dir, "other.sql")
	c.Assert(os.WriteFile(other, []byte("CREATE TABLE not_named (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

	database, err := schemaload.Load(schemaload.Options{
		SchemaFiles:     []string{"env://src"},
		ProjectEnv:      loadEnvProject(c, dir),
		EnvSelectorFlag: "env",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 1)
	c.Assert(database.Tables[0].Name, qt.Equals, "named_by_env")
}

// TestLoad_EnvReferenceWithSelectorButNoEnvironmentPointsAtTheFlag holds the
// refusal a command that offers --env still owes when the run passed none: it
// names the flag, because passing it is what resolves the reference.
func TestLoad_EnvReferenceWithSelectorButNoEnvironmentPointsAtTheFlag(t *testing.T) {
	c := qt.New(t)

	_, err := schemaload.Load(schemaload.Options{
		SchemaFiles:     []string{"env://src"},
		EnvSelectorFlag: "env",
	})

	c.Assert(err, qt.ErrorMatches, `--schema-file "env://src": env:// names the "src" attribute of a selected project environment, and this run selected none; pass --env, or pass the schema file itself`)
}

// TestLoad_EnvRefusalsDifferByWhetherTheCommandOffersASelector is the point of
// the split: a command with no --env must not tell the user to pass --env, and
// a command that has one must not send them to another binary.
func TestLoad_EnvRefusalsDifferByWhetherTheCommandOffersASelector(t *testing.T) {
	c := qt.New(t)

	_, withSelector := schemaload.Load(schemaload.Options{
		SchemaFiles:     []string{"env://src"},
		EnvSelectorFlag: "env",
	})
	_, withoutSelector := schemaload.Load(schemaload.Options{SchemaFiles: []string{"env://src"}})

	c.Assert(withSelector, qt.IsNotNil)
	c.Assert(withoutSelector, qt.IsNotNil)
	c.Assert(withSelector.Error(), qt.Not(qt.Equals), withoutSelector.Error())
	c.Assert(withSelector.Error(), qt.Contains, "pass --env")
	c.Assert(withoutSelector.Error(), qt.Not(qt.Contains), "pass --env")
}

// TestLoad_EnvReferenceToANonFileSourceNamesTheKind holds the refusal for an
// environment whose desired state is something --schema-file cannot read. It
// resolves, and then says what it found rather than reporting a missing file.
func TestLoad_EnvReferenceToANonFileSourceNamesTheKind(t *testing.T) {
	c := qt.New(t)

	dir := writeEnvProject(c, "sqlite://other.db", "")

	_, err := schemaload.Load(schemaload.Options{
		SchemaFiles:     []string{"env://src"},
		ProjectEnv:      loadEnvProject(c, dir),
		EnvSelectorFlag: "env",
	})

	c.Assert(err, qt.ErrorMatches, `--schema-file "env://src": the selected environment names a database URL \(sqlite://other\.db\), which --schema-file does not read; pass a schema file, or use ptah-compat, whose --to and --from read it`)
}

// TestLoad_PlainSchemaFileIgnoresTheProjectEnvironment is the negative control:
// expansion keys on the env:// scheme, so a run that carries an environment
// still loads the file it was given rather than the one the environment names.
func TestLoad_PlainSchemaFileIgnoresTheProjectEnvironment(t *testing.T) {
	c := qt.New(t)

	dir := writeEnvProject(c, "file://desired.sql", "CREATE TABLE named_by_env (id INTEGER PRIMARY KEY);\n")
	given := filepath.Join(dir, "given.sql")
	c.Assert(os.WriteFile(given, []byte("CREATE TABLE given (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

	database, err := schemaload.Load(schemaload.Options{
		SchemaFiles:     []string{given},
		ProjectEnv:      loadEnvProject(c, dir),
		EnvSelectorFlag: "env",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 1)
	c.Assert(database.Tables[0].Name, qt.Equals, "given")
}
