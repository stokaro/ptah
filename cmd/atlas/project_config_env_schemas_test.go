package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/envbool/envbooltest"
)

// TestCompatEnvSchemasOptOutIsRefusedWithoutAProjectFile pins the refusal on
// the compatibility adapter's OWN project-load boundary, which is the one door
// into an atlas.hcl evaluation that config/projectconfig cannot see.
//
// The adapter opens the project file itself and answers "no project" when the
// file is absent, so on that arm it calls neither
// [projectconfig.LoadAtlasFileCollectionWithOptions] nor
// [projectconfig.ParseAtlasFSCollectionWithOptions], where the eager resolve
// lives. Measured on ptah-compat built from the branch before this test, with
// `PTAH_ATLAS_IGNORE_ENV_SCHEMAS` exported empty and
// `schema inspect --url sqlite://probe.db`:
//
//	directory holding an atlas.hcl   exit 1  invalid boolean value "" for …
//	directory holding none           exit 0  schema "main" { }
//
// One environment, two answers, chosen by the presence of a file the variable
// says nothing about. The `schemas` attribute appears in no fixture here on
// purpose: a config that spells it reaches the parser arm and would pass
// whatever the boundary did.
//
// The other arm of the boundary is
// [TestCompatEnvSchemasOptOutIsRefusedWithAProjectFileThatOmitsSchemas], and
// the non-interference control -- the values that must still describe the
// database -- is [TestCompatEnvSchemasValidValuesStillDescribeTheDatabase]. A
// refusal and a description are two different measurements, so they are two
// tests.
func TestCompatEnvSchemasOptOutIsRefusedWithoutAProjectFile(t *testing.T) {
	tests := []struct {
		name    string
		env     func(testing.TB)
		wantErr string
	}{
		{
			name:    "an exported empty value is refused",
			env:     envbooltest.Set(projectconfig.IgnoreEnvSchemasEnvVar, ""),
			wantErr: `invalid boolean value "" for PTAH_ATLAS_IGNORE_ENV_SCHEMAS`,
		},
		{
			name:    "a typo is refused",
			env:     envbooltest.Set(projectconfig.IgnoreEnvSchemasEnvVar, "yes"),
			wantErr: `invalid boolean value "yes" for PTAH_ATLAS_IGNORE_ENV_SCHEMAS`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			test.env(t)
			seedSQLiteDBAt(t, filepath.Join(dir, "probe.db"), "CREATE TABLE users (id INTEGER PRIMARY KEY)")
			t.Chdir(dir)

			out, err := runCompatCommand(t, "schema", "inspect", "--url", "sqlite://probe.db")

			c.Assert(errMessageOrEmpty(err), qt.Equals, test.wantErr, qt.Commentf("%s", out))
			c.Assert(out, qt.Not(qt.Contains), `table "users"`)
		})
	}
}

// TestCompatEnvSchemasOptOutIsRefusedWithAProjectFileThatOmitsSchemas is the
// other side of the boundary: a project file that exists and parses reaches the
// parser arm, and the same value has to be refused there too. Without this the
// refusal could be read off the absent-file arm alone.
func TestCompatEnvSchemasOptOutIsRefusedWithAProjectFileThatOmitsSchemas(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	writeAtlasProjectFileWithoutSchemas(c, dir)
	t.Setenv(projectconfig.IgnoreEnvSchemasEnvVar, "")
	seedSQLiteDBAt(t, filepath.Join(dir, "probe.db"), "CREATE TABLE users (id INTEGER PRIMARY KEY)")
	t.Chdir(dir)

	out, err := runCompatCommand(t, "schema", "inspect", "--url", "sqlite://probe.db")

	c.Assert(errMessageOrEmpty(err), qt.Equals,
		`invalid boolean value "" for PTAH_ATLAS_IGNORE_ENV_SCHEMAS`, qt.Commentf("%s", out))
	c.Assert(out, qt.Not(qt.Contains), `table "users"`)
}

// TestCompatEnvSchemasValidValuesStillDescribeTheDatabase is the
// non-interference control for the two refusals above. A refusal that fired on
// an absent, false or true value would pass both of them and break every
// project that sets the variable correctly, so the database has to be described
// in each of these rows.
//
// Absent is a state of its own, not the empty string: [envbooltest.Unset] is
// what reaches it, because t.Setenv("") EXPORTS an empty value, which is the
// first row of the refusal test.
func TestCompatEnvSchemasValidValuesStillDescribeTheDatabase(t *testing.T) {
	tests := []struct {
		name string
		env  func(testing.TB)
	}{
		{
			name: "an unset variable",
			env:  envbooltest.Unset(projectconfig.IgnoreEnvSchemasEnvVar),
		},
		{
			name: "a valid false",
			env:  envbooltest.Set(projectconfig.IgnoreEnvSchemasEnvVar, "false"),
		},
		{
			name: "a valid true",
			env:  envbooltest.Set(projectconfig.IgnoreEnvSchemasEnvVar, "1"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			test.env(t)
			seedSQLiteDBAt(t, filepath.Join(dir, "probe.db"), "CREATE TABLE users (id INTEGER PRIMARY KEY)")
			t.Chdir(dir)

			out, err := runCompatCommand(t, "schema", "inspect", "--url", "sqlite://probe.db")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, `table "users"`)
		})
	}
}

// writeAtlasProjectFileWithoutSchemas writes the control config: a project file
// that exists and parses, and whose selected environment does not spell
// `schemas`, so the value can only be read by an eager resolve.
func writeAtlasProjectFileWithoutSchemas(c *qt.C, dir string) {
	c.Helper()
	config := "env \"local\" {\n  url = \"sqlite://probe.db\"\n}\n"
	c.Assert(os.WriteFile(filepath.Join(dir, projectconfig.AtlasFileName), []byte(config), 0o600), qt.IsNil)
}
