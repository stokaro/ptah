package atlas_test

import (
	"os"
	"path/filepath"
	"strings"
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
// says nothing about. The `schemas` attribute appears in none of the rows on
// purpose: a config that spells it reaches the parser arm and would pass
// whatever the boundary did.
//
// The last three rows are the non-interference controls. A refusal that fired
// on an absent, false or true value would pass the first three rows and break
// every project that sets the variable correctly, so the database has to be
// described in each of them.
func TestCompatEnvSchemasOptOutIsRefusedWithoutAProjectFile(t *testing.T) {
	tests := []struct {
		name          string
		project       func(testing.TB, string)
		env           func(testing.TB)
		wantErr       string
		wantDescribed bool
	}{
		{
			name:    "an exported empty value is refused with no project file",
			project: withoutAtlasProjectFile,
			env:     envbooltest.Set(projectconfig.IgnoreEnvSchemasEnvVar, ""),
			wantErr: `invalid boolean value "" for PTAH_ATLAS_IGNORE_ENV_SCHEMAS`,
		},
		{
			name:    "a typo is refused with no project file",
			project: withoutAtlasProjectFile,
			env:     envbooltest.Set(projectconfig.IgnoreEnvSchemasEnvVar, "yes"),
			wantErr: `invalid boolean value "yes" for PTAH_ATLAS_IGNORE_ENV_SCHEMAS`,
		},
		{
			name:    "the same value is refused with a project file that omits schemas",
			project: withAtlasProjectFileWithoutSchemas,
			env:     envbooltest.Set(projectconfig.IgnoreEnvSchemasEnvVar, ""),
			wantErr: `invalid boolean value "" for PTAH_ATLAS_IGNORE_ENV_SCHEMAS`,
		},
		{
			name:          "an unset variable describes the database with no project file",
			project:       withoutAtlasProjectFile,
			env:           envbooltest.Unset(projectconfig.IgnoreEnvSchemasEnvVar),
			wantDescribed: true,
		},
		{
			name:          "a valid false describes the database with no project file",
			project:       withoutAtlasProjectFile,
			env:           envbooltest.Set(projectconfig.IgnoreEnvSchemasEnvVar, "false"),
			wantDescribed: true,
		},
		{
			name:          "a valid true describes the database with no project file",
			project:       withoutAtlasProjectFile,
			env:           envbooltest.Set(projectconfig.IgnoreEnvSchemasEnvVar, "1"),
			wantDescribed: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			test.project(t, dir)
			test.env(t)
			seedSQLiteDBAt(t, filepath.Join(dir, "probe.db"), "CREATE TABLE users (id INTEGER PRIMARY KEY)")
			t.Chdir(dir)

			out, err := runCompatCommand(t, "schema", "inspect", "--url", "sqlite://probe.db")

			c.Assert(errMessageOrEmpty(err), qt.Equals, test.wantErr, qt.Commentf("%s", out))
			c.Assert(strings.Contains(out, `table "users"`), qt.Equals, test.wantDescribed,
				qt.Commentf("command output:\n%s", out))
		})
	}
}

// withoutAtlasProjectFile leaves dir with no atlas.hcl, which is the arm the
// adapter short-circuits on.
func withoutAtlasProjectFile(testing.TB, string) {}

// withAtlasProjectFileWithoutSchemas writes the control config: a project file
// that exists and parses, and whose selected environment does not spell
// `schemas`, so the value can only be read by an eager resolve.
func withAtlasProjectFileWithoutSchemas(t testing.TB, dir string) {
	t.Helper()
	c := qt.New(t)
	config := "env \"local\" {\n  url = \"sqlite://probe.db\"\n}\n"
	c.Assert(os.WriteFile(filepath.Join(dir, projectconfig.AtlasFileName), []byte(config), 0o600), qt.IsNil)
}
