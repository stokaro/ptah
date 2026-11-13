package schematests_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas/internal/atlastest"
)

// loneProdSchemaEnv is one env naming a database, a dev database and a desired
// schema, which no flag of the run selects (stokaro/ptah#5999).
const loneProdSchemaEnv = `env "prod" {
  url = "sqlite://prod.db"
  dev = "sqlite://file?mode=memory"
  src = "file://schema.sql"
}
`

// writeSchemaEnvSelectionProject writes two desired schemas and project as
// ./atlas.hcl into the working directory.
func writeSchemaEnvSelectionProject(c *qt.C, project string) {
	c.Helper()
	c.Assert(os.WriteFile("schema.sql", []byte("CREATE TABLE s1 (id int);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("from.sql", []byte("CREATE TABLE t1 (id int);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(project), 0o600), qt.IsNil)
}

// TestSchemaVerbsSelectNoEnvWithoutEnvFlag_FailurePath holds that a lone env no
// flag names gives a schema verb neither its database, its dev database nor
// its desired schema. Every row fails the way the pinned community binary
// v1.3.0 fails in the same directory, and prod.db, which any read of the env
// would create, stays absent.
func TestSchemaVerbsSelectNoEnvWithoutEnvFlag_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "apply",
			args:    []string{"schema", "apply", "--to", "file://schema.sql", "--auto-approve"},
			wantErr: `required flag\(s\) "url" not set`,
		},
		{
			name:    "apply with a --config",
			args:    []string{"schema", "apply", "--to", "file://schema.sql", "--auto-approve", "--config", "file://atlas.hcl"},
			wantErr: `required flag\(s\) "url" not set`,
		},
		{
			name:    "inspect",
			args:    []string{"schema", "inspect"},
			wantErr: `required flag\(s\) "url" not set`,
		},
		{
			name:    "clean",
			args:    []string{"schema", "clean", "--auto-approve"},
			wantErr: `required flag\(s\) "url" not set`,
		},
		{
			name:    "diff",
			args:    []string{"schema", "diff", "--from", "file://from.sql", "--to", "file://schema.sql"},
			wantErr: `--dev-url cannot be empty\. See: https://atlasgo\.io/atlas-schema/sql#dev-database`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := t.TempDir()
			t.Chdir(root)
			writeSchemaEnvSelectionProject(c, loneProdSchemaEnv)

			output, err := atlastest.RunCompatOutput(test.args...)

			c.Assert(err, qt.ErrorMatches, test.wantErr, qt.Commentf("command output:\n%s", output))
			atlastest.AssertPathAbsent(c, filepath.Join(root, "prod.db"))
		})
	}
}

// TestSchemaValidateReadsTheAdapterSelection_HappyPath holds that the forwarded
// native `schema validate` reads the project the adapter selected and does not
// discover ./atlas.hcl on its own. Left to discover it, the native command sees
// no --env, which the adapter consumes, so it refuses a file with two envs even
// when --env names one, and refuses a file with a syntax error that no flag
// names.
func TestSchemaValidateReadsTheAdapterSelection_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		project string
		args    []string
	}{
		{
			name:    "two envs and an --env naming one",
			project: "env \"a\" {\n  url = \"sqlite://a.db\"\n}\n\nenv \"b\" {\n  url = \"sqlite://b.db\"\n}\n",
			args:    []string{"--env", "a"},
		},
		{
			name:    "a file with a syntax error no flag names",
			project: "env \"x\" {\n  url =\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Chdir(t.TempDir())
			writeSchemaEnvSelectionProject(c, test.project)
			args := append([]string{"schema", "validate", "--dialect", "sqlite", "--schema-file", "schema.sql"}, test.args...)

			output, err := atlastest.RunCompatOutput(args...)

			c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", output))
		})
	}
}
