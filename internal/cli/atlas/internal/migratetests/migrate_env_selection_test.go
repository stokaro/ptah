package migratetests_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/atlas/internal/atlastest"
)

// loneProdEnv is the project file stokaro/ptah#5999 measured: one env naming a
// database and a dev database, which no flag of the run selects.
const loneProdEnv = `env "prod" {
  url = "sqlite://prod.db"
  dev = "sqlite://file?mode=memory"
}
`

// writeEnvSelectionProject writes a hashed two-file migration directory, a
// desired schema, and project under name into the working directory.
func writeEnvSelectionProject(c *qt.C, name, project string) {
	c.Helper()
	atlastest.WriteAtlasApplyProjectMigration(c, "migrations", "1.sql", "CREATE TABLE t1 (id int);\n")
	atlastest.WriteAtlasApplyProjectMigration(c, "migrations", "2.sql", "CREATE TABLE t2 (id int);\n")
	atlastest.WriteAtlasApplyProjectSum(c, "migrations")
	c.Assert(os.WriteFile("schema.sql", []byte("CREATE TABLE s1 (id int);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(name, []byte(project), 0o600), qt.IsNil)
}

// TestMigrateVerbsSelectNoEnvWithoutEnvFlag_FailurePath holds the defect
// stokaro/ptah#5999 names: selecting a lone env in ./atlas.hcl that no flag
// names makes `migrate apply` with no --url migrate the env's database at exit
// 0. The pinned community binary v1.3.0 reads an env only when --env names it,
// so every row fails the way that binary fails, and prod.db, which any read of
// the env would create, stays absent.
func TestMigrateVerbsSelectNoEnvWithoutEnvFlag_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "apply",
			args:    []string{"migrate", "apply", "--dir", "file://migrations"},
			wantErr: `required flag "url" not set`,
		},
		{
			name:    "apply with a --var",
			args:    []string{"migrate", "apply", "--dir", "file://migrations", "--var", "x=1"},
			wantErr: `required flag "url" not set`,
		},
		{
			name:    "apply with a --config",
			args:    []string{"migrate", "apply", "--dir", "file://migrations", "--config", "file://atlas.hcl"},
			wantErr: `required flag "url" not set`,
		},
		{
			name:    "apply with an empty --env",
			args:    []string{"migrate", "apply", "--dir", "file://migrations", "--env", ""},
			wantErr: `required flag "url" not set`,
		},
		{
			name:    "status",
			args:    []string{"migrate", "status", "--dir", "file://migrations"},
			wantErr: `sql/sqlclient: missing driver\. See: https://atlasgo\.io/url`,
		},
		{
			name:    "set",
			args:    []string{"migrate", "set", "1", "--dir", "file://migrations"},
			wantErr: `sql/sqlclient: missing driver\. See: https://atlasgo\.io/url`,
		},
		{
			name:    "lint",
			args:    []string{"migrate", "lint", "--dir", "file://migrations", "--latest", "1"},
			wantErr: `required flag\(s\) "dev-url" not set`,
		},
		{
			name:    "diff",
			args:    []string{"migrate", "diff", "--dir", "file://migrations", "--to", "file://schema.sql"},
			wantErr: `required flag\(s\) "dev-url" not set`,
		},
		{
			name:    "down",
			args:    []string{"migrate", "down", "--dir", "file://migrations"},
			wantErr: `database URL is required`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := t.TempDir()
			t.Chdir(root)
			writeEnvSelectionProject(c, "atlas.hcl", loneProdEnv)

			output, err := atlastest.RunCompatOutput(test.args...)

			c.Assert(err, qt.ErrorMatches, test.wantErr, qt.Commentf("command output:\n%s", output))
			atlastest.AssertPathAbsent(c, filepath.Join(root, "prod.db"))
		})
	}
}

// TestMigrateDownReadsNoPtahYAMLWithoutEnvFlag_FailurePath holds the forwarded
// half of the same defect. `migrate down` runs a native command that reads
// project configuration itself. Left alone it discovers ./ptah.yaml and
// selects its lone env, or takes its top-level url, although the adapter
// selected no project.
func TestMigrateDownReadsNoPtahYAMLWithoutEnvFlag_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		ptah string
	}{
		{name: "a lone env", ptah: "env:\n  prod:\n    url: sqlite://prod.db\n"},
		{name: "a top-level url", ptah: "url: sqlite://prod.db\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root := t.TempDir()
			t.Chdir(root)
			writeEnvSelectionProject(c, "ptah.yaml", test.ptah)

			output, err := atlastest.RunCompatOutput("migrate", "down", "--dir", "file://migrations")

			c.Assert(err, qt.ErrorMatches, `database URL is required`, qt.Commentf("command output:\n%s", output))
			atlastest.AssertPathAbsent(c, filepath.Join(root, "prod.db"))
		})
	}
}

// projectLintLogs declares a lint report template at the top level and another
// in the env, so the report says which of them a run read.
const projectLintLogs = `lint {
  log = "TOP{{ range .Files }}{{ .Name }}{{ end }}"
}

env "prod" {
  lint {
    log = "ENV{{ range .Files }}{{ .Name }}{{ end }}"
  }
}
`

// TestMigrateLintProjectLogSelection_HappyPath holds which blocks a run reads.
// Measured on the pinned community binary v1.3.0 with this file: no project
// flag prints the default report, --config alone prints TOP2.sql, and --env
// prints ENV2.sql. The --config row is what separates "the env is not selected"
// from "the file is not read".
func TestMigrateLintProjectLogSelection_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		project []string
		want    string
	}{
		{name: "no project flag reads no file", want: "Analyzing changes from version 1 to 2"},
		{name: "a --config reads the top-level block", project: []string{"--config", "file://atlas.hcl"}, want: "TOP2.sql"},
		{name: "an --env reads the env block", project: []string{"--env", "prod"}, want: "ENV2.sql"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Chdir(t.TempDir())
			writeEnvSelectionProject(c, "atlas.hcl", projectLintLogs)
			args := append([]string{
				"migrate", "lint",
				"--dir", "file://migrations",
				"--dev-url", "sqlite://file?mode=memory",
				"--latest", "1",
			}, test.project...)

			output, err := atlastest.RunCompatOutput(args...)

			c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", output))
			c.Assert(output, qt.Contains, test.want)
		})
	}
}

// unparsableProject is an atlas.hcl with a syntax error.
const unparsableProject = "env \"x\" {\n  url =\n"

// TestMigrateLintUnnamedProjectFileIsNotParsed_HappyPath holds that a file no
// flag names is not even parsed: the pinned community binary v1.3.0 runs this
// lint at exit 0 beside a project file with a syntax error.
func TestMigrateLintUnnamedProjectFileIsNotParsed_HappyPath(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	writeEnvSelectionProject(c, "atlas.hcl", unparsableProject)

	output, err := atlastest.RunCompatOutput(
		"migrate", "lint",
		"--dir", "file://migrations",
		"--dev-url", "sqlite://file?mode=memory",
		"--latest", "1",
	)

	c.Assert(err, qt.IsNil, qt.Commentf("command output:\n%s", output))
	c.Assert(output, qt.Contains, "Analyzing changes from version 1 to 2")
}

// TestMigrateLintNamedProjectFileIsParsed_FailurePath is the control for the
// test above: the same file named with --config is parsed and refused, as on
// the pinned community binary v1.3.0.
func TestMigrateLintNamedProjectFileIsParsed_FailurePath(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	writeEnvSelectionProject(c, "atlas.hcl", unparsableProject)

	output, err := atlastest.RunCompatOutput(
		"migrate", "lint",
		"--dir", "file://migrations",
		"--dev-url", "sqlite://file?mode=memory",
		"--latest", "1",
		"--config", "file://atlas.hcl",
	)

	c.Assert(err, qt.ErrorMatches, `(?s)parse atlas project config: atlas\.hcl:2,8-3,1: Invalid expression.*`)
	c.Assert(output, qt.Not(qt.Contains), "Analyzing changes")
}
