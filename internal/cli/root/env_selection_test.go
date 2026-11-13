package root_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
)

// envProjectFile is an atlas.hcl whose only env names prod.db and its
// migration directory, the shape stokaro/ptah#5999 measured.
const envProjectFile = "env \"prod\" {\n  url = \"sqlite://prod.db\"\n  migration {\n    dir = \"file://migrations\"\n  }\n}\n"

// writeEnvProject writes, in the working directory, a migration, a schema file,
// the project file and an empty prod.db.
func writeEnvProject(c *qt.C, file, content string) {
	c.Helper()
	c.Assert(os.Mkdir("migrations", 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join("migrations", "1.sql"), []byte("CREATE TABLE t1 (id int);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("schema.sql", []byte("CREATE TABLE t1 (id int);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(file, []byte(content), 0o600), qt.IsNil)
	c.Assert(os.WriteFile("prod.db", nil, 0o600), qt.IsNil)
}

// prodTables reads back the tables of prod.db in the working directory.
func prodTables(c *qt.C) []string {
	c.Helper()
	path, err := filepath.Abs("prod.db")
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(path))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(c.Context(), "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	var tables []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		tables = append(tables, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return tables
}

// TestNativeCommandsSelectNoEnvWithoutEnvFlag_FailurePath holds the rule of
// stokaro/ptah#5999 on every native command that reaches a database, and on
// project adopt: beside a project file whose only env names prod.db, a run with
// no --env selects no env, so it has no database URL and adoption has nothing to
// analyze. Each command refuses at the usage exit status, says no --env was
// given and names the env, and prod.db holds no table. A loader that takes the
// lone env for the project default runs each of these against prod.db, and an
// adopt --check that analyzed the top-level settings alone would pass at exit 0.
func TestNativeCommandsSelectNoEnvWithoutEnvFlag_FailurePath(t *testing.T) {
	const hint = `: no --env given; atlas.hcl declares env "prod"` + "\n"
	tests := []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{
			name:       "migrations up",
			args:       []string{"migrations", "up"},
			wantStderr: "error: database URL is required" + hint,
		},
		{
			name:       "migrations up with a migration directory",
			args:       []string{"migrations", "up", "--migrations-dir", "migrations"},
			wantStderr: "error: database URL is required" + hint,
		},
		{
			name:       "migrations down",
			args:       []string{"migrations", "down"},
			wantStderr: "error: database URL is required" + hint,
		},
		{
			name:       "migrations status",
			args:       []string{"migrations", "status"},
			wantStderr: "error: database URL is required" + hint,
		},
		{
			name:       "migrations tag",
			args:       []string{"migrations", "tag"},
			wantStderr: "error: database URL is required" + hint,
		},
		{
			name:       "migrations plan",
			args:       []string{"migrations", "plan", "--schema-file", "schema.sql"},
			wantStderr: "error: database URL is required" + hint,
		},
		{
			name:       "migrations generate",
			args:       []string{"migrations", "generate", "--schema-file", "schema.sql"},
			wantStderr: "error: database URL is required" + hint,
		},
		{
			name:       "schema apply",
			args:       []string{"schema", "apply", "--schema-file", "schema.sql"},
			wantStderr: "error: database URL is required" + hint,
		},
		{
			name:       "schema plan",
			args:       []string{"schema", "plan", "--schema-file", "schema.sql"},
			wantStderr: "error: database URL is required" + hint,
		},
		{
			name:       "schema compare",
			args:       []string{"schema", "compare", "--schema-file", "schema.sql"},
			wantStderr: "error: database URL is required" + hint,
		},
		{
			name:       "schema inspect",
			args:       []string{"schema", "inspect"},
			wantStderr: "error: an inspection source is required: pass --db-url, --schema-file, or --migrations-dir" + hint,
		},
		{
			name:       "project adopt --check",
			args:       []string{"project", "adopt", "--check"},
			wantStderr: "error: project adopt analyzes one env" + hint,
		},
		{
			name:       "project adopt --preflight",
			args:       []string{"project", "adopt", "--check", "--preflight"},
			wantStderr: "error: project adopt analyzes one env" + hint,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Chdir(c.TempDir())
			writeEnvProject(c, "atlas.hcl", envProjectFile)

			stdout, stderr, code := runRoot(c, test.args...)

			c.Assert(code, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stderr, qt.Equals, test.wantStderr)
			c.Assert(prodTables(c), qt.HasLen, 0)
		})
	}
}

// TestNativeMigrationsUpSelectsNoPtahYAMLEnvWithoutEnvFlag_FailurePath is the
// same rule for ptah.yaml: its lone env is not the project default either.
func TestNativeMigrationsUpSelectsNoPtahYAMLEnvWithoutEnvFlag_FailurePath(t *testing.T) {
	c := qt.New(t)
	t.Chdir(c.TempDir())
	writeEnvProject(c, "ptah.yaml", "env:\n  prod:\n    url: sqlite://prod.db\n    migration:\n      dir: ./migrations\n")

	stdout, stderr, code := runRoot(c, "migrations", "up")

	c.Assert(code, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stderr, qt.Equals, `error: database URL is required: no --env given; ptah.yaml declares env "prod"`+"\n")
	c.Assert(prodTables(c), qt.HasLen, 0)
}

// TestNativeMigrationsUpWithEnvFlag_HappyPath is the control for the test
// above: naming the env applies its url and migration directory, so the
// refusals there come from the selection rule and not from a project file that
// is never read.
func TestNativeMigrationsUpWithEnvFlag_HappyPath(t *testing.T) {
	c := qt.New(t)
	t.Chdir(c.TempDir())
	writeEnvProject(c, "atlas.hcl", envProjectFile)

	stdout, stderr, code := runRoot(c, "migrations", "up", "--env", "prod")

	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(prodTables(c), qt.DeepEquals, []string{"atlas_schema_revisions", "t1"})
}
