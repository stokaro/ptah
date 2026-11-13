//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/atlascompat"
	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
	"ptah.run/migration/migrationfile"
)

// envSelectionProject writes the directory stokaro/ptah#5999 measured: two
// hashed migrations, an atlas.hcl whose only env names prod.db, and prod.db
// itself as an empty SQLite database, so a run that reads the env has a
// database to migrate and the read-back can tell.
func envSelectionProject(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.Mkdir(migrations, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "1.sql"), []byte("CREATE TABLE t1 (id int);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "2.sql"), []byte("CREATE TABLE t2 (id int);\n"), 0o600), qt.IsNil)
	sum, err := atlascompat.ComputeSum(os.DirFS(migrations), migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, atlascompat.AtlasSumFileName), sum.Bytes(), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte("env \"prod\" {\n  url = \"sqlite://prod.db\"\n}\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "prod.db"), nil, 0o600), qt.IsNil)
	return dir
}

// envSelectionTables reads back the tables of prod.db in dir, by name.
func envSelectionTables(c *qt.C, dir string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.ToSlash(filepath.Join(dir, "prod.db")))
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

// TestCompatMigrateApplyWithoutEnvLeavesTheEnvDatabaseAloneE2E runs `migrate
// apply` with no --url and no --env beside a project whose only env names a
// database, through the shipped ptah-compat and the pinned community binary,
// and reads the database back.
//
// The community binary reads an env only when --env names it, so the flags
// are the whole configuration: it exits 1 with `required flag "url" not set`
// and prod.db holds no table. An adapter that selects the lone env instead
// migrates prod.db at exit 0, with or without --config, --var or strict
// compatibility.
func TestCompatMigrateApplyWithoutEnvLeavesTheEnvDatabaseAloneE2E(t *testing.T) {
	reference := atlasReferenceBinary(t)
	tests := []struct {
		name string
		args []string
		env  []string
	}{
		{name: "no project flag"},
		{name: "a --config", args: []string{"--config", "file://atlas.hcl"}},
		{name: "a --var", args: []string{"--var", "x=1"}},
		{name: "strict compatibility", env: []string{"PTAH_ATLAS_STRICT_COMPAT=1"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			communityDir := envSelectionProject(c)
			compatDir := envSelectionProject(c)
			args := append([]string{"migrate", "apply", "--dir", "file://migrations"}, test.args...)

			communityOut, communityErr, err := runCLIProcess(c.Context(), communityDir, reference, args...)
			compat := clirun.Run(c, clirun.Compat, clirun.Options{
				Dir: compatDir,
				Env: append(environmentWithoutPtahVariables(), test.env...),
			}, args...)

			c.Assert(exitStatusOf(c, err), qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", communityOut, communityErr))
			c.Assert(communityOut, qt.Equals, "")
			c.Assert(communityErr, qt.Equals, "Error: required flag \"url\" not set\n")
			c.Assert(envSelectionTables(c, communityDir), qt.HasLen, 0)
			c.Assert(compat.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", compat.Stdout, compat.Stderr))
			c.Assert(compat.Stdout, qt.Equals, communityOut)
			c.Assert(compat.Stderr, qt.Equals, communityErr)
			c.Assert(envSelectionTables(c, compatDir), qt.HasLen, 0)
		})
	}
}

// TestCompatMigrateApplyWithEnvMigratesTheEnvDatabaseE2E is the control for the
// test above: naming the env still applies its url on both binaries, so the
// refusal there comes from the selection rule and not from a project file that
// is never read.
func TestCompatMigrateApplyWithEnvMigratesTheEnvDatabaseE2E(t *testing.T) {
	c := qt.New(t)
	reference := atlasReferenceBinary(t)
	communityDir := envSelectionProject(c)
	compatDir := envSelectionProject(c)
	args := []string{"migrate", "apply", "--dir", "file://migrations", "--env", "prod"}

	communityOut, communityErr, err := runCLIProcess(c.Context(), communityDir, reference, args...)
	compat := clirun.Run(c, clirun.Compat, clirun.Options{Dir: compatDir, Env: environmentWithoutPtahVariables()}, args...)

	c.Assert(exitStatusOf(c, err), qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", communityOut, communityErr))
	c.Assert(envSelectionTables(c, communityDir), qt.DeepEquals, []string{"atlas_schema_revisions", "t1", "t2"})
	c.Assert(compat.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", compat.Stdout, compat.Stderr))
	c.Assert(envSelectionTables(c, compatDir), qt.DeepEquals, []string{"atlas_schema_revisions", "t1", "t2"})
}
