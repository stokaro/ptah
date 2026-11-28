//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
)

// A dev database URL of another engine than the target is refused before
// either database is written. The refusal names each URL the way the operator
// gave it: a native command names its own target flag, --db-url, and a dev URL
// it read from ptah.yaml as the dev database named in the project file, since
// the operator typed no --dev-url. ptah-compat keeps the community binary's
// wording, which names --dev-url and --url.

// otherEngineDevURL is a PostgreSQL dev URL beside a SQLite target. Nothing
// connects to it: the dialect is read from the scheme.
//
// #nosec G101 -- a fixture password for a URL nothing connects to.
const otherEngineDevURL = "postgres://ptah:unused-secret@127.0.0.1:1/dev?sslmode=disable"

// devURLSourceRow is one native command that reads the dev URL from the flag
// or from the project file.
type devURLSourceRow struct {
	name string
	args func(target, schema, dir string) []string
	// fromProject and fromFlag are the refusals when ptah.yaml and --dev-url
	// give the dev URL.
	fromProject string
	fromFlag    string
}

var devURLSourceRows = []devURLSourceRow{
	{
		name: "schema apply",
		args: func(target, schema, _ string) []string {
			return []string{"schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run"}
		},
		fromProject: `the dialect "postgres" of the dev database named in the project file does not match --db-url dialect "sqlite"`,
		fromFlag:    `--dev-url dialect "postgres" does not match --db-url dialect "sqlite"`,
	},
	{
		name: "schema plan",
		args: func(target, schema, _ string) []string {
			return []string{"schema", "plan", "--db-url", target, "--schema-file", schema, "--dry-run"}
		},
		fromProject: `the dialect "postgres" of the dev database named in the project file does not match --db-url dialect "sqlite"`,
		fromFlag:    `--dev-url dialect "postgres" does not match --db-url dialect "sqlite"`,
	},
	{
		name: "schema inspect",
		args: func(target, _, _ string) []string {
			return []string{"schema", "inspect", "--db-url", target}
		},
		fromProject: `the dialect "postgres" of the dev database named in the project file does not match --db-url dialect "sqlite"`,
		fromFlag:    `--dev-url dialect "postgres" does not match --db-url dialect "sqlite"`,
	},
	{
		name: "schema diff",
		args: func(target, schema, _ string) []string {
			return []string{"schema", "diff", "--from", target, "--to", schema}
		},
		fromProject: `--from database dialect "sqlite" does not match the dialect "postgres" of the dev database named in the project file`,
		fromFlag:    `--from database dialect "sqlite" does not match --dev-url dialect "postgres"`,
	},
	{
		name: "migrations lint",
		args: func(_, _, dir string) []string {
			return []string{"migrations", "lint", "--dir", dir, "--dialect", "sqlite"}
		},
		fromProject: `lint dialect "sqlite" does not match the dialect "postgres" of the dev database named in the project file`,
		fromFlag:    `lint dialect "sqlite" does not match --dev-url dialect "postgres"`,
	},
}

// devURLSourceFixture is a directory holding a SQLite target, a schema file
// and an empty migration directory, and the target's URL.
func devURLSourceFixture(c *qt.C) (dir, target, schema, migrations string) {
	c.Helper()
	dir = c.TempDir()
	schema = filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE TABLE t1 (id INTEGER NOT NULL);\n"), 0o600), qt.IsNil)
	migrations = filepath.Join(dir, "migrations")
	c.Assert(os.Mkdir(migrations, 0o700), qt.IsNil)
	return dir, atlasurl.SQLiteURLFromPath(filepath.Join(dir, "target.db")), schema, migrations
}

// TestNativeCommandsNameTheDevURLSourceE2E refuses the same pair of URLs
// twice per command: once with the dev URL in ptah.yaml, where the refusal
// names the project file, and once with --dev-url, where it names the flag.
// Both name the target by the native flag.
func TestNativeCommandsNameTheDevURLSourceE2E(t *testing.T) {
	for _, row := range devURLSourceRows {
		t.Run(row.name+" with the project file", func(t *testing.T) {
			c := qt.New(t)
			dir, target, schema, migrations := devURLSourceFixture(c)
			c.Assert(os.WriteFile(filepath.Join(dir, "ptah.yaml"), []byte("dev: \""+otherEngineDevURL+"\"\n"), 0o600), qt.IsNil)

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir}, row.args(target, schema, migrations)...)

			c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, row.fromProject)
		})
		t.Run(row.name+" with the flag", func(t *testing.T) {
			c := qt.New(t)
			dir, target, schema, migrations := devURLSourceFixture(c)

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
				append(row.args(target, schema, migrations), "--dev-url", otherEngineDevURL)...)

			c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, row.fromFlag)
		})
	}
}

// TestCompatSchemaApplyKeepsTheCommunityDevURLWordingE2E is the control for
// the rows above: ptah-compat names the same pair by the community binary's
// flags, so the native names do not reach the compatibility surface.
func TestCompatSchemaApplyKeepsTheCommunityDevURLWordingE2E(t *testing.T) {
	c := qt.New(t)
	dir, target, schema, _ := devURLSourceFixture(c)

	got := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir},
		"schema", "apply", "--url", target, "--to", "file://"+filepath.ToSlash(schema), "--dev-url", otherEngineDevURL, "--dry-run")

	c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Contains, `--dev-url dialect "postgres" does not match --url dialect "sqlite"`)
}
