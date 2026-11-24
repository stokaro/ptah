//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// connectFailureRow is a command given an address nothing listens on, and the
// part of the driver's error it prints.
type connectFailureRow struct {
	name   string
	binary clirun.Target
	// args are the command's arguments, given a schema file to plan from.
	args     func(schema string) []string
	exitCode int
	want     string
}

// connectFailureSchema writes a schema file for a command that plans against
// the address it is given.
func connectFailureSchema(c *qt.C) string {
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte("CREATE TABLE t (id integer);\n"), 0o600), qt.IsNil)
	return path
}

// schemaApplyAgainst is `ptah schema apply --dry-run` of a schema file against
// dbURL.
func schemaApplyAgainst(dbURL string) func(schema string) []string {
	return func(schema string) []string {
		return []string{"schema", "apply", "--dry-run", "--db-url", dbURL, "--schema-file", schema}
	}
}

// A password too short to look for in free text is taken out of the address
// alone. pgx names the user and the database it tried, and the words that hold
// the password's characters print whole.
func TestConnectFailureKeepsTheWordsOfAShortPasswordE2E(t *testing.T) {
	rows := []connectFailureRow{
		{
			name:     "a letter of the database name",
			binary:   clirun.Ptah,
			args:     schemaApplyAgainst("postgres://app:x@127.0.0.1:1/execute?sslmode=disable"),
			exitCode: 2,
			want:     "failed to connect to `user=app database=execute`",
		},
		{
			name:     "the user and the database",
			binary:   clirun.Ptah,
			args:     schemaApplyAgainst("postgres://ptah:ptah@127.0.0.1:1/ptah?sslmode=disable"),
			exitCode: 2,
			want:     "failed to connect to `user=ptah database=ptah`",
		},
		{
			name:     "the port after the host",
			binary:   clirun.Ptah,
			args:     schemaApplyAgainst("postgres://app:1@127.0.0.1:1/shop?sslmode=disable"),
			exitCode: 2,
			want:     "127.0.0.1:1 (127.0.0.1)",
		},
		{
			name:   "the compatibility binary",
			binary: clirun.Compat,
			args: func(string) []string {
				return []string{"schema", "inspect", "-u", "postgres://app:x@127.0.0.1:1/execute?sslmode=disable"}
			},
			exitCode: 1,
			want:     "failed to connect to `user=app database=execute`",
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			result := clirun.Run(c, row.binary, clirun.Options{}, row.args(connectFailureSchema(c))...)

			c.Assert(result.ExitCode, qt.Equals, row.exitCode, qt.Commentf("stderr: %s", result.Stderr))
			c.Assert(result.Stderr, qt.Contains, row.want)
			c.Assert(result.Stderr, qt.Not(qt.Contains), "***")
		})
	}
}

// The control: a password long enough to look for is taken out where it stands
// as a word, here the database pgx names.
func TestConnectFailureHidesALongPasswordE2E(t *testing.T) {
	c := qt.New(t)
	args := schemaApplyAgainst("postgres://app:Pa55wordX9@127.0.0.1:1/Pa55wordX9?sslmode=disable")

	result := clirun.Run(c, clirun.Ptah, clirun.Options{}, args(connectFailureSchema(c))...)

	c.Assert(result.ExitCode, qt.Equals, 2, qt.Commentf("stderr: %s", result.Stderr))
	c.Assert(result.Stderr, qt.Contains, "failed to connect to `user=app database=***`")
	c.Assert(result.Stderr+result.Stdout, qt.Not(qt.Contains), "Pa55wordX9")
}
