package root_test

import (
	"bytes"
	"net"
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/root"
)

// refusedMySQLServer is a MySQL URL that names no database, on a port nothing
// listens on.
func refusedMySQLServer(c *qt.C) string {
	c.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, qt.IsNil)
	address := listener.Addr().String()
	c.Assert(listener.Close(), qt.IsNil)
	return "mysql://root:secret@" + address + "/"
}

// runServerScopeVerb runs the native command tree with args and returns its error.
func runServerScopeVerb(args ...string) error {
	cmd := root.NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	return cmd.Execute()
}

// writeServerSchema writes a schema file naming one table in a database.
func writeServerSchema(c *qt.C) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte("CREATE TABLE app.orders (id int PRIMARY KEY);\n"), 0o600), qt.IsNil)
	return path
}

// TestComparisonVerbsDialAMySQLServerURL gives `schema compare` and `schema
// drift` a MySQL URL that names no database. Each reads it as the whole
// server, as `schema apply` does, so the run dials the port nothing listens
// on, and the failure is the dial's rather than the URL's.
func TestComparisonVerbsDialAMySQLServerURL(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "schema compare", args: []string{"schema", "compare"}},
		{name: "schema drift", args: []string{"schema", "drift"}},
		{name: "schema apply", args: []string{"schema", "apply", "--dry-run"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := runServerScopeVerb(slices.Concat(test.args,
				[]string{"--db-url", refusedMySQLServer(c), "--schema-file", writeServerSchema(c)})...)

			c.Assert(err, qt.ErrorAs, new(*net.OpError))
			c.Assert(err, qt.Not(qt.ErrorMatches), `.*names no database.*`)
		})
	}
}

// TestMigrationsVerbsRefuseAMySQLServerURL is the control: the versioned
// verbs read and change one database, and refuse the same URL before they
// dial.
func TestMigrationsVerbsRefuseAMySQLServerURL(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "migrations plan", args: []string{"migrations", "plan"}},
		{name: "migrations generate", args: []string{"migrations", "generate", "--migrations-dir", "migrations"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Chdir(c.TempDir())

			err := runServerScopeVerb(slices.Concat(test.args,
				[]string{"--db-url", refusedMySQLServer(c), "--schema-file", writeServerSchema(c)})...)

			c.Assert(err, qt.ErrorMatches, `(?s).*the database URL names no database, and this command reads and changes one MySQL or MariaDB database, not a whole server.*`)
		})
	}
}

// TestSchemasHelpOffersAWholeServerWhereTheVerbReadsOne holds the --schemas
// help of each verb that reads a MySQL or MariaDB URL naming no database to
// the sentence that says so.
func TestSchemasHelpOffersAWholeServerWhereTheVerbReadsOne(t *testing.T) {
	for _, path := range [][]string{
		{"schema", "compare"}, {"schema", "drift"}, {"schema", "apply"}, {"schema", "inspect"},
	} {
		t.Run(path[0]+" "+path[1], func(t *testing.T) {
			c := qt.New(t)

			flag := schemasFlagOf(c, root.NewRootCommand(), path...)

			c.Assert(flag.Usage, qt.Contains, "a MySQL or MariaDB URL naming no database across every user database")
		})
	}
}

// TestSchemasHelpOffersNoWholeServerWhereTheVerbRefusesOne is the other half:
// a verb that refuses a MySQL or MariaDB URL naming no database does not offer
// to read one.
func TestSchemasHelpOffersNoWholeServerWhereTheVerbRefusesOne(t *testing.T) {
	for _, path := range [][]string{{"migrations", "plan"}, {"migrations", "generate"}} {
		t.Run(path[0]+" "+path[1], func(t *testing.T) {
			c := qt.New(t)

			flag := schemasFlagOf(c, root.NewRootCommand(), path...)

			c.Assert(flag.Usage, qt.Not(qt.Contains), "MySQL")
			c.Assert(flag.Usage, qt.Contains, "a URL pinning search_path is read at that schema")
		})
	}
}
