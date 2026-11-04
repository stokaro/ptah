package schema_test

import (
	"net"
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
)

// refusedDiffServerURL is a MySQL URL that names no database, on a port
// nothing listens on.
func refusedDiffServerURL(c *qt.C) string {
	c.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, qt.IsNil)
	address := listener.Addr().String()
	c.Assert(listener.Close(), qt.IsNil)
	return "mysql://root:secret@" + address + "/"
}

// writeDiffServerRealm writes an HCL document declaring one database and a
// table in it, and returns its file URL.
func writeDiffServerRealm(c *qt.C) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "realm.hcl")
	document := "schema \"app\" {}\ntable \"t\" {\n  schema = schema.app\n  column \"id\" {\n    type = int\n  }\n}\n"
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return "file://" + path
}

// TestSchemaDiffDialsAFromServerBesideAFile gives `schema diff` a --from
// MySQL URL that names no database and a --to file, with no --dev-url. The
// server is read whole, as `schema diff` reads two servers, so the run dials
// the port nothing listens on, and the failure is the dial's rather than the
// URL's.
func TestSchemaDiffDialsAFromServerBesideAFile(t *testing.T) {
	c := qt.New(t)

	_, _, err := runSchemaStreams("diff", "--from", refusedDiffServerURL(c), "--to", writeDiffServerRealm(c))

	c.Assert(err, qt.ErrorAs, new(*net.OpError))
	c.Assert(err, qt.Not(qt.ErrorMatches), `.*names no database.*`)
}

// TestSchemaDiffRefusesAServerItCannotCompareWhole is the other half: a --to
// server beside a --from file, and a server beside a dev database, are refused
// before anything is dialed, because no read there compares the server's
// databases.
func TestSchemaDiffRefusesAServerItCannotCompareWhole(t *testing.T) {
	tests := []struct {
		name string
		args func(server, file string) []string
	}{
		{
			name: "a --to server beside a --from file",
			args: func(server, file string) []string { return []string{"--from", file, "--to", server} },
		},
		{
			name: "a --from server beside a dev database",
			args: func(server, file string) []string {
				return []string{"--from", server, "--to", file, "--dev-url", "mysql://root:secret@127.0.0.1:1/dev"}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, _, err := runSchemaStreams(slices.Concat([]string{"diff"}, test.args(refusedDiffServerURL(c), writeDiffServerRealm(c)))...)

			c.Assert(err, qt.ErrorMatches, `(?s).*the database URL names no database, and this command reads and changes one MySQL or MariaDB database, not a whole server.*`)
		})
	}
}
