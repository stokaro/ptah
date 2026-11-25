//go:build integration

package valuerounding_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// server is how a test makes a database of its own on an engine's server.
type server struct {
	engine dbtarget.Engine
	// create and drop are the statements that create and drop the database
	// named by %[1]s.
	create string
	drop   []string
}

// scratch creates a database of its own on s, runs statements on it with nothing
// of Ptah's in between, and returns the address Ptah connects to it with. The
// database is dropped when the test ends.
func (s server) scratch(c *qt.C, statements ...string) string {
	c.Helper()
	adminURL := dbtarget.URL(c, s.engine)
	admin, err := dbschema.ConnectToDatabase(c.Context(), adminURL)
	c.Assert(err, qt.IsNil)
	random := make([]byte, 6)
	_, err = rand.Read(random)
	c.Assert(err, qt.IsNil)
	name := "ptah_rounding_" + hex.EncodeToString(random)
	_, err = admin.ExecContext(c.Context(), strings.ReplaceAll(s.create, "%[1]s", name))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		for _, drop := range s.drop {
			_, dropErr := admin.ExecContext(context.Background(), strings.ReplaceAll(drop, "%[1]s", name))
			c.Check(dropErr, qt.IsNil)
		}
		dbschema.CloseAndWarn(admin)
	})
	address, err := atlasurl.WithDatabaseName(adminURL, name)
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		run(c, address, statement)
	}
	return address
}

// run runs statement on a session of database.
func run(c *qt.C, database, statement string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), database)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), statement)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
}

// read answers query, which returns one string, on a session of database.
func read(c *qt.C, database, query string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), database)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var got string
	c.Assert(conn.QueryRowContext(c.Context(), query).Scan(&got), qt.IsNil, qt.Commentf("%s", query))
	return got
}

// schemaFile writes text to a schema file of its own and returns its path.
func schemaFile(c *qt.C, text string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(text), 0o600), qt.IsNil)
	return path
}

// ptah runs the native binary in a directory of its own.
func ptah(c *qt.C, args ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, args...)
}

// outcome is what a test reports beside a failed assertion about a run.
func outcome(result clirun.Result) qt.Comment {
	return qt.Commentf("exit %d\nstdout:\n%s\nstderr:\n%s", result.ExitCode, result.Stdout, result.Stderr)
}
