package migratestatus_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/migratestatus"
	"ptah.run/internal/errorkind"
	"ptah.run/migration/migrator"
)

// refusedAddress is a loopback host:port nothing listens on.
func refusedAddress(c *qt.C) string {
	c.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, qt.IsNil)
	address := listener.Addr().String()
	c.Assert(listener.Close(), qt.IsNil)
	return address
}

// runStatusSplit runs the command with the two streams apart, because the
// document is standard output and nothing else is.
func runStatusSplit(args ...string) (stdout, stderr string, err error) {
	cmd := migratestatus.NewMigrateStatusCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

// decodeOnly reads stdout as exactly one JSON document of type T, refusing a
// field T does not declare and anything after the document.
func decodeOnly[T any](c *qt.C, stdout string) T {
	c.Helper()
	decoder := json.NewDecoder(strings.NewReader(stdout))
	decoder.DisallowUnknownFields()
	var document T
	c.Assert(decoder.Decode(&document), qt.IsNil, qt.Commentf("stdout:\n%s", stdout))
	var trailing json.RawMessage
	c.Assert(decoder.Decode(&trailing), qt.Equals, io.EOF, qt.Commentf("stdout:\n%s", stdout))
	return document
}

func statusMigrations(c *qt.C) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.up.sql"),
		[]byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_init.down.sql"),
		[]byte("DROP TABLE widgets;\n"), 0o600), qt.IsNil)
	return dir
}

// TestMigrateStatusJSONNamesAnUnreachableTarget is the document the operator's
// history Job reads when the database cannot be reached. It carries the status
// contract's version and none of a status's fields, so no reader takes it for
// a database with nothing applied.
func TestMigrateStatusJSONNamesAnUnreachableTarget(t *testing.T) {
	c := qt.New(t)

	stdout, stderr, err := runStatusSplit("--db-url", "mysql://ptah:secret@"+refusedAddress(c)+"/app",
		"--migrations-dir", statusMigrations(c), "--json")

	c.Assert(err, qt.IsNotNil)
	c.Assert(decodeOnly[errorkind.Failure](c, stdout), qt.DeepEquals, errorkind.Failure{
		ContractVersion: migrator.StatusContractVersion,
		Command:         "migrations status",
		Error:           &errorkind.Report{Kind: errorkind.DatabaseUnreachable, Message: err.Error()},
	})
	c.Assert(stderr, qt.Equals, "error: "+err.Error()+"\n")
}

// TestMigrateStatusJSONGivesAnUnreachableRegistryNoKind is the control: the
// migration directory's registry refuses the dial, and that says nothing about
// the database.
func TestMigrateStatusJSONGivesAnUnreachableRegistryNoKind(t *testing.T) {
	c := qt.New(t)

	stdout, _, err := runStatusSplit("--db-url", "sqlite://"+filepath.Join(c.TempDir(), "app.db"),
		"--migrations-dir", "oci://"+refusedAddress(c)+"/migrations/app:v1", "--plain-http", "--json")

	c.Assert(err, qt.ErrorAs, new(*net.OpError))
	document := decodeOnly[errorkind.Failure](c, stdout)
	c.Assert(document.Error, qt.DeepEquals, &errorkind.Report{Message: err.Error()})
}

// TestMigrateStatusJSONNamesItsCommand: a status document names the command
// beside its version, and a run that read one prints only that one.
func TestMigrateStatusJSONNamesItsCommand(t *testing.T) {
	c := qt.New(t)

	stdout, stderr, err := runStatusSplit("--db-url", "sqlite://"+filepath.Join(c.TempDir(), "app.db"),
		"--migrations-dir", statusMigrations(c), "--json", "--exit-code")

	c.Assert(err, qt.IsNotNil, qt.Commentf("a pending migration fails --exit-code"))
	status := decodeOnly[migrator.MigrationStatus](c, stdout)
	c.Assert(status.ContractVersion, qt.Equals, migrator.StatusContractVersion)
	c.Assert(status.Command, qt.Equals, "migrations status")
	c.Assert(status.PendingMigrations, qt.DeepEquals, []int64{1})
	c.Assert(stderr, qt.Equals, "")
}
