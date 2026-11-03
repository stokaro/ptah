package migrateup_test

import (
	"encoding/json"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

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

// TestMigrateUpJSONNamesAnUnreachableTarget is the run the operator's apply Job
// reports when the database cannot be reached. Nothing was selected, so
// nothing was sent: the outcome is failed rather than unknown, with no plan
// and no status, and the error names the kind.
func TestMigrateUpJSONNamesAnUnreachableTarget(t *testing.T) {
	c := qt.New(t)

	stdout, stderr, err := runUpSplit("--db-url", "postgres://ptah:secret@"+refusedAddress(c)+"/app?sslmode=disable",
		"--migrations-dir", writeUpMigrations(t), "--json")

	c.Assert(err, qt.IsNotNil)
	c.Assert(decodeOnly[migrator.RunResult](c, stdout), qt.DeepEquals, migrator.RunResult{
		ContractVersion: migrator.RunContractVersion,
		Command:         "migrations up",
		Direction:       migrator.MigrationDirectionUp,
		Outcome:         migrator.RunOutcomeFailed,
		Error:           &migrator.ErrorReport{Kind: string(errorkind.DatabaseUnreachable), Message: err.Error()},
	})
	c.Assert(stderr, qt.Contains, "error: "+err.Error()+"\n")
}

// TestMigrateUpJSONGivesAnUnreachableRegistryNoKind is the control: the
// migration directory's registry refuses the dial, which is a run that stopped
// before it selected anything and says nothing about the database.
func TestMigrateUpJSONGivesAnUnreachableRegistryNoKind(t *testing.T) {
	c := qt.New(t)

	stdout, _, err := runUpSplit("--db-url", "sqlite://"+filepath.Join(t.TempDir(), "app.db"),
		"--migrations-dir", "oci://"+refusedAddress(c)+"/migrations/app:v1", "--plain-http", "--json")

	c.Assert(err, qt.ErrorAs, new(*net.OpError))
	result := decodeOnly[migrator.RunResult](c, stdout)
	c.Assert(result.Outcome, qt.Equals, migrator.RunOutcomeFailed)
	c.Assert(result.Error, qt.DeepEquals, &migrator.ErrorReport{Message: err.Error()})
}

// TestMigrateUpJSONNamesItsCommand: the document of a run that applied
// migrations names the command, and is the only document the run prints.
func TestMigrateUpJSONNamesItsCommand(t *testing.T) {
	c := qt.New(t)

	stdout, _, err := runUpSplit("--db-url", "sqlite://"+filepath.Join(t.TempDir(), "app.db"),
		"--migrations-dir", writeUpMigrations(t), "--json")

	c.Assert(err, qt.IsNil)
	result := decodeOnly[migrator.RunResult](c, stdout)
	c.Assert(result.Command, qt.Equals, "migrations up")
	c.Assert(result.Outcome, qt.Equals, migrator.RunOutcomeApplied)
	c.Assert(result.Error, qt.IsNil)
}
