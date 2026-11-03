package drift_test

import (
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/drift"
	"ptah.run/internal/errorkind"
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

// TestRunDrift_AnUnreachableTargetIsAFailureDocumentOnStdout is the document a
// caller reads when the check could not run: on standard output, where the
// report would have been, with the kind of failure and none of the report's
// fields, so no reader takes it for a database without drift. Standard error
// carries the line a person reads.
func TestRunDrift_AnUnreachableTargetIsAFailureDocumentOnStdout(t *testing.T) {
	c := qt.New(t)
	schemaFile := filepath.Join(c.TempDir(), "desired.sql")
	c.Assert(os.WriteFile(schemaFile, []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)

	code, stdout, stderr := runDriftCommand(c,
		"--db-url", "postgres://ptah:secret@"+refusedAddress(c)+"/app?sslmode=disable",
		"--schema-file", schemaFile, "--format", "json")

	c.Assert(code, qt.Equals, 2)
	document := decodeOnly[errorkind.Failure](c, stdout)
	c.Assert(document.ContractVersion, qt.Equals, drift.ContractVersion)
	c.Assert(document.Command, qt.Equals, "schema drift")
	c.Assert(document.Error, qt.IsNotNil)
	c.Assert(document.Error.Kind, qt.Equals, errorkind.DatabaseUnreachable)
	c.Assert(stderr, qt.Equals, "error: "+document.Error.Message+"\n")
}

// TestRunDrift_AnUnreachableRegistryHasNoKind is the control for the kind: the
// target opens and the desired schema's registry refuses the dial, which is a
// failure of the same shape that says nothing about the database.
func TestRunDrift_AnUnreachableRegistryHasNoKind(t *testing.T) {
	c := qt.New(t)

	code, stdout, _ := runDriftCommand(c,
		"--db-url", "sqlite://"+filepath.Join(c.TempDir(), "target.db"),
		"--schema-file", "oci://"+refusedAddress(c)+"/schemas/app:v1", "--plain-http",
		"--format", "json")

	c.Assert(code, qt.Equals, 2)
	document := decodeOnly[errorkind.Failure](c, stdout)
	c.Assert(document.Error, qt.IsNotNil)
	c.Assert(document.Error.Message, qt.Matches, `(?s).*connect.*`)
	c.Assert(document.Error.Kind, qt.Equals, errorkind.Kind(""))
}

// TestRunDrift_TheReportNamesItsContractAndCommand pins the two fields a
// report carries for a reader that checks what it holds before reading it.
func TestRunDrift_TheReportNamesItsContractAndCommand(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	schemaFile := filepath.Join(dir, "desired.sql")
	c.Assert(os.WriteFile(schemaFile, nil, 0o600), qt.IsNil)

	code, stdout, stderr := runDriftCommand(c,
		"--db-url", "sqlite://"+filepath.Join(dir, "target.db"),
		"--schema-file", schemaFile, "--format", "json")

	c.Assert(code, qt.Equals, 0, qt.Commentf("stderr:\n%s", stderr))
	var report map[string]any
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil)
	c.Assert(report["contract_version"], qt.Equals, float64(1))
	c.Assert(report["command"], qt.Equals, "schema drift")
	c.Assert(report["drift"], qt.IsFalse)
	_, carriesError := report["error"]
	c.Assert(carriesError, qt.IsFalse)
}
