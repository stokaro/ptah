package schema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// exposureModel is a Go model carrying the four exposure declarations plus one
// column that declares nothing, which is what the two field policies differ
// about.
const exposureModel = `package model

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true" api_expose="read"
	ID int

	//ptah:schema:field name="email" type="TEXT" not_null="true" api_expose="read-write"
	Email string

	//ptah:schema:field name="password_hash" type="TEXT" api_expose="write"
	PasswordHash string

	//ptah:schema:field name="internal_state" type="TEXT" api_expose="none"
	InternalState string

	//ptah:schema:field name="undeclared" type="TEXT"
	Undeclared string
}
`

func writeExposureModel(c *qt.C) string {
	c.Helper()
	dir := resolvedTempDir(c)
	c.Assert(os.WriteFile(filepath.Join(dir, "model.go"), []byte(exposureModel), 0o600), qt.IsNil)
	return dir
}

// TestSchemaExportExposureOmitsHiddenColumns walks the CLI for all three
// targets: a column declared none reaches none of them, and the column that
// declares nothing follows the field policy.
//
// One test over three targets is the point rather than a convenience. #904 asks
// that OpenAPI, GraphQL and Protobuf consume the SAME finalized decision, and a
// per-target test would pass even if each had its own answer.
func TestSchemaExportExposureOmitsHiddenColumns(t *testing.T) {
	tests := []struct {
		name   string
		format string
		extra  []string
	}{
		{name: "openapi", format: "openapi-v3"},
		{name: "graphql", format: "graphql"},
		{name: "protobuf", format: "protobuf", extra: []string{"--proto-package", protoTestPackage}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeExposureModel(c)
			outPath := filepath.Join(resolvedTempDir(c), "out")

			args := append([]string{
				"--to", test.format, "--root-dir", dir, "--out", outPath,
			}, test.extra...)
			_, stderr, err := runSchemaExport(args...)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			data, readErr := os.ReadFile(outPath)
			c.Assert(readErr, qt.IsNil)
			c.Assert(string(data), qt.Not(qt.Contains), "internal_state")
			c.Assert(string(data), qt.Contains, "email")
			// The default policy publishes a column that declares nothing, so
			// no existing schema changes shape by upgrading.
			c.Assert(string(data), qt.Contains, "undeclared")
		})
	}
}

// TestSchemaExportExposureAllowlistWithholdsUndeclaredColumns pins the flag that
// makes an additive migration unable to widen a contract on its own.
func TestSchemaExportExposureAllowlistWithholdsUndeclaredColumns(t *testing.T) {
	tests := []struct {
		name   string
		format string
		extra  []string
	}{
		{name: "openapi", format: "openapi-v3"},
		{name: "graphql", format: "graphql"},
		{name: "protobuf", format: "protobuf", extra: []string{"--proto-package", protoTestPackage}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeExposureModel(c)
			outPath := filepath.Join(resolvedTempDir(c), "out")

			args := append([]string{
				"--to", test.format, "--root-dir", dir, "--out", outPath,
				"--api-field-policy", "allowlist",
			}, test.extra...)
			_, stderr, err := runSchemaExport(args...)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			data, readErr := os.ReadFile(outPath)
			c.Assert(readErr, qt.IsNil)
			c.Assert(string(data), qt.Not(qt.Contains), "undeclared")
			c.Assert(string(data), qt.Contains, "email")
			// The withholding is reported rather than silent, naming the table
			// and the column.
			c.Assert(stderr, qt.Contains, "users.undeclared")
			c.Assert(stderr, qt.Contains, "api_expose")
		})
	}
}

// TestSchemaExportExposureRefusesAnUnknownPolicy is the refusal path: a
// misspelled policy is named rather than silently falling back to the
// permissive one, which would export more than the operator asked for.
func TestSchemaExportExposureRefusesAnUnknownPolicy(t *testing.T) {
	tests := []struct {
		name   string
		format string
		extra  []string
	}{
		{name: "openapi", format: "openapi-v3"},
		{name: "graphql", format: "graphql"},
		{name: "protobuf", format: "protobuf", extra: []string{"--proto-package", protoTestPackage}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeExposureModel(c)
			outPath := filepath.Join(resolvedTempDir(c), "out")

			args := append([]string{
				"--to", test.format, "--root-dir", dir, "--out", outPath,
				"--api-field-policy", "strict",
			}, test.extra...)
			_, stderr, err := runSchemaExport(args...)

			c.Assert(err, qt.IsNotNil)
			c.Assert(stderr+err.Error(), qt.Contains, "api-field-policy")
			_, statErr := os.Stat(outPath)
			c.Assert(os.IsNotExist(statErr), qt.IsTrue, qt.Commentf("a refused export wrote a file"))
		})
	}
}

// TestSchemaExportExposureRefusesAnUnknownDeclaration pins that a misspelled
// api_expose on a column stops the export.
//
// Treating it as absent is the dangerous direction: under the default policy
// the column would be published, which is what the author wrote a declaration
// to prevent.
func TestSchemaExportExposureRefusesAnUnknownDeclaration(t *testing.T) {
	c := qt.New(t)
	dir := resolvedTempDir(c)
	source := `package model

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int

	//ptah:schema:field name="secret" type="TEXT" api_expose="nome"
	Secret string
}
`
	c.Assert(os.WriteFile(filepath.Join(dir, "model.go"), []byte(source), 0o600), qt.IsNil)
	outPath := filepath.Join(resolvedTempDir(c), "out")

	_, stderr, err := runSchemaExport("--to", "openapi-v3", "--root-dir", dir, "--out", outPath)

	c.Assert(err, qt.IsNotNil)
	c.Assert(stderr+err.Error(), qt.Contains, "nome")
	_, statErr := os.Stat(outPath)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue, qt.Commentf("a refused export wrote a file"))
}
