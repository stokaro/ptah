package schema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// A field that disappears from the source retires its number and reserves its
// name. That is a change to the contract a consumer already holds, and it was
// the one compatibility-relevant event in this exporter that neither warned nor
// was gated: a renamed column exited 0 with one number retired and another
// allocated, while a removed type, a changed type and a reused name were each
// already refused by default (stokaro/ptah#905).
//
// The exporter cannot tell a rename from a removal — a column carries no
// identity beyond its name — so both are refused until the caller says which
// they meant.

const fieldRemovalModelWithEmail = `package models

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64

	//ptah:schema:field name="email" type="TEXT" not_null="true"
	Email string
}
`

const fieldRemovalModelRenamed = `package models

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64

	//ptah:schema:field name="email_address" type="TEXT" not_null="true"
	EmailAddress string
}
`

const fieldRemovalModelWithBoth = `package models

//ptah:schema:table name="users"
type User struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int64

	//ptah:schema:field name="email" type="TEXT" not_null="true"
	Email string

	//ptah:schema:field name="nickname" type="TEXT"
	Nickname string
}
`

// exportFieldRemovalBaseline writes the starting model and exports it, so the
// run under test has a compatibility history to change.
func exportFieldRemovalBaseline(c *qt.C) (dir, outPath string) {
	c.Helper()

	dir = resolvedTempDir(c)
	outPath = filepath.Join(resolvedTempDir(c), "schema.proto")
	writeFieldRemovalModel(c, dir, fieldRemovalModelWithEmail)

	_, stderr, err := runSchemaExport(
		"--to", "protobuf", "--root-dir", dir, "--out", outPath, "--proto-package", protoTestPackage)
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	return dir, outPath
}

func writeFieldRemovalModel(c *qt.C, dir, source string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, "model.go"), []byte(source), 0o600), qt.IsNil)
}

func TestSchemaExportProtobufRefusesARetiredFieldByDefault(t *testing.T) {
	tests := []struct {
		name  string
		model string
	}{
		{
			// The case the issue reports: nothing is dropped from the caller's
			// point of view, but the exporter sees one field vanish and another
			// appear.
			name:  "a renamed column",
			model: fieldRemovalModelRenamed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir, outPath := exportFieldRemovalBaseline(c)
			writeFieldRemovalModel(c, dir, tt.model)

			_, stderr, err := runSchemaExport(
				"--to", "protobuf", "--root-dir", dir, "--out", outPath, "--proto-package", protoTestPackage)

			c.Assert(err, qt.IsNotNil, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(err.Error(), qt.Contains, "fields removed from User: email")
			c.Assert(err.Error(), qt.Contains, "--proto-on-field-removal=reserve")
		})
	}
}

func TestSchemaExportProtobufRetiresAFieldWhenAsked(t *testing.T) {
	c := qt.New(t)
	dir, outPath := exportFieldRemovalBaseline(c)
	writeFieldRemovalModel(c, dir, fieldRemovalModelRenamed)

	_, stderr, err := runSchemaExport(
		"--to", "protobuf", "--root-dir", dir, "--out", outPath, "--proto-package", protoTestPackage,
		"--proto-on-field-removal", "reserve")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	// The retirement is announced rather than silent, which is the whole point:
	// the number and the name are both gone from the live contract.
	c.Assert(stderr, qt.Contains, `field "email" was removed from User and retired`)

	rendered, readErr := os.ReadFile(outPath)
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(rendered), qt.Contains, "string email_address = 3;")
	c.Assert(string(rendered), qt.Contains, "reserved 2;")
	c.Assert(string(rendered), qt.Contains, "reserved email;")
}

func TestSchemaExportProtobufSaysNothingWhenNoFieldRetires(t *testing.T) {
	tests := []struct {
		name  string
		model string
	}{
		{
			name:  "the same model again",
			model: fieldRemovalModelWithEmail,
		},
		{
			// Adding a field allocates a number and retires none, so the
			// refusal must not fire on growth.
			name:  "a field added",
			model: fieldRemovalModelWithBoth,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir, outPath := exportFieldRemovalBaseline(c)
			writeFieldRemovalModel(c, dir, tt.model)

			_, stderr, err := runSchemaExport(
				"--to", "protobuf", "--root-dir", dir, "--out", outPath, "--proto-package", protoTestPackage)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(stderr, qt.Not(qt.Contains), "removed from User")
		})
	}
}
