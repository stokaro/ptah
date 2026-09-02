package schema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

const metadataGoModels = `package models

//ptah:schema:table name="billing_invoices" api_name="invoices" openapi_name="invoice_documents" graphql_name="invoice_records" proto_name="invoice_records"
type Invoice struct {
	//ptah:schema:field name="id" type="SERIAL" primary="true"
	ID int64

	//ptah:schema:field name="billing_amount_minor" type="INTEGER" api_name="amount" api_type="TEXT" api_expose="read-write"
	Amount int64

	//ptah:schema:field name="internal_note" type="TEXT" openapi_name="note-value" graphql_name="noteText" proto_name="note_text" api_expose="read"
	Note string
}
`

const metadataYAMLSchema = `tables:
  billing_invoices:
    api_name: invoices
    openapi_name: invoice_documents
    graphql_name: invoice_records
    proto_name: invoice_records
    columns:
      id:
        type: SERIAL
        primary: true
      billing_amount_minor:
        type: INTEGER
        api_name: amount
        api_type: TEXT
        api_expose: read-write
      internal_note:
        type: TEXT
        openapi_name: note-value
        graphql_name: noteText
        proto_name: note_text
        api_expose: read
`

const metadataHCLSchema = `schema "public" {}

table "billing_invoices" {
  schema        = schema.public
  api_name      = "invoices"
  openapi_name  = "invoice_documents"
  graphql_name  = "invoice_records"
  proto_name    = "invoice_records"

  column "id" {
    type = serial
  }

  column "billing_amount_minor" {
    type       = integer
    null       = true
    api_name   = "amount"
    api_type   = "TEXT"
    api_expose = "read-write"
  }

  column "internal_note" {
    type         = text
    null         = true
    openapi_name = "note-value"
    graphql_name = "noteText"
    proto_name   = "note_text"
    api_expose   = "read"
  }

  primary_key {
    columns = [column.id]
  }
}
`

func metadataSourceFixture(c *qt.C) (goDir, yamlPath, hclPath string) {
	c.Helper()
	dir := resolvedTempDir(c)
	goDir = filepath.Join(dir, "models")
	c.Assert(os.MkdirAll(goDir, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(goDir, "model.go"), []byte(metadataGoModels), 0o600), qt.IsNil)
	yamlPath = filepath.Join(dir, "schema.yaml")
	c.Assert(os.WriteFile(yamlPath, []byte(metadataYAMLSchema), 0o600), qt.IsNil)
	hclPath = filepath.Join(dir, "schema.hcl")
	c.Assert(os.WriteFile(hclPath, []byte(metadataHCLSchema), 0o600), qt.IsNil)
	return goDir, yamlPath, hclPath
}

// TestSchemaExportMetadataIsEquivalentAcrossGoYAMLAndHCL proves that the three
// authoring formats carry the same contract metadata, not merely the same
// storage schema. Every target is compared byte for byte with a fresh
// Protobuf history file, so a parser that drops one target-specific name,
// exposure, or type override cannot pass by sharing old output state.
func TestSchemaExportMetadataIsEquivalentAcrossGoYAMLAndHCL(t *testing.T) {
	c := qt.New(t)
	goDir, yamlPath, hclPath := metadataSourceFixture(c)
	sources := []struct {
		name string
		args []string
	}{
		{name: "Go", args: []string{"--root-dir", goDir}},
		{name: "YAML", args: []string{"--schema-file", yamlPath}},
		{name: "HCL", args: []string{"--schema-file", hclPath}},
	}

	for _, target := range apiExportTargets() {
		t.Run(target.name, func(t *testing.T) {
			c := qt.New(t)
			baseline := runExportTarget(c, target, sources[0].args)
			for _, source := range sources[1:] {
				t.Run(source.name, func(t *testing.T) {
					c := qt.New(t)
					c.Assert(runExportTarget(c, target, source.args), qt.Equals, baseline)
				})
			}

			switch target.name {
			case "openapi-v3":
				c.Assert(baseline, qt.Contains, "    invoice_documents:\n")
				c.Assert(baseline, qt.Contains, "        amount:\n")
				c.Assert(baseline, qt.Contains, "        note-value:\n")
			case "graphql":
				c.Assert(baseline, qt.Contains, "type InvoiceRecord {")
				c.Assert(baseline, qt.Contains, "amount: String")
				c.Assert(baseline, qt.Contains, "noteText: String")
			case "protobuf":
				c.Assert(baseline, qt.Contains, "message InvoiceRecord {")
				c.Assert(baseline, qt.Contains, "string amount =")
				c.Assert(baseline, qt.Contains, "string note_text =")
			}
		})
	}
}

// TestSchemaExportRefusesInvalidYAMLMetadataBeforeReplacingOutput exercises the
// reader-to-command boundary. Renderer unit tests prove an empty Result; this
// test proves the CLI also leaves a previously published artifact untouched.
func TestSchemaExportRefusesInvalidYAMLMetadataBeforeReplacingOutput(t *testing.T) {
	c := qt.New(t)
	dir := resolvedTempDir(c)
	sourcePath := filepath.Join(dir, "schema.yaml")
	c.Assert(os.WriteFile(sourcePath, []byte(`tables:
  users:
    columns:
      id:
        type: INTEGER
        graphql_name: invalid-name
`), 0o600), qt.IsNil)
	outPath := filepath.Join(dir, "schema.graphql")
	original := []byte("published contract\n")
	c.Assert(os.WriteFile(outPath, original, 0o600), qt.IsNil)

	stdout, stderr, err := runSchemaExport(
		"--to", "graphql", "--schema-file", sourcePath, "--out", outPath,
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr+err.Error(), qt.Contains, `graphql_name "invalid-name"`)
	after, readErr := os.ReadFile(outPath)
	c.Assert(readErr, qt.IsNil)
	c.Assert(after, qt.DeepEquals, original)
}

func TestSchemaExportRefusesInvalidSharedOpenAPINameBeforeReplacingOutput(t *testing.T) {
	c := qt.New(t)
	dir := resolvedTempDir(c)
	sourcePath := filepath.Join(dir, "schema.yaml")
	c.Assert(os.WriteFile(sourcePath, []byte(`tables:
  users:
    api_name: user documents
    columns:
      id:
        type: INTEGER
`), 0o600), qt.IsNil)
	outPath := filepath.Join(dir, "openapi.yaml")
	original := []byte("published contract\n")
	c.Assert(os.WriteFile(outPath, original, 0o600), qt.IsNil)

	stdout, stderr, err := runSchemaExport(
		"--to", "openapi-v3", "--schema-file", sourcePath, "--out", outPath,
	)

	c.Assert(err, qt.IsNotNil)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr+err.Error(), qt.Contains, `api_name "user documents"`)
	after, readErr := os.ReadFile(outPath)
	c.Assert(readErr, qt.IsNil)
	c.Assert(after, qt.DeepEquals, original)
}
