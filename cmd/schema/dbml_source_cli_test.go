package schema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestSchemaCommandsReadADBMLSchemaFile is the test the loader slice needed and
// did not have.
//
// Wiring `.dbml` into internal/schemafile is not enough to make any command read
// one: internal/schemaload keeps its own list of accepted extensions and rejects
// the file before the loader is reached. A unit test that called the loader
// directly passed while every CLI path still refused the document, which is what
// this measures instead — through the command, where a second gate cannot hide.
func TestSchemaCommandsReadADBMLSchemaFile(t *testing.T) {
	document := "Table public.users {\n  id bigint [pk, increment]\n  email text [not null, unique]\n}\n"

	rows := []struct {
		name    string
		verb    string
		expects string
	}{
		{name: "validate", verb: "validate", expects: ""},
		{name: "render", verb: "render", expects: "users"},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(t.TempDir(), "schema.dbml")
			c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)

			out, err := runSchema("", row.verb, "--schema-file", path, "--dialect", "postgres")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, row.expects)
		})
	}
}

// TestSchemaValidateReportsADBMLSyntaxErrorWithItsPosition pins that the
// diagnostic a reader gets through the command carries file, line and column
// rather than being wrapped into something positionless.
func TestSchemaValidateReportsADBMLSyntaxErrorWithItsPosition(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "broken.dbml")
	c.Assert(os.WriteFile(path, []byte("Table t {\n  a int [unlock_everything]\n}\n"), 0o600), qt.IsNil)

	out, err := runSchema("", "validate", "--schema-file", path, "--dialect", "postgres")

	c.Assert(err, qt.IsNotNil)
	combined := out + err.Error()
	c.Assert(combined, qt.Contains, "broken.dbml:")
	c.Assert(combined, qt.Contains, `unsupported column setting "unlock_everything"`)
}

// TestSchemaValidateRefusesAnUnknownExtensionAndNamesDBML keeps the refusal in
// step with what the loader accepts. Two lists of accepted extensions is what
// let the gap above exist, so the message that quotes one is worth pinning.
func TestSchemaValidateRefusesAnUnknownExtensionAndNamesDBML(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "schema.txt")
	c.Assert(os.WriteFile(path, []byte("nothing\n"), 0o600), qt.IsNil)

	out, err := runSchema("", "validate", "--schema-file", path, "--dialect", "postgres")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out+err.Error(), qt.Contains, ".dbml")
}
