//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/schemaprep"
)

// A verb that only reads a declared schema, and neither renders nor compares
// it, has no later step to refuse two CHECKs of one name in one table. Each
// refuses it as it reads the file, with no database and no registry reached.
func TestSchemaReadVerbsRefuseASecondCheckOfANameInOneTableFS(t *testing.T) {
	tests := []struct {
		name string
		args func(schema string) []string
	}{
		{name: "export", args: func(schema string) []string {
			return []string{"schema", "export", "--from", "sql", "--to", "dbml", "--schema-file", schema}
		}},
		{name: "lineage", args: func(schema string) []string {
			return []string{"schema", "lineage", "--dialect", "postgres", "--schema-file", schema}
		}},
		{name: "push", args: func(schema string) []string {
			// The registry is a closed port: a push that read the file first
			// is refused before it connects.
			return []string{"schema", "push", "--dialect", "postgres", "--schema-file", schema, "--plain-http", "oci://127.0.0.1:1/ptah/schema:v1"}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			schema := writeCheckNameSchema(c, "CREATE TABLE t (a int CONSTRAINT dd CHECK (a > 0), CONSTRAINT dd CHECK (a < 9));\n")

			out, err := runPtahNativeWithError(test.args(schema)...)

			c.Assert(err, qt.ErrorIs, schemaprep.ErrDuplicateCheckName, qt.Commentf("%s", out))
		})
	}
}

// validate reports the same refusal as a structural problem and exits 1, since
// it has no other place to say it.
func TestSchemaValidateReportsASecondCheckOfANameInOneTableFS(t *testing.T) {
	c := qt.New(t)
	schema := writeCheckNameSchema(c, "CREATE TABLE t (a int CONSTRAINT dd CHECK (a > 0), CONSTRAINT dd CHECK (a < 9));\n")

	out, err := runPtahNativeWithError("schema", "validate", "--dialect", "postgres", "--schema-file", schema)

	c.Assert(err, qt.ErrorMatches, `(?s).*1 structural problem.*`)
	c.Assert(out, qt.Matches, `(?s).*two CHECK constraints claim the same name: table "t" declares CHECK "dd" and CHECK "dd".*`)
}
